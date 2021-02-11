import argparse
import json
import os
from subprocess import Popen, PIPE
from collections import defaultdict
from datetime import datetime
from time import time
from itertools import product


def parse_args():
    ap = argparse.ArgumentParser(description="Benchmarks histogram creation")
    ap.add_argument("--runs", "-r", type=int, help="Number of runs. Default 10", default=10)
    ap.add_argument(
        "--output", "-o", type=str, help="Output file. Default none, results are printed to console", default=None
    )
    ap.add_argument("--keep_logs", help="Keep log files of benchmark runs. Default delete them", action="store_true")
    ap.add_argument(
        "--log_path", type=str, help="Path for intermediate log files. Default ./benchmark", default="benchmark"
    )
    return vars(ap.parse_args())


class HistogramConfig(dict):
    @classmethod
    def default_config(cls):
        return {
            "python": "/usr/bin/python3",
            "script": "./scripts/create_histograms_parallel.py",
            "change_dir": "changes",
            "change_file": "table_changes_aggregated_grouped.json",
            "output": "histogram.csv",
            "min_sup": 0,
            "max_sup": 1,
            "min_conf": 0,
            "num_threads": 1,
            "partition_size": 1000,
        }

    def __init__(self, init_values=dict()):
        # set default values
        for key, value in self.__class__.default_config().items():
            self[key] = value
        # set user parameters
        for key, value in init_values.items():
            self[key] = value

    def get_cli_command(self):
        return [
            self["python"],
            self["script"],
            self["change_dir"],
            self["change_file"],
            self["output"],
            "--min_supp",
            str(self["min_sup"]),
            "--max_supp",
            str(self["max_sup"]),
            "--min_conf",
            str(self["min_conf"]),
            "--threads",
            str(self["num_threads"]),
        ]


class BenchmarkResult:
    def __init__(self, num_rules, duration):
        self.num_rules = num_rules
        self.duration = duration

    def __str__(self):
        return f"BenchmarkResult({self.num_rules} rules, duration {self.duration})"


class Benchmark:
    def __init__(self, num_runs, output_path, log_path, keep_logs):
        self.num_runs = num_runs
        self.benchmarks = list()
        self.output_path = output_path
        self.output = dict()
        self.log_path = log_path
        self.keep_logs = keep_logs

    def add_experiment(self, variable, variable_values, fixed_values):
        self.benchmarks.append((variable, variable_values, fixed_values))

    def run(self):
        if not os.path.isdir(self.log_path):
            os.mkdir(self.log_path)
        self.output["num_runs"] = self.num_runs
        self.output["experiments"] = list()
        for benchmark in self.benchmarks:
            result = self.__run_experiment(benchmark[0], benchmark[1], benchmark[2])
            runtimes = {k[1]: [r.duration for r in runs] for k, runs in result.items()}
            num_rules = result[list(result.keys())[0]][0].num_rules
            result_entry = {"fixed_values": benchmark[2], "num_rules": num_rules, "runs": runtimes}
            self.output["experiments"].append(result_entry)

        if self.output_path:
            with open(self.output_path, "w") as f:
                json.dump(self.output, f, indent=4)

        if not self.keep_logs:
            for file_name in os.listdir(self.log_path):
                os.remove(os.path.join(self.log_path, file_name))
            os.rmdir(self.log_path)

    def __run_experiment(self, variable_key, variable_values, fixed_values):
        results = defaultdict(list)
        config = HistogramConfig(fixed_values)
        config["output"] = os.path.join(self.log_path, "histogram.csv")
        time_stamp = time()

        for value in variable_values:
            config[variable_key] = value
            print(f"{variable_key} = {value}")
            item_start = time()
            for i in range(self.num_runs):
                item_run_start = time()
                with open(os.path.join(self.log_path, f"log_{time_stamp}_{variable_key}_{value}_{i}"), "w") as log:
                    with Popen(config.get_cli_command(), stdout=log, stderr=log) as proc:
                        proc.wait()
                        item_run_end = time()
                        duration = item_run_end - item_run_start
                        num_rules = None

                        if proc.returncode == 0:
                            try:
                                with open(config["output"]) as f:
                                    num_rules = len(f.readlines())
                                os.remove(config["output"])
                            except FileNotFoundError:
                                pass
                        results[(variable_key, value)].append(BenchmarkResult(num_rules, duration))
            item_end = time()
            item_duration = round(item_end - item_start, 3)
            summary = (
                f"    --> executed {self.num_runs} time(s) "
                + f"in {item_duration} s, "
                + f"{results[(variable_key, value)][0].num_rules} rules"
            )
            print(summary)
        return results


def main(args):
    benchmark = Benchmark(args["runs"], args["output"], args["log_path"], args["keep_logs"])

    fixed_values = {"min_sup": 0.4, "max_sup": 0.5, "num_threads": 5}
    benchmark.add_experiment("min_conf", [x / 100 for x in range(10, 105, 5)], fixed_values)
    fixed_values = {"min_conf": 0.9, "max_sup": 0.5, "num_threads": 5}
    benchmark.add_experiment("min_sup", [x / 100 for x in range(5, 50, 5)], fixed_values)
    fixed_values = {"min_conf": 0.9, "max_sup": 0.5, "min_sup": 0.4}
    benchmark.add_experiment("num_threads", list(range(1, 11)), fixed_values)
    fixed_values = {"min_conf": 0.9, "max_sup": 0.5, "min_sup": 0.4, "num_threads": 5}
    benchmark.add_experiment("partition_size", list(range(500, 3000, 500)), fixed_values)
    benchmark.run()


if __name__ == "__main__":
    main(parse_args())
