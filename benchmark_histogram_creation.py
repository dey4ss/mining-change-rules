#!/usr/local/bin/python3
import argparse
import json
import os
import sys
import multiprocessing as mp
from collections import defaultdict
from contextlib import contextmanager
from time import time

from create_histograms_parallel import create_histograms


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


@contextmanager
def capture_log(file_name):
    with open(file_name, "w") as devnull:
        old_stdout = sys.stdout
        sys.stdout = devnull
        try:
            yield
        finally:
            sys.stdout = old_stdout


class ExperimentConfig(dict):
    @classmethod
    def default_values(cls):
        return {
            "change_dir": "changes",
            "change_file": "table_changes_aggregated_grouped.json",
            "output": "histogram.csv",
            "min_sup": 0,
            "max_sup": 1,
            "min_conf": 0,
            "threads": 1,
            "partition_size": 200,
            "num_bins": 11,
        }

    def __init__(self, init_values=dict()):
        # set default values
        for key, value in self.__class__.default_values().items():
            self[key] = value
        # set user parameters
        for key, value in init_values.items():
            self[key] = value


class BenchmarkResult:
    def __init__(self, num_rules, runtime):
        self.num_rules = num_rules
        self.runtime = runtime

    def __str__(self):
        return f"BenchmarkResult({self.num_rules} rules, runtime {self.runtime} s)"


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
        for benchmark, num_benchmark in zip(self.benchmarks, range(1, len(self.benchmarks) + 1)):
            result = self.__run_experiment(num_benchmark, benchmark[0], benchmark[1], benchmark[2])
            run_summary = {
                variable[1]: {"rules": runs[0].num_rules, "runs": [r.runtime for r in runs]}
                for variable, runs in result.items()
            }
            result_entry = {
                "parameter": benchmark[0],
                "fixed_values": benchmark[2],
                "results": run_summary,
            }
            self.output["experiments"].append(result_entry)

        if self.output_path:
            with open(self.output_path, "w") as f:
                json.dump(self.output, f, indent=2)

        if not self.keep_logs:
            for file_name in os.listdir(self.log_path):
                os.remove(os.path.join(self.log_path, file_name))
            os.rmdir(self.log_path)

    def __run_experiment(self, benchmark_index, variable_key, variable_values, fixed_values):
        results = defaultdict(list)
        config = ExperimentConfig(fixed_values)
        config["output"] = os.path.join(self.log_path, "histogram.csv")
        time_stamp = time()
        print(f"Experiment {benchmark_index}: {variable_key}")

        for value in variable_values:
            config[variable_key] = value
            print(f"{self.indent(1)}{variable_key} = {value}")
            item_start = time()
            for run in range(1, self.num_runs + 1):
                item_run_start = time()
                p = mp.Process(target=create_histograms, args=[config])
                item_run_start = time()
                with capture_log(os.path.join(self.log_path, f"log_{time_stamp}_{variable_key}_{value}_run-{run}")):
                    p.start()
                    p.join()
                    item_run_end = time()
                runtime = item_run_end - item_run_start
                num_rules = None

                try:
                    with open(config["output"]) as f:
                        num_rules = len(f.readlines())
                    os.remove(config["output"])
                except FileNotFoundError:
                    pass
                results[(variable_key, value)].append(BenchmarkResult(num_rules, runtime))

            item_end = time()
            item_runtime = round(item_end - item_start, 3)
            mean_runtime = sum([r.runtime for r in results[(variable_key, value)]]) / self.num_runs
            summary = (
                f"{self.indent(2)}--> executed {self.num_runs} time(s) "
                + f"in {item_runtime} s"
                + f" ({round(mean_runtime, 3)} s/iter), "
                + f"{results[(variable_key, value)][0].num_rules} rules"
            )
            print(summary)
        return results

    def indent(self, level):
        return " " * 4 * level


def main(runs, output, log_path, keep_logs):
    benchmark = Benchmark(runs, output, log_path, keep_logs)

    # min confidence [0.1, 0.15, 0.2, ... 1.0]
    fixed_values = {"min_sup": 0.4, "max_sup": 0.5, "threads": 5}
    benchmark.add_experiment("min_conf", [x / 100 for x in range(90, 105, 5)], fixed_values)

    # min support [0.05, 0.1, 0.15, ..., 0.5]
    fixed_values = {"min_conf": 0.9, "max_sup": 0.5, "threads": 5}
    benchmark.add_experiment("min_sup", [x / 100 for x in range(5, 50, 5)], fixed_values)

    # 1 to 10 threads
    fixed_values = {"min_conf": 0.9, "max_sup": 0.5, "min_sup": 0.4}
    benchmark.add_experiment("num_threads", list(range(1, 11)), fixed_values)

    # partition size [500, 1000, 1500, ..., 5000]
    fixed_values = {"min_conf": 0.9, "max_sup": 0.5, "min_sup": 0.4, "threads": 5}
    benchmark.add_experiment("partition_size", list(range(100, 2000, 100)), fixed_values)

    benchmark.run()


if __name__ == "__main__":
    args = parse_args()
    main(args["runs"], args["output"], args["log_path"], args["keep_logs"])
