#!/usr/bin/python3

import argparse
import json
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.lines as mlines
import os
import re
import seaborn as sns
import pandas as pd
from collections import defaultdict


def parse_args():
    ap = argparse.ArgumentParser(description="Creates plots from a benchmark output file")
    ap.add_argument("benchmark_file", type=str, help="Path to an output file of benchmark_historgam_creation.py ")
    ap.add_argument("output_path", type=str, help="Path to output directory")
    ap.add_argument(
        "--format", "-f", type=str, help="Plot file format", choices=["eps", "pdf", "png", "svg"], default="png"
    )
    return vars(ap.parse_args())


def input_size_from_filename(file_path):
    file_name = file_path.split(os.sep)[-1]
    regex = r"\d+(?=_changes.json)"
    return int(re.search(regex, file_name).group())


def main(input_file, output_path, file_extension):
    parameter_print_names = {
        "change_file": "Input size",
        "min_conf": "Min confidence",
        "min_sup": "Min support",
        "max_sup": "Max support",
        "threads": "Number of threads",
        "num_bins": "Window size",
        "partition_size": "Partition size",
    }

    with open(input_file) as f:
        benchmark_results = json.load(f)

    runsum = 0
    num_runs = benchmark_results["num_runs"]
    seen_parameters = defaultdict(int)
    for experiment in benchmark_results["experiments"]:
        results = experiment["results"]
        parameter = experiment["parameter"]
        print(parameter)
        values = list()
        runtimes_avg = list()
        num_rules = list()
        fixed_values = {k: v for k, v in experiment["fixed_values"].items()}
        if parameter != "change_file":
            input_size = input_size_from_filename(fixed_values["change_file"])
            fixed_values["input_size"] = input_size
            del fixed_values["change_file"]

        for value, item_result in results.items():
            item_runtime = sum(item_result["runs"])
            if parameter != "change_file":
                parsed_value = float(value)
            else:
                parsed_value = input_size_from_filename(value)
            values.append(parsed_value)
            runtimes_avg.append(item_runtime / num_runs)
            runsum += item_runtime
            num_rules.append(item_result["rules"])

        data = pd.DataFrame({parameter: values, "runtime": runtimes_avg, "rules": num_rules})

        fixed_value_str = ", ".join([f"{k}={v}" for k, v in fixed_values.items()])
        sns.set()
        sns.set_theme(style="whitegrid")
        param_readable = parameter_print_names[parameter]
        _, ax1 = plt.subplots(figsize=(7, 6))

        max_value = max(runtimes_avg)
        ax1.set_ylim(0, max_value * 1.05)

        ax1.set_xlabel(param_readable)
        ax1.set_ylabel("Run-time [s]")
        ax1 = sns.scatterplot(x=parameter, y="runtime", data=data)
        max_value = max(runtimes_avg)
        ax1.set_ylim(0, max_value * 1.05)

        ax2 = ax1.twinx()
        ax2.set_ylabel("Number of rules")
        ax2 = sns.scatterplot(x=parameter, y="rules", data=data, color="black", marker="x")
        plt.title(f"Run-time w.r.t. {param_readable}\n{fixed_value_str}")
        max_rules = max(num_rules)
        ax2.set_ylim(0, max_rules * 1.05)

        r_t = mlines.Line2D([], [], color="blue", marker="o", label="Run-time", lw=0)
        n_r = mlines.Line2D([], [], color="black", marker="x", label="# rules", lw=0)

        plt.tight_layout()
        suffix = "" if seen_parameters[parameter] == 0 else f"_{seen_parameters[parameter]}"
        plt.legend(handles=[r_t, n_r])
        plt.savefig(os.path.join(output_path, f"{parameter}{suffix}.{file_extension}"), dpi=300)
        plt.close()
        seen_parameters[parameter] += 1


if __name__ == "__main__":
    args = parse_args()
    main(args["benchmark_file"], args["output_path"], args["format"])
