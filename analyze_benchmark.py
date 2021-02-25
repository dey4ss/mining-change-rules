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

from benchmark_histogram_creation import ExperimentConfig


def benchmark_parameters():
    prohibited_parameters = {"timepoint_file", "output", "extensive_log", "change_file"}
    all_parameters = ExperimentConfig.default_values().keys()
    parameters = [p for p in all_parameters if p not in prohibited_parameters]
    parameters += ["input_size", "num_days"]
    return parameters


def parse_args():
    ap = argparse.ArgumentParser(description="Creates plots from a benchmark output file")
    ap.add_argument("benchmark_file", type=str, help="Path to an output file of benchmark_historgam_creation.py ")
    ap.add_argument("output_path", type=str, help="Path to output directory")
    ap.add_argument(
        "--format", "-f", type=str, help="Plot file format", choices=["eps", "pdf", "png", "svg"], default="png"
    )
    ap.add_argument(
        "--force_parameters",
        "-p",
        nargs="+",
        default=list(),
        help="Force parameters to be shown in plot title. Default none",
        choices=benchmark_parameters(),
    )
    ap.add_argument(
        "--second_measure",
        "-m",
        default=None,
        help="Second measure/y-axis. Default none",
        choices={"rules", "filtered_input"},
    )
    return vars(ap.parse_args())


def input_size_from_filename(file_path):
    file_name = file_path.split(os.sep)[-1]
    regex = r"\d+(?=_changes.json)"
    return int(re.search(regex, file_name).group())


def input_days_from_filename(file_path):
    file_name = file_path.split(os.sep)[-1]
    regex = r"\d+(?=_days)"
    return int(re.search(regex, file_name).group())


def main(input_file, output_path, file_extension, forced_parameters, second_measure):
    default_config = ExperimentConfig.default_values()
    del default_config["output"]
    del default_config["extensive_log"]
    del default_config["timepoint_file"]
    del default_config["change_file"]
    default_config["num_days"] = 359
    default_config["input_size"] = 1000

    parameter_print_names = {
        "input_size": "Number of changes",
        "min_conf": "Min confidence",
        "min_sup": "Min support",
        "max_sup": "Max support",
        "threads": "Number of threads",
        "num_bins": "Window size",
        "partition_size": "Partition size",
        "num_days": "Number of time points",
    }

    second_measure_readable = {
        "rules": ("# rules", "Number of rules"),
        "filtered_input": ("Filtered input size", "Number of changes"),
    }

    with open(input_file) as f:
        benchmark_results = json.load(f)

    if not os.path.isdir(output_path):
        os.mkdir(output_path)

    runsum = 0
    num_runs = benchmark_results["num_runs"]
    seen_parameters = defaultdict(int)
    for experiment in benchmark_results["experiments"]:
        results = experiment["results"]
        parameter = experiment["parameter"]
        parameter_disambiguation = parameter
        print(parameter)
        values = list()
        runtimes_avg = list()
        second_measure_values = list()

        fixed_values = experiment["fixed_values"]

        if parameter != "change_file":
            input_size = input_size_from_filename(fixed_values["change_file"])
            fixed_values["input_size"] = input_size
            del fixed_values["change_file"]

        for value, item_result in results.items():
            item_runtime = sum(item_result["runs"])
            if parameter != "change_file":
                parsed_value = float(value)
            else:
                input_size = input_size_from_filename(value)
                is_day_experiment = "days" in value
                if is_day_experiment:
                    fixed_values["input_size"] = input_size
                    parsed_value = input_days_from_filename(value)
                    parameter_disambiguation = "num_days"
                else:
                    parsed_value = input_size
                    parameter_disambiguation = "input_size"
            values.append(parsed_value)
            runtimes_avg.append(item_runtime / num_runs)
            runsum += item_runtime
            if second_measure:
                second_measure_values.append(item_result[second_measure])

        fixed_values = {k: v for k, v in fixed_values.items() if v != default_config[k] or k in forced_parameters}
        plot_values = {parameter_disambiguation: values, "runtime": runtimes_avg}
        if second_measure:
            plot_values[second_measure] = second_measure_values
        plot_data = pd.DataFrame(plot_values)

        fixed_value_str = ", ".join([f"{k}={v}" for k, v in fixed_values.items()])
        sns.set()
        sns.set_theme(style="whitegrid")
        param_readable = parameter_print_names[parameter_disambiguation]
        _, ax1 = plt.subplots(figsize=(7, 6))

        max_value = max(runtimes_avg)
        ax1.set_ylim(0, max_value * 1.05)

        ax1.set_xlabel(param_readable)
        ax1.set_ylabel("Run-time [s]")
        ax1 = sns.scatterplot(x=parameter_disambiguation, y="runtime", data=plot_data)
        max_value = max(runtimes_avg)
        ax1.set_ylim(0, max_value * 1.05)

        legend = [mlines.Line2D([], [], color="blue", marker="o", label="Run-time", lw=0)]
        plt.title(f"Run-time w.r.t. {param_readable}\n{fixed_value_str}")

        if second_measure:
            label = second_measure_readable[second_measure][1]
            legend_entry = second_measure_readable[second_measure][0]
            ax2 = ax1.twinx()
            ax2.set_ylabel(label)
            ax2 = sns.scatterplot(
                x=parameter_disambiguation, y=second_measure, data=plot_data, color="black", marker="x"
            )
            max_second_measure = max(second_measure_values)
            ax2.set_ylim(0, max_second_measure * 1.05)
            legend.append(mlines.Line2D([], [], color="black", marker="x", label=legend_entry, lw=0))

        plt.tight_layout()
        suffix = (
            "" if seen_parameters[parameter_disambiguation] == 0 else f"_{seen_parameters[parameter_disambiguation]}"
        )
        if second_measure:
            plt.legend(handles=legend)
        plt.savefig(os.path.join(output_path, f"{parameter_disambiguation}{suffix}.{file_extension}"), dpi=300)
        plt.close()
        seen_parameters[parameter_disambiguation] += 1


if __name__ == "__main__":
    args = parse_args()
    main(args["benchmark_file"], args["output_path"], args["format"], args["force_parameters"], args["second_measure"])
