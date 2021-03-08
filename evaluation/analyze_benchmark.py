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


def parse_args():
    ap = argparse.ArgumentParser(description="Creates plots from a benchmark output file")
    ap.add_argument(
        "input",
        type=str,
        help="Path to an output file of benchmark_historgam_creation.py or to a directory containing such files",
    )
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
        "-s",
        default=None,
        help="Second measure/y-axis. Default none",
        choices={"rules", "filtered_input"},
    )
    ap.add_argument(
        "--merge_multiple",
        "-m",
        action="store_true",
        help="Merge all .json files in given directory",
    )
    return vars(ap.parse_args())


def benchmark_parameters():
    prohibited_parameters = {"timepoint_file", "output", "extensive_log", "change_file"}
    all_parameters = ExperimentConfig.default_values().keys()
    parameters = [p for p in all_parameters if p not in prohibited_parameters]
    parameters += ["input_size", "num_days"]
    return parameters


def input_size_from_filename(file_path):
    file_name = file_path.split(os.sep)[-1]
    regex = r"\d+(?=_changes.json)"
    return int(re.search(regex, file_name).group())


def input_days_from_filename(file_path):
    file_name = file_path.split(os.sep)[-1]
    regex = r"\d+(?=_days)"
    return int(re.search(regex, file_name).group())


def parameter_print_names():
    return {
        "input_size": "Number of changes",
        "min_conf": "Min confidence",
        "min_sup": "Min support",
        "max_sup": "Max support",
        "threads": "Number of threads",
        "num_bins": "Window size",
        "partition_size": "Partition size",
        "num_days": "Number of time points",
    }


def second_measure_readable():
    return {
        "rules": ("# rules", "Number of rules"),
        "filtered_input": ("Filtered input size", "Number of changes"),
    }


def make_plot(
    variable,
    plot_data,
    fixed_values,
    second_measure=None,
    second_measure_values=None,
    variable_names=None,
    log_scale=False,
):
    markers = ["^", "X", "o", "D"]
    colors = ["blue", "orange", "green", "red"]
    sns.set()
    sns.set_theme(style="whitegrid")
    param_readable = variable if variable_names else parameter_print_names()[variable]
    _, ax1 = plt.subplots(figsize=(6, 5))

    if variable_names is None:
        plot_data = [plot_data]
        canonical_fixed_values = fixed_values
    else:
        canonical_fixed_values = fixed_values[0]
        for v in fixed_values:
            if v != canonical_fixed_values:
                canonical_fixed_values = {}

    max_value = 0
    fixed_value_str = ", ".join([f"{k}={v}" for k, v in canonical_fixed_values.items()])
    data_ids = len(variable_names) if variable_names else 1
    print_names = parameter_print_names()

    for data, marker, color, i in zip(plot_data, markers, colors, range(data_ids)):
        x = data[0]
        y = data[1]
        max_value = max(max(y), max_value)
        variable_name = variable_names[i] if variable_names else None
        variable_printable = print_names[variable_name] if variable_name in print_names else variable_name
        label = variable_printable if variable_names else None
        ax1 = sns.lineplot(x=x, y=y, marker=marker, color=color, label=label, mew=0)
        legend = [mlines.Line2D([], [], color=color, marker=marker, label="Run-time")]
    plt.title(f"Run-time w.r.t. {param_readable}\n{fixed_value_str}")
    ax1.set_xlabel(param_readable)
    ax1.set_ylabel("Run-time [s]")
    ax1.set_ylim(0, max_value * 1.05)

    if second_measure:
        if len(plot_data) > 1:
            raise ValueError("Second measure is only supported for a single dataset.")
        label = second_measure_readable()[second_measure][1]
        legend_entry = second_measure_readable()[second_measure][0]
        ax2 = ax1.twinx()
        ax2.set_ylabel(label)
        ax2 = sns.lineplot(x=second_measure_values[0], y=second_measure_values[1], color="black", marker="x", mew=0)
        max_second_measure = max(second_measure_values[1])
        ax2.set_ylim(0, max_second_measure * 1.05)
        legend.append(mlines.Line2D([], [], color="black", marker="x", label=legend_entry))

    plt.tight_layout()
    if second_measure:
        plt.legend(handles=legend)


def plot_multiple(experiments, title):
    plot_data = list()
    variable_names = list()
    fixed_values = list()
    for e in experiments:
        variable = e[0]
        if title == "Partition Size":
            if "input_size" in e[1]:
                size = e[1]["input_size"]
            else:
                size = 1000
            variable = f"{size} changes"
        variable_names.append(variable)
        fixed_values.append(e[1])
        plot_data.append(e[2])
    make_plot(title, plot_data, fixed_values, variable_names=variable_names)


def main(input_path, output_path, file_extension, forced_parameters, second_measure, merge):
    default_config = ExperimentConfig.default_values()
    del default_config["output"]
    del default_config["extensive_log"]
    del default_config["timepoint_file"]
    del default_config["change_file"]
    default_config["num_days"] = 359
    default_config["input_size"] = 1000
    parsed_experiments = list()

    if merge:
        num_runs = 0
        experiments = list()
        files = [f for f in os.listdir(input_path) if f.endswith(".json")]
        for file_name in files:
            file_path = os.path.join(input_path, file_name)
            with open(file_path) as f:
                experiment = json.load(f)
            my_runs = experiment["num_runs"]
            if len(experiments) == 0:
                num_runs = my_runs
            elif my_runs != num_runs:
                raise ValueError(f"{file_path} has differing num of runs (was {num_runs}, is {my_runs}).")
            experiments += experiment["experiments"]

        benchmark_results = {"num_runs": num_runs, "experiments": experiments}
    else:
        with open(input_path) as f:
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
        plot_data = (values, runtimes_avg)
        make_plot(
            parameter_disambiguation,
            plot_data,
            fixed_values,
            second_measure=second_measure,
            second_measure_values=(values, second_measure_values),
        )
        suffix = (
            "" if seen_parameters[parameter_disambiguation] == 0 else f"_{seen_parameters[parameter_disambiguation]}"
        )
        plt.savefig(os.path.join(output_path, f"{parameter_disambiguation}{suffix}.{file_extension}"), dpi=300)
        plt.close()
        seen_parameters[parameter_disambiguation] += 1
        parsed_experiments.append((parameter_disambiguation, fixed_values, plot_data))

    sup_experiments = [e for e in parsed_experiments if e[0].endswith("sup")]
    if len(sup_experiments) > 1:
        plot_multiple(sup_experiments, "Support")
        plt.savefig(os.path.join(output_path, f"support_all.{file_extension}"), dpi=300)
        plt.close()

    partition_experiments = [e for e in parsed_experiments if e[0] == "partition_size"]
    partition_experiments.sort(key=lambda e: e[1]["input_size"] if "input_size" in e[1] else 1000, reverse=True)
    if len(partition_experiments) > 1:
        plot_multiple(partition_experiments, "Partition Size")
        plt.savefig(os.path.join(output_path, f"partition_size_all.{file_extension}"), dpi=300)
        plt.close()


if __name__ == "__main__":
    args = parse_args()
    main(
        args["input"],
        args["output_path"],
        args["format"],
        args["force_parameters"],
        args["second_measure"],
        args["merge_multiple"],
    )
