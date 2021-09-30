#!/usr/bin/python3

import argparse
import json
import matplotlib.pyplot as plt
import matplotlib.lines as mlines
import os
import re
import seaborn as sns
import sys
from collections import defaultdict


sys.path.append(os.path.dirname(os.path.abspath(__file__)) + f"{os.sep}..")
from benchmark_histogram_creation import ExperimentConfig
from util.util import format_number, number_formatter, colors, markers


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
        "--second_data",
        "-d",
        type=str,
        default=None,
        help="Path to second result file to compare with.",
    )
    ap.add_argument(
        "--merge_multiple",
        "-m",
        action="store_true",
        help="Merge all .json files in given directory",
    )
    ap.add_argument(
        "--title",
        "-t",
        action="store_true",
        help="Add title",
    )
    ap.add_argument(
        "--caption",
        "-c",
        action="store_true",
        help="Add caption",
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
        "rules": ("# rules", "number of rules"),
        "filtered_input": ("filtered input size", "number of changes"),
        "input_size": ("# changes", "number of changes"),
        "min_conf": ("min conf.", "minimum confidence"),
        "min_sup": ("min sup.", "minimum support"),
        "max_sup": ("max sup.", "maximum support"),
        "threads": ("# threads", "number of threads"),
        "num_bins": ("window size", "window size"),
        "partition_size": ("partition size", "partition size"),
        "num_days": ("# time points", "number of time points"),
    }


def make_plot(
    variable,
    plot_data,
    fixed_values,
    add_title,
    add_caption,
    second_measure=None,
    second_measure_values=None,
    variable_names=None,
    log_scale=False,
    sources=None,
    hide_legend=False,
):
    sns.set()
    sns.set_theme(style="whitegrid")
    print_names = parameter_print_names()
    param_readable = variable if variable_names else print_names[variable][1]

    if type(plot_data) == tuple or variable_names is None:
        plot_data = [plot_data]
    if variable_names is None:
        canonical_fixed_values = fixed_values
    else:
        canonical_fixed_values = fixed_values[0]
        for v in fixed_values:
            if v != canonical_fixed_values:
                canonical_fixed_values = {}

    height = 4 if add_title or add_caption else 3
    width = 8 if variable == "partition size" else 5.33
    a, ax1 = plt.subplots(figsize=(width, height))

    max_value = 0
    fixed_value_str = ", ".join([f"{k}={v}" for k, v in canonical_fixed_values.items()])
    data_ids = len(variable_names) if variable_names else 1
    x_decimals = any([any([x % 1 != 0 for x in data[0]]) for data in plot_data])
    x_decimals = 1 if x_decimals else 0
    ax1.get_xaxis().set_major_formatter(number_formatter(decimals=x_decimals))
    ax1.get_yaxis().set_major_formatter(number_formatter())
    ax1_handles = list()

    for data, marker, color, i in zip(plot_data, markers(), colors(), range(data_ids)):
        x = data[0]
        y = data[1]
        if variable == "num_days":
            max_x = max(x)
            x = [0 if x_val == 0 else x_val / max_x for x_val in x]
            ax1.get_xaxis().set_major_formatter(number_formatter(decimals=1))
        max_value = max(max(y), max_value)
        variable_name = variable_names[i] if variable_names else None
        variable_printable = print_names[variable_name][0] if variable_name in print_names else variable_name
        label = variable_printable if variable_names else None
        sns.lineplot(x=x, y=y, marker=marker, color=color, label=label, mew=0)
        ax1_handles.append(mlines.Line2D([], [], color=color, marker=marker))
    if add_title:
        plt.title(f"Run-time w.r.t. {param_readable}\n{fixed_value_str}")
    if add_caption:
        ax1.set_xlabel(param_readable)
    ax1.set_ylabel("Socrata, run-time [s]")
    ax1.legend(frameon=False, handles=ax1_handles, loc="center left")

    if second_measure:
        if len(plot_data) > 1:
            raise ValueError("Second measure is only supported for a single dataset.")
        label = "Wikipedia, run-time [s]"
        legend_entry = param_readable
        ax2 = ax1.twinx()
        ax2.set_ylabel(label)
        print(label)
        x = second_measure_values[0]
        if variable == "num_days":
            max_x = max(x)
            x = [0 if x_val == 0 else x_val / max_x for x_val in x]
            print(x)

        color = colors()[len(plot_data)]
        marker = markers()[len(plot_data)]
        ax2 = sns.lineplot(x=x, y=second_measure_values[1], color=color, marker=marker, mew=0)
        max_second_measure = max(second_measure_values[1])
        ax2.set_ylim(0, max_second_measure * 1.05)
        ax2.legend(frameon=False, loc="center right", handles=[mlines.Line2D([], [], color=color, marker=marker)])
        if variable == "input_size":
            ax2.get_yaxis().set_major_formatter(number_formatter())
            ax2.set_ylim(0, max_value * 1.05)
        else:
            ax2.get_yaxis().set_major_formatter(number_formatter(decimals=1))

    if variable == "partition size":
        plt.legend(ncol=2)
        plt.yscale("log")
        ax1.set_ylim([10 ** 0, ax1.get_ylim()[1]])
    else:
        ax1.set_ylim(0, max_value * 1.05)
    ax1.set_xlim(0, ax1.get_xlim()[1])
    plt.tight_layout()


def plot_multiple(experiments, title, add_title, add_caption):
    plot_data = list()
    variable_names = list()
    fixed_values = list()
    for e in experiments:
        variable = e[0]
        if title == "partition size":
            if "input_size" in e[1]:
                size = e[1]["input_size"]
            else:
                size = 1000
            variable = f"{format_number(size)} changes"
        variable_names.append(variable)
        fixed_values.append(e[1])
        plot_data.append(e[2])
    make_plot(title, plot_data, fixed_values, add_title, add_caption, variable_names=variable_names)


def main(
    input_path,
    output_path,
    file_extension,
    forced_parameters,
    second_measure,
    merge,
    add_title,
    add_caption,
    second_data_path,
):
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

    if second_data_path:
        with open(second_data_path) as f:
            compare_data = json.load(f)

    if not os.path.isdir(output_path):
        os.mkdir(output_path)

    num_runs = benchmark_results["num_runs"]
    seen_parameters = defaultdict(int)
    for experiment in benchmark_results["experiments"]:
        results = experiment["results"]
        parameter = experiment["parameter"]
        if parameter == "threads":
            continue
        parameter_disambiguation = parameter
        print(parameter)
        values = list()
        runtimes_avg = list()
        second_measure_values = list()

        fixed_values = list()

        fixed_values.append(experiment["fixed_values"])

        if parameter != "change_file":
            input_size = input_size_from_filename(fixed_values[0]["change_file"])
            fixed_values[0]["input_size"] = input_size
            del fixed_values[0]["change_file"]

        data = [(num_runs, results, fixed_values[0])]
        plot_data = [(list(), list())]
        is_time_experiment = lambda x: "days" in x

        if second_data_path:
            if parameter == "partition_size":
                continue
            compare_runs = compare_data["num_runs"]
            compare_results = None
            for compare_experiment in compare_data["experiments"]:
                compare_parameter = compare_experiment["parameter"]
                if compare_parameter != parameter:
                    continue
                compare_results = compare_experiment["results"]
                if parameter != "change_file":
                    break
                if is_time_experiment(list(results.keys())[0]) == is_time_experiment(list(compare_results.keys())[0]):
                    break
                compare_results = None

            if not compare_results:
                print(f"\tCould not find compare values for {parameter}")
                continue
            fixed_values.append(dict())
            data.append((compare_runs, compare_results, fixed_values[1]))
            plot_data.append((list(), list()))

        for my_data, my_plot_data in zip(data, plot_data):
            for value, item_result in my_data[1].items():
                item_runtime = sum(item_result["runs"])
                if parameter != "change_file":
                    parsed_value = float(value)
                else:
                    input_size = input_size_from_filename(value)
                    if is_time_experiment(value):
                        my_data[2]["input_size"] = input_size
                        parsed_value = input_days_from_filename(value)
                        parameter_disambiguation = "num_days"
                    else:
                        parsed_value = input_size
                        parameter_disambiguation = "input_size"
                my_plot_data[0].append(parsed_value)
                my_plot_data[1].append(item_runtime / my_data[0])

        for f_v in fixed_values:
            f_v = {k: v for k, v in f_v.items() if v != default_config[k] or k in forced_parameters}
        variable_names = ["data_one", "data_two"] if len(plot_data) > 1 else None
        hide_legend = len(plot_data) > 1
        if len(plot_data) == 1:
            plot_data = plot_data[0]
        elif len(plot_data) == 2:
            second_measure = True
            second_measure_values = plot_data[1]
            plot_data = plot_data[0]
        if len(fixed_values) == 1:
            fixed_values = fixed_values[0]
        make_plot(
            parameter_disambiguation,
            plot_data,
            fixed_values,
            add_title,
            add_caption,
            second_measure=second_measure,
            second_measure_values=second_measure_values,
            variable_names=variable_names,
            hide_legend=hide_legend,
        )
        suffix = (
            "" if seen_parameters[parameter_disambiguation] == 0 else f"_{seen_parameters[parameter_disambiguation]}"
        )
        print(f"\t--> {parameter_disambiguation}{suffix}.{file_extension}")
        plt.savefig(
            os.path.join(output_path, f"{parameter_disambiguation}{suffix}.{file_extension}"), dpi=300, transparent=True
        )
        plt.close()
        print("transparent")
        seen_parameters[parameter_disambiguation] += 1
        parsed_experiments.append((parameter_disambiguation, fixed_values, plot_data))

    if second_data_path:
        return

    print("sup + conf")
    sup_experiments = [e for e in parsed_experiments if e[0].endswith("sup") or e[0].endswith("conf")]
    sup_experiments.sort(key=lambda e: e[0] if "sup" in e[0] else "z", reverse=True)
    if len(sup_experiments) > 1:
        plot_multiple(sup_experiments, "threshold", add_title, add_caption)
        plt.savefig(os.path.join(output_path, f"sup_conf.{file_extension}"), dpi=300)
        plt.close()

    print("partition sizes")
    partition_experiments = [e for e in parsed_experiments if e[0] == "partition_size"]
    partition_experiments.sort(key=lambda e: e[1]["input_size"] if "input_size" in e[1] else 1000, reverse=True)
    if len(partition_experiments) > 1:
        plot_multiple(partition_experiments, "partition size", add_title, add_caption)
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
        args["title"],
        args["caption"],
        args["second_data"],
    )
