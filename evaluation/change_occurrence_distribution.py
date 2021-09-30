#!/usr/bin/python3

import argparse
import json
import matplotlib.pyplot as plt
import numpy as np
import os
import re
import seaborn as sns
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + f"{os.sep}..")
from benchmark_histogram_creation import ExperimentConfig
from util.util import format_number, number_formatter, colors, markers, date_range


def parse_args():
    ap = argparse.ArgumentParser(description="Creates a histogram with the number of occurrences per change")
    ap.add_argument("dataset", type=str, choices=["wiki", "socrata"])
    ap.add_argument("change_file", type=str, help="Path to the index change -> list of dates ")
    ap.add_argument("output_path", type=str, help="Path to output directory")
    ap.add_argument(
        "--format",
        "-f",
        type=str,
        nargs="+",
        help="Plot file format",
        choices=["eps", "pdf", "png", "svg"],
        default=["png"],
    )
    ap.add_argument("--num_bins", "-b", type=int, help="Number of bins. Default 10", default=10)
    ap.add_argument("--log_scale", "-l", action="store_true", help="Log scale for y axis")
    ap.add_argument(
        "--title",
        "-t",
        action="store_true",
        help="Add title",
    )
    return vars(ap.parse_args())


def main(dataset, input_file, output_path, file_extensions, num_bins, log_scale, show_title):
    with open(input_file) as f:
        changes = json.load(f)
    print(f"{len(changes)} changes loaded")

    change_counts = [len(occurrences) for occurrences in changes.values()]
    hours = [str(h).rjust(2) for h in range(25)]
    num_days = len(date_range(min_date, max_date))
    num_days = num_days if dataset == "wiki" else 359
    max_occurrences = max(change_counts)
    print(
        f"{sum(change_counts)} occurrences; avg. {np.mean(change_counts)} occurrences/change (median {np.median(change_counts)}); max. {max_occurrences}"
    )
    rare_item_sum = 0
    for n in range(1, 21):
        items_n_changes = sum([1 for x in change_counts if x == n])
        rare_item_sum += items_n_changes
        print(
            f"{100 * items_n_changes / len(changes)} % ({items_n_changes}) only have {n} occurrence(s); {100 * rare_item_sum / len(changes)} % ({rare_item_sum}) at most {n}"
        )

    if not os.path.isdir(output_path):
        os.mkdir(output_path)

    sns.set()
    sns.set_theme(style="whitegrid")
    plt.figure(figsize=(8, 3))
    plt.xlabel("number of occurrences")
    plt.ylabel("number of change patterns")
    if show_title:
        plt.title("Distribution of change occurrences")
    ax = sns.histplot(data=change_counts, stat="count", binwidth=10, color=colors()[0])
    if log_scale:
        plt.yscale("symlog")
    else:
        ax.get_yaxis().set_major_formatter(number_formatter())
    ax.set_ylim([0, ax.get_ylim()[1]])
    ax.set_xlim([0, max_occurrences + 1])

    plt.tight_layout(pad=0)
    log_indicator = "_log" if log_scale else ""
    for file_extension in file_extensions:
        plt.savefig(os.path.join(output_path, f"change_occurrences_{dataset}{log_indicator}.{file_extension}"), dpi=300)
    plt.close()


if __name__ == "__main__":
    args = parse_args()
    main(
        args["dataset"],
        args["change_file"],
        args["output_path"],
        args["format"],
        args["num_bins"],
        args["log_scale"],
        args["title"],
    )
