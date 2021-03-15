#!/usr/bin/python3

import argparse
import json
import matplotlib.pyplot as plt
import os
import re
import seaborn as sns
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + f"{os.sep}..")
from benchmark_histogram_creation import ExperimentConfig
from util.util import format_number, number_formatter, colors, markers


def parse_args():
    ap = argparse.ArgumentParser(description="Creates a histogram with the number of occurrences per change")
    ap.add_argument("change_file", type=str, help="Path to the index change -> list of dates ")
    ap.add_argument("output_path", type=str, help="Path to output directory")
    ap.add_argument(
        "--format", "-f", type=str, help="Plot file format", choices=["eps", "pdf", "png", "svg"], default="png"
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


def main(input_file, output_path, file_extension, num_bins, log_scale, show_title):
    with open(input_file) as f:
        changes = json.load(f)
    print(f"{len(changes)} changes loaded")
    change_counts = [len(occurrences) for occurrences in changes.values()]
    change_count_sum = sum(change_counts)
    print(f"{change_count_sum} occurrences; avg. {change_count_sum / len(changes)} occurrences/change")
    num_days = 359
    tick_count = min(num_bins, 12)
    ticks = [round(i * num_days / tick_count) for i in range(tick_count + 1)]

    if not os.path.isdir(output_path):
        os.mkdir(output_path)

    sns.set()
    sns.set_theme(style="whitegrid")
    plt.figure(figsize=(8, 3))
    plt.xlabel("number of occurrences")
    plt.ylabel("count")
    if show_title:
        plt.title("Distribution of change occurrences")
    ax = sns.histplot(data=change_counts, stat="count", bins=num_bins, color=colors()[0])
    plt.xticks(ticks)
    x_min = 0
    if log_scale:
        plt.yscale("log")
        x_min = 10 ** 0
    else:
        ax.get_yaxis().set_major_formatter(number_formatter())
    ax.set_ylim([x_min, ax.get_ylim()[1]])
    plt.tight_layout()
    plt.savefig(os.path.join(output_path, f"change_occurrences_{num_bins}-bins.{file_extension}"), dpi=300)
    plt.close()


if __name__ == "__main__":
    args = parse_args()
    main(args["change_file"], args["output_path"], args["format"], args["num_bins"], args["log_scale"], args["title"])
