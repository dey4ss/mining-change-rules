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
    ap = argparse.ArgumentParser(description="Creates a histogram with the number of occurrences per change")
    ap.add_argument("change_file", type=str, help="Path to the index change -> list of dates ")
    ap.add_argument("output_path", type=str, help="Path to output directory")
    ap.add_argument(
        "--format", "-f", type=str, help="Plot file format", choices=["eps", "pdf", "png", "svg"], default="png"
    )
    ap.add_argument("--num_bins", "-b", type=int, help="Number of bins. Default 10", default=10)
    ap.add_argument("--log_scale", "-l", action="store_true", help="Log scale for y axis")
    return vars(ap.parse_args())


def main(input_file, output_path, file_extension, num_bins, log_scale):
    with open(input_file) as f:
        changes = json.load(f)
    print(f"{len(changes)} changes loaded")
    change_counts = [len(occurrences) for occurrences in changes.values()]
    num_days = 359
    tick_count = min(num_bins, 12)
    ticks = [round(i * num_days / tick_count) for i in range(tick_count + 1)]

    if not os.path.isdir(output_path):
        os.mkdir(output_path)

    sns.set()
    sns.set_theme(style="whitegrid")
    plt.xlabel("number of occurrences")
    plt.ylabel("count")
    plt.title("Distribution of change occurrences")
    sns.histplot(data=change_counts, stat="count", bins=num_bins)
    plt.xticks(ticks)
    if log_scale:
        plt.yscale("log")
    plt.tight_layout()
    plt.savefig(os.path.join(output_path, f"change_occurrences_{num_bins}-bins.{file_extension}"), dpi=300)
    plt.close()


if __name__ == "__main__":
    args = parse_args()
    main(args["change_file"], args["output_path"], args["format"], args["num_bins"], args["log_scale"])
