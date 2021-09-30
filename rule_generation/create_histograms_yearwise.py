#!/usr/bin/python3

import argparse
import json
import math
import multiprocessing as mp
import os
import random
import re
import sys
import yaml
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime
from time import time

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + f"{os.sep}..")
from rule_generation.create_histograms import create_histograms
from util.util import date_range


def parse_args():
    ap = argparse.ArgumentParser(description="Runs histogram creation for subsequent years")
    ap.add_argument("change_dir", type=str, help="Files with index change -> occurrences as Python JSON dictionary")
    ap.add_argument("output", type=str, help="Output path. Default none, results are printed to console")
    ap.add_argument("granularity", type=str, help=f"Temporal granularity of changes", choices=["d", "h"])
    ap.add_argument(
        "--min_sup",
        "-l",
        type=float,
        help=f"Minimal support (lower bound). Default 0.05",
        default=0.1,
    )
    ap.add_argument(
        "--max_sup",
        "-u",
        type=float,
        help=f"Maximal support (upper bound). Default 0.1",
        default=0.5,
    )
    ap.add_argument(
        "--min_conf",
        "-c",
        type=float,
        help=f"Minimal Confidence. Default 0.9",
        default=0.8,
    )
    ap.add_argument(
        "--infoboxes",
        "-i",
        type=str,
        help=f"YAML file with infobox whitelist and categories. Default None",
        default=None,
    )

    return vars(ap.parse_args())


def to_str(n):
    return str(n).replace(".", "")


def main(change_dir, out, min_conf, min_sup, max_sup, granularity, whitelists_file):
    if not os.path.isdir(out):
        os.makedirs(out)

    sups = [0.025] + [x / 100 for x in range(5, 41, 5)]

    input_files = [f"wiki_changes_{y}-{y+1}.json" for y in range(2015, 2018)]
    input_files = sorted(os.listdir(change_dir))
    year_pattern = re.compile(r"(?<=wiki_changes_)\d{4}-\d{4}(?=.json)")

    params = {
        "change_file": None,
        "timepoint_file": None,
        "output": None,
        "min_sup": min_sup,
        "max_sup": max_sup,
        "min_conf": min_conf,
        "threads": 10,
        "partition_size": 10000,
        "num_bins": 32,
        "extensive_log": False,
        "whitelist": None,
    }

    whitelists = [None]
    if whitelists_file:
        with open(whitelists_file) as f:
            whitelists = yaml.load(f, Loader=yaml.CLoader)

    for input_file in input_files:
        file_path = os.path.join(change_dir, input_file)
        print(f"Loading changes")
        with open(file_path) as f:
            all_changes = json.load(f)
        for whitelist in whitelists:
            if not whitelist:
                continue
            print(f"Preparing changes for {whitelist}")
            my_categories = whitelists[whitelist]
            my_changes = dict()
            for change, occurrences in all_changes.items():
                if change.split("_")[0] in my_categories:
                    my_changes[change] = occurrences
            f_name = f"{file_path}.changes_wl-{whitelist}.json"
            with open(f_name, "w") as f:
                json.dump(my_changes, f)
    del all_changes

    for file_name in input_files:
        print(f"\n{file_name}")
        file_path = os.path.join(change_dir, file_name)
        years = year_pattern.search(file_name).group(0).split("-")
        dates = date_range(f"{years[0]}-01-01", f"{years[1]}-12-31")
        sups = [min(0.025, 20 / len(dates))] + [x / 100 for x in range(5, 41, 5)]
        if granularity == "h":
            all_hours = list()
            hours = [str(i).rjust(2, "0") for i in range(25)]
            for date in dates:
                date_hours = [f"{date}T{hour}" for hour in hours]
                all_hours += date_hours
            dates = all_hours

        granularity_index = "days" if granularity == "d" else "hours"
        days_file = f"{file_path}.{granularity_index}.json"
        with open(days_file, "w", encoding="utf-8") as f:
            json.dump(dates, f)

        bins = [7, 31] if granularity == "d" else [24]
        for n_b in bins:
            start = datetime.now()
            print("\nSTART:", start)
            print(f"\n\twindow size {n_b}")
            params["num_bins"] = n_b

            for whitelist in whitelists:
                whitelist_file = ""
                if whitelist:
                    whitelist_file = f"{file_path}.wl-{whitelist}.json"
                    with open(whitelist_file, "w") as f:
                        json.dump(whitelists[whitelist], f)
                    params["whitelist"] = whitelist_file
                    whitelist_ext = f"_{whitelist}_"
                    params["change_file"] = f"{file_path}.changes_wl-{whitelist}.json"
                else:
                    params["whitelist"] = None
                    whitelist_ext = ""
                    params["change_file"] = file_path

                for min_s, max_s in zip(sups[:-1], sups[1:]):
                    print(f"\n\t\t{whitelist}  {min_s} <= sup <= {max_s}")
                    if granularity == "h":
                        min_s = (1 / 24) * min_s
                        max_s = (1 / 24) * max_s
                        print(f"\t\t({min_s} <= sup <= {max_s})")
                    out_file = os.path.join(
                        out,
                        f"{file_name}.dependencies{whitelist_ext}_{to_str(min_s)}_{to_str(max_s)}_conf_{to_str(min_conf)}_{n_b}_bins.csv",
                    )
                    params["min_sup"] = min_s
                    params["max_sup"] = max_s
                    params["timepoint_file"] = days_file
                    params["output"] = out_file

                    p = mp.Process(target=create_histograms, args=[params])
                    p.start()
                    p.join()
                    print("\n")
                if whitelist:
                    os.remove(whitelist_file)
            end = datetime.now()
            print("\nEND:", end)
            print("DURATION:", end - start)
        os.remove(days_file)


if __name__ == "__main__":
    args = parse_args()
    main(
        args["change_dir"],
        args["output"],
        args["min_conf"],
        args["min_sup"],
        args["max_sup"],
        args["granularity"],
        args["infoboxes"],
    )
