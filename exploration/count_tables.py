#!/usr/bin/python3

import argparse
import os
import sys


sys.path.append(os.path.dirname(os.path.abspath(__file__)) + f"{os.sep}..")
from util.util import date_range, file_extension


def parse_args():
    ap = argparse.ArgumentParser(description="Extracts change transactions")
    ap.add_argument("directory", type=str, help="Directory of the change files.")
    ap.add_argument("--start", type=str, help="Start date. Default 2020-05-01", default="2019-11-01")
    ap.add_argument("--end", type=str, help="End date. Default 2020-11-01", default="2020-11-02")
    return vars(ap.parse_args())


def find_changes(subdirs, path):
    tables = set()
    month = "1990-02"
    for subdir in subdirs:
        subdir_path = os.path.join(path, subdir)
        if not os.path.isdir(subdir_path):
            continue
        subdir_month = subdir[:7]
        if subdir_month > month:
            print(subdir_month)
            month = subdir_month
        day_tables = set([f for f in os.listdir(subdir_path) if f.endswith(file_extension())])
        tables = tables | day_tables
    print(f"\nNum tables: {len(tables)}")


def main():
    args = parse_args()
    subdirs = date_range(args["start"], args["end"])
    find_changes(
        subdirs, args["directory"],
    )


if __name__ == "__main__":
    main()
