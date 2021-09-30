#!/usr/bin/python3

import argparse
import json
import os
import sys
import math
from datetime import datetime

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + f"{os.sep}..")
from util.util import date_range


def parse_args():
    ap = argparse.ArgumentParser(description="Limits extracted changes to all subsequent two years.")
    ap.add_argument("change_file", type=str, help="File with changes.")
    ap.add_argument("out", type=str, help="Output directory.")
    ap.add_argument(
        "--min_sup",
        "-l",
        type=float,
        help=f"Minimal support (lower bound). Default 0",
        default=0,
    )
    ap.add_argument(
        "--max_sup",
        "-u",
        type=float,
        help=f"Maximal support (upper bound). Default 1",
        default=1,
    )
    return vars(ap.parse_args())


def main(change_file, out, min_sup, max_sup):
    if not os.path.isdir(out):
        os.makedirs(out)

    with open(change_file, encoding="utf-8") as f:
        all_changes = json.load(f)

    years = [str(year) for year in range(2015, 2020)]
    periods = [4]
    for period in periods:
        sub = period - 1
        for start, end in zip(years[:-sub], years[sub:]):
            limit_changes = dict()
            num_tp = len(date_range(f"{start}-01-01", f"{end}-12-31"))
            min_s = math.ceil(num_tp * min_sup)
            max_s = math.floor(num_tp * max_sup)
            print(f"{start}-{end}")
            for change, occurrences in all_changes.items():
                if occurrences[0][:4] > end or occurrences[-1][:4] < start:
                    continue
                limit_occurrences = [
                    occurrence for occurrence in occurrences if occurrence[:4] >= start and occurrence[:4] <= end
                ]

                # there can still be no occurrences in the period
                # e.g. period 2007/2008, occurrences in 2006 and 2009
                # also, support can be wrong
                if len(limit_occurrences) < 1 or len(limit_occurrences) < min_s or len(limit_occurrences) > max_s:
                    continue
                limit_changes[change] = limit_occurrences
            file_name = f"wiki_changes_{start}-{end}.json"
            with open(os.path.join(out, file_name), "w", encoding="utf-8") as f:
                json.dump(limit_changes, f)


if __name__ == "__main__":
    args = parse_args()
    start = datetime.now()
    print("start:", start)

    main(args["change_file"], args["out"], args["min_sup"], args["max_sup"])

    end = datetime.now()
    print("end:", end)
    print("duration:", end - start)
