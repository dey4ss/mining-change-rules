#!/usr/bin/python3

import argparse
import json
import os
import sys
import math
from collections import defaultdict
from datetime import datetime

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + f"{os.sep}..")
from util.util import date_range

def parse_args():
    ap = argparse.ArgumentParser(description="Filters changes of Wikipedia infoboxes.")
    ap.add_argument("change_file", type=str, help="JSON dictionary of changes with their occurrences.")
    ap.add_argument("out", type=str, help="Output file.")
    ap.add_argument("--min_sup", type=float, help="Minimum support. Default 0.05", default=0.05)
    ap.add_argument("--max_sup", type=float, help="maximum support. Default 0.1", default=0.1)
    return vars(ap.parse_args())


def main(change_file, out, min_sup, max_sup):
    with open(change_file, encoding="utf-8") as f:
        all_changes = json.load(f)

    #all_changes_dates = dict()
    print(f"input: {len(all_changes)} changes")

    min_date = "2020-11-11"
    max_date = "1900-01-01"

    for change, occurrences in all_changes.items():
        # occurrence_dates = sorted(set([date[:10] for date in occurrences]))
        #all_changes_dates[change] = occurrence_dates
        min_date = min(min_date, occurrences[0])
        max_date = max(max_date, occurrences[-1])

    #del all_changes
    all_dates = date_range(min_date, max_date)

    print(min_date, max_date)
    min_sup_threshold = math.ceil(len(all_dates) * min_sup)
    max_sup_threshold = math.floor(len(all_dates) * max_sup)

    print(min_sup_threshold, max_sup_threshold)
    skip_changes = list()
    for change, occurrences in all_changes.items():
        if len(occurrences) > max_sup_threshold or len(occurrences) < min_sup_threshold:
            skip_changes.append(change)

    for change in skip_changes:
        del all_changes[change]

    print(f"{len(all_changes)} changes remaining with min sup {min_sup} and max sup {max_sup}")
    with open(out, "w", encoding="utf-8") as f:
        json.dump(all_changes, f)
    with open(f"{out}.dates.json", "w", encoding="utf-8") as f:
        json.dump(all_dates, f)


if __name__ == "__main__":
    args = parse_args()
    main(args["change_file"], args["out"], args["min_sup"], args["max_sup"])
