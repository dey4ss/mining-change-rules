#!/usr/local/bin/python3

import argparse
import json
import math
import multiprocessing as mp
import os
from collections import defaultdict
from datetime import datetime

from util import Entity, date_range


def parse_args():
    min_supp_default = 0.1
    max_supp_default = 0.5
    min_conf_default = 0.7
    thread_default = 10  # unused for now, but there might be a case, e.g. chunks of all changes?
    bin_default = 11

    ap = argparse.ArgumentParser(
        description="Generates a dictionary of changes with their occurences, filtered by support."
    )
    ap.add_argument("change_dir", type=str, help="Directory of the change files.")
    ap.add_argument(
        "change_file",
        type=str,
        help="File with occurences per change (expect Python dict as .json)",
    )
    ap.add_argument(
        "--threads",
        type=int,
        help=f"Number of threads. Default {thread_default}",
        default=thread_default,
    )
    ap.add_argument(
        "--min_supp",
        type=float,
        help=f"Minimal support. Default {min_supp_default}",
        default=min_supp_default,
    )
    ap.add_argument(
        "--max_supp",
        type=float,
        help=f"Maximal support. Default {max_supp_default}",
        default=max_supp_default,
    )
    ap.add_argument(
        "--min_conf",
        type=float,
        help=f"Minimal Confidence. Default {min_conf_default}",
        default=min_conf_default,
    )
    ap.add_argument(
        "--num_bins",
        type=float,
        help=f"Bin count. Default {bin_default}",
        default=bin_default,
    )
    return vars(ap.parse_args())


def get_hist(all_changes, daily_changes, min_supp, max_supp, min_conf, days, num_days):
    hists = defaultdict(dict)
    actives = dict()
    already_done = defaultdict(set)
    print(f"start: {datetime.now()}")
    sum_days = len(days)
    support_threshold = math.ceil(min_supp * sum_days)

    for date, day_index in zip(days, range(sum_days)):
        print(date)
        active_today = dict()
        outdated = set()
        # update actives
        for change in daily_changes[date]:
            active_today[change] = 0
        for change in actives:
            actives[change] += 1
            if actives[change] >= num_days:
                outdated.add(change)
        actives.update(active_today)
        for change in outdated:
            del actives[change]
        # skip if min support cannot be reached
        can_shortcut_support = sum_days - day_index < num_days

        # begin with real work:
        for change in active_today:
            for other_change in actives:
                if other_change == change:
                    continue

                occurences_change = all_changes[change]
                occurences_other = all_changes[other_change]

                # skip if this has been done before:
                if change in hists[other_change] or change in already_done[other_change]:
                    continue

                # skip if min confidence cannot be reached
                confidence_threshold = math.floor(len(occurences_other) * min_conf)
                if confidence_threshold > len(occurences_change):
                    already_done[other_change].add(change)
                    continue

                if can_shortcut_support:
                    already_done[other_change].add(change)
                    continue

                # create empty histogram
                hist = [0 for _ in range(num_days)]

                # move two pointers to occurences of change and other change
                # to always add other change --> change to histogram
                ind_change = 0
                ind_other = 0
                while ind_other < len(occurences_other) and ind_change < len(occurences_change):
                    date_other = occurences_other[ind_other]
                    date_change = occurences_change[ind_change]
                    while date_change < date_other:
                        ind_change += 1
                        if ind_change >= len(occurences_change):
                            break
                        date_change = occurences_change[ind_change]
                    if ind_change >= len(occurences_change):
                        break
                    while ind_other < len(occurences_other) - 1 and occurences_other[ind_other + 1] <= date_change:
                        ind_other += 1
                        date_other = occurences_other[ind_other]
                    deviation = days.index(date_change) - days.index(date_other)
                    if deviation < num_days - 1:
                        hist[days.index(date_change) - days.index(date_other)] += 1
                    ind_other += 1
                    ind_change += 1
                # only add if satisfies min confidence
                change_count = sum(hist)
                if change_count >= max(support_threshold, confidence_threshold):
                    hists[other_change][change] = hist
                already_done[other_change].add(change)

    print(f"saving: {datetime.now()}")
    with open("histograms_tables.json", "w") as f:
        json.dump(hists, f)

    print(f"end: {datetime.now()}")


def main():
    args = parse_args()
    with open(args["change_file"]) as f:
        all_changes = json.load(f)
    daily_changes = defaultdict(set)
    actual_days = list(
        sorted({file_name[:10] for file_name in os.listdir(args["change_dir"]) if file_name.startswith("20")})
    )
    min_supp = args["min_supp"]
    support_threshold = math.ceil(min_supp * len(actual_days))
    too_infrequent_changes = set()
    for change, occurences in all_changes.items():
        if len(occurences) < support_threshold:
            too_infrequent_changes.add(change)
            continue
        for date in occurences:
            daily_changes[date].add(change)
    for change in too_infrequent_changes:
        del all_changes[change]

    get_hist(
        all_changes,
        daily_changes,
        min_supp,
        args["max_supp"],
        args["min_conf"],
        actual_days,
        args["num_bins"],
    )


if __name__ == "__main__":
    main()
