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


class Histogram:
    def __init__(self):
        self._is_setup = False
        self._is_active = True
        self._actual_right = 0
        self._count_left = 0

    def is_setup(self):
        return self._is_setup

    def is_active(self):
        return self._is_active

    def setup(self, bin_count, occurences_right):
        if self._is_setup:
            raise RuntimeError(f"{self.__class__.__name__} can only be set up once.")
        if not self._is_active:
            raise RuntimeError(f"{self.__class__.__name__} has been inactivated")
        self._bins = [0 for _ in range(bin_count)]
        self._count_right = occurences_right
        self._is_setup = True

    def add_occurence(self, bin):
        if not self._is_setup:
            raise RuntimeError(f"{self.__class__.__name__} needs to be set up first.")
        if not self._is_active:
            raise RuntimeError(f"{self.__class__.__name__} has been inactivated")
        self._bins[bin] += 1
        self._count_left += 1

    def add_right_occurence(self):
        self._actual_right += 1

    def right_occurences(self):
        return self._actual_right

    def bins(self):
        return self._bins

    def inactivate(self):
        self._bins = None
        self._is_active = False

    def confidence(self):
        return self._count_left / self._count_right

    def abs_support(self):
        return self._count_left


def get_hist(all_changes, daily_changes, min_supp, max_supp, min_conf, days, num_days):
    hists = defaultdict(lambda: defaultdict(Histogram))
    actives = dict()
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
            for _, hist in hists[change].items():
                hist.add_right_occurence()
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

                hist = hists[other_change][change]

                if not hist.is_active():
                    continue

                occurences_other = all_changes[other_change]
                occurences_change = all_changes[change]

                if not hist.is_setup():
                    maximal_confidence = len(occurences_change) / len(occurences_other)
                    if can_shortcut_support or maximal_confidence < min_conf:
                        hist.inactivate()
                        continue
                    else:
                        hist.setup(num_days, len(occurences_other))

                # skip if other change has appeared too often to reach min confidence
                # or too few occurrences are left for reach min support
                remaining_other_occurences = len(occurences_other) - hist.right_occurences() + 1
                remaining_change_occurences = len(occurences_change) - occurences_change.index(date)
                possible_occurences = min(remaining_change_occurences, remaining_other_occurences)
                can_reach_conf = (hist.abs_support() + possible_occurences) / len(occurences_other) >= min_conf
                can_reach_sup = hist.abs_support() + possible_occurences >= support_threshold

                if not (can_reach_conf and can_reach_sup):
                    hist.inactivate()
                    continue

                # actually add value to histogram
                hist.add_occurence(actives[other_change])

    print(f"saving: {datetime.now()}")
    del actives
    del all_changes
    del daily_changes

    useless_hists = defaultdict(set)
    useless_left_changes = set()
    for left_change, right_changes in hists.items():
        remaining = len(right_changes)
        for right_change, hist in right_changes.items():
            if not hist.is_active():
                useless_hists[left_change].add(right_change)
                remaining -= 1
        if remaining == 0:
            useless_left_changes.add(left_change)

    for change in useless_left_changes:
        del hists[left_change]
        del useless_hists[left_change]

    for left_change, right_changes in useless_hists.items():
        for right_change in right_changes:
            del hists[left_change][right_change]

    result = {
        left_change: {right_change: hist.bins() for right_change, hist in right_changes.items()}
        for left_change, right_changes in hists.items()
    }
    with open("histograms_tables-2.json", "w") as f:
        json.dump(result, f)

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
