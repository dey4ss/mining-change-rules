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
        self._actual_antecedent = 0
        self._occurence_count = 0

    def is_setup(self):
        return self._is_setup

    def setup(self, bin_count, occurences_right):
        if self._is_setup:
            raise RuntimeError(f"{self.__class__.__name__} has already been set up.")
        self._bins = [0 for _ in range(bin_count)]
        self._count_antecedent = occurences_right
        self._is_setup = True

    def add_occurence(self, bin):
        if not self._is_setup:
            raise RuntimeError(f"{self.__class__.__name__} needs to be set up first.")
        self._bins[bin] += 1
        self._occurence_count += 1

    def add_antecedent_occurence(self):
        self._actual_antecedent += 1

    def antecedent_occurences(self):
        return self._actual_antecedent

    def bins(self):
        return self._bins

    def confidence(self):
        return self._occurence_count / self._count_antecedent

    def abs_support(self):
        return self._occurence_count


def get_hist(all_changes, daily_changes, min_supp_abs, max_supp_abs, min_conf, days, num_days, candidates):
    hists = defaultdict(lambda: defaultdict(Histogram))
    start = datetime.now()
    print(f"start : {start}")
    sum_days = len(days)

    # index of changes within num days
    # change -> days since last occurence
    active_changes = dict()

    # index consequent -> antecedents
    pruned_combinations = defaultdict(set)

    for date, day_index in zip(days, range(sum_days)):
        print(date)
        active_today = dict()
        outdated = set()
        # gather changes of current day, update antecedent counts
        for change in daily_changes[date]:
            active_today[change] = 0
            for hist in hists[change].values():
                hist.add_antecedent_occurence()

        # update time since occurence for older antecedents
        for change in active_changes:
            active_changes[change] += 1
            if active_changes[change] >= num_days:
                outdated.add(change)

        # merge, remove too old antecedents
        active_changes.update(active_today)
        for change in outdated:
            del active_changes[change]

        # skip if min support cannot be reached
        can_shortcut_support = sum_days - day_index < num_days

        antecedent_candidates = set(active_changes.keys())
        consequents = candidates & daily_changes[date]

        # begin with real work:
        for consequent in consequents:
            antecedents = antecedent_candidates - pruned_combinations[consequent]
            occurences_consequent = all_changes[consequent]
            ind_today = occurences_consequent.index(date)
            days_since_last_consequent_occurence = (
                num_days if ind_today == 0 else days.index(date) - days.index(occurences_consequent[ind_today - 1])
            )

            for antecedent in antecedents:
                if antecedent == consequent:
                    continue

                hist = hists[antecedent][consequent]
                occurences_antecedent = all_changes[antecedent]
                occurences_consequent = all_changes[consequent]

                # make sure that consequent has not occured in between
                days_since_antecedent_occurence = active_changes[antecedent]
                difference = days_since_antecedent_occurence - days_since_last_consequent_occurence
                if difference >= 0:
                    continue

                # check if histogram is already created
                # prune if min confidence or min support cannot be reached
                if not hist.is_setup():
                    maximal_confidence = len(occurences_consequent) / len(occurences_antecedent)
                    if can_shortcut_support or maximal_confidence < min_conf:
                        del hists[antecedent][consequent]
                        pruned_combinations[consequent].add(antecedent)
                        continue
                    else:
                        hist.setup(num_days, len(occurences_antecedent))

                # prune if antecedent has appeared too often to reach min confidence
                # or too few occurrences are left for reaching min support
                # or max supp is too high
                remaining_antecedent_occurences = len(occurences_antecedent) - hist.antecedent_occurences() + 1
                remaining_consequent_occurences = len(occurences_consequent) - occurences_consequent.index(date)
                possible_occurences = min(remaining_consequent_occurences, remaining_antecedent_occurences)
                can_reach_conf = (hist.abs_support() + possible_occurences) / len(occurences_antecedent) >= min_conf
                can_reach_sup = hist.abs_support() + possible_occurences >= min_supp_abs
                under_max_sup = hist.abs_support() < max_supp_abs

                if not (can_reach_conf and can_reach_sup and under_max_sup):
                    del hists[antecedent][consequent]
                    pruned_combinations[consequent].add(antecedent)
                    continue

                # actually add value to histogram
                hist.add_occurence(days_since_antecedent_occurence)

    end = datetime.now()
    print(f"saving: {end}")
    print("duration", end - start)
    del active_changes
    # del all_changes
    del daily_changes
    del pruned_combinations

    # remove antecedents without consequents
    # combinations with low min support / confidence may not have been removed previously
    useless_antecedents = set()
    useless_combinations = defaultdict(set)
    for antecedent, consequents in hists.items():
        num_useless_combinations = 0
        for consequent, hist in consequents.items():
            if hist.abs_support() < min_supp_abs or hist.confidence() < min_conf:
                useless_combinations[antecedent].add(consequent)
                num_useless_combinations += 1
        if len(consequents) == num_useless_combinations:
            useless_antecedents.add(antecedent)

    for antecedent in useless_antecedents:
        del hists[antecedent]
        try:
            del useless_combinations[antecedent]
        except KeyError:
            pass

    for antecedent, consequents in useless_combinations.items():
        for consequent in consequents:
            del hists[antecedent][consequent]

    lift = lambda ant, con, sup: sup / (len(all_changes[ant]) * len(all_changes[con]))
    result_entry = lambda ant, con, histo: [
        histo.abs_support(),
        histo.confidence(),
        lift(ant, con, histo.abs_support()),
        histo.bins(),
    ]

    result = {
        antecedent: {consequent: result_entry(antecedent, consequent, hist) for consequent, hist in consequents.items()}
        for antecedent, consequents in hists.items()
    }

    num_rules = sum([len(consequents) for antecedent, consequents in hists.items()])
    print(num_rules, "rules generated")

    with open("histograms_columns.json", "w") as f:
        json.dump(result, f)

    print(f"end: {datetime.now()}")


def main():
    start = datetime.now()
    print("start program:", start)
    args = parse_args()
    min_supp = args["min_supp"]
    actual_days = list(
        sorted({file_name[:10] for file_name in os.listdir(args["change_dir"]) if file_name.startswith("20")})
    )
    support_threshold = math.ceil(min_supp * len(actual_days))

    # get index change -> dates
    with open(args["change_file"]) as f:
        all_changes = json.load(f)

    # build index date -> changes
    # remove change if min support to low
    daily_changes = defaultdict(set)
    too_infrequent_changes = set()
    for change, occurences in all_changes.items():
        if len(occurences) < support_threshold:
            too_infrequent_changes.add(change)
            continue
        for date in occurences:
            daily_changes[date].add(change)
    for change in too_infrequent_changes:
        del all_changes[change]

    candidates = set(all_changes.keys())
    # candidates = set(list(all_changes.keys())[:10])

    get_hist(
        all_changes,
        daily_changes,
        support_threshold,
        math.floor(args["max_supp"] * len(actual_days)),
        args["min_conf"],
        actual_days,
        args["num_bins"],
        candidates,
    )


if __name__ == "__main__":
    main()
