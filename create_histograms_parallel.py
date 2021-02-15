#!/usr/local/bin/python3

import argparse
import json
import math
import multiprocessing as mp
import os
import queue
from collections import defaultdict
from datetime import datetime
from itertools import product
from time import time


def parse_args():
    min_sup_default = 0.1
    max_sup_default = 0.5
    min_conf_default = 0.7
    thread_default = 10
    bin_default = 11
    partition_default = 1000

    ap = argparse.ArgumentParser(description="Generates a list of changes with their occurences, filtered by support.")
    ap.add_argument("change_dir", type=str, help="Directory of the change files")
    ap.add_argument(
        "change_file",
        type=str,
        help="File with occurences per change (expect Python dict as .json)",
    )
    ap.add_argument("output", type=str, help=f"Output file path")
    ap.add_argument(
        "--threads",
        type=int,
        help=f"Number of threads. Default {thread_default}",
        default=thread_default,
    )
    ap.add_argument(
        "--min_sup",
        type=float,
        help=f"Minimal support. Default {min_sup_default}",
        default=min_sup_default,
    )
    ap.add_argument(
        "--max_sup",
        type=float,
        help=f"Maximal support. Default {max_sup_default}",
        default=max_sup_default,
    )
    ap.add_argument(
        "--min_conf",
        type=float,
        help=f"Minimal Confidence. Default {min_conf_default}",
        default=min_conf_default,
    )
    ap.add_argument(
        "--num_bins",
        type=int,
        help=f"Bin count. Default {bin_default}",
        default=bin_default,
    )
    ap.add_argument(
        "--partition_size",
        type=int,
        help=f"Partition Size. Default {partition_default}",
        default=partition_default,
    )
    return vars(ap.parse_args())


class Histogram:
    def __init__(self):
        self._is_setup = False
        self._actual_antecedent = 0
        self._occurence_count = 0
        self.lift = 0

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


class Job:
    def __init__(self, antecedents, consequents):
        self.antecedents = antecedents
        self.consequents = consequents


def log(message):
    print(f"{datetime.now()} | {message}")


def get_histograms_of_partitions(
    antecedents, daily_antecedents, consequents, daily_consequents, min_sup_abs, max_sup_abs, min_conf, days, num_bins
):
    hists = defaultdict(lambda: defaultdict(Histogram))

    # index of changes within num days
    # change -> days since last occurence
    active_changes = dict()

    # index consequent -> antecedents
    pruned_combinations = defaultdict(set)

    # prohibit self-combinations
    for consequent in consequents:
        pruned_combinations[consequent].add(consequent)

    for date, day_index in zip(days, range(len(days))):
        active_today = dict()
        outdated = set()
        # gather changes of current day, update antecedent counts
        for change in daily_antecedents[date]:
            active_today[change] = 0
            for hist in hists[change].values():
                hist.add_antecedent_occurence()

        # update time since occurence for older antecedents
        for change in active_changes:
            active_changes[change] += 1
            if active_changes[change] >= num_bins:
                outdated.add(change)

        # merge, remove too old antecedents
        active_changes.update(active_today)
        for change in outdated:
            del active_changes[change]

        # skip if min support cannot be reached
        can_shortcut_support = len(days) - day_index < num_bins

        antecedent_candidates = set(active_changes.keys())

        # begin with real work:
        for consequent in daily_consequents[date]:
            my_antecedents = antecedent_candidates - pruned_combinations[consequent]
            occurences_consequent = consequents[consequent]
            ind_today = occurences_consequent.index(date)
            days_since_last_consequent_occurence = (
                num_bins if ind_today == 0 else days.index(date) - days.index(occurences_consequent[ind_today - 1])
            )

            for antecedent in my_antecedents:
                hist = hists[antecedent][consequent]
                occurences_antecedent = antecedents[antecedent]

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
                        hist.setup(num_bins, len(occurences_antecedent))

                # prune if antecedent has appeared too often to reach min confidence
                # or too few occurrences are left for reaching min support
                # or max sup is too high
                remaining_antecedent_occurences = len(occurences_antecedent) - hist.antecedent_occurences() + 1
                remaining_consequent_occurences = len(occurences_consequent) - occurences_consequent.index(date)
                possible_occurences = min(remaining_consequent_occurences, remaining_antecedent_occurences)
                can_reach_conf = (hist.abs_support() + possible_occurences) / len(occurences_antecedent) >= min_conf
                can_reach_sup = hist.abs_support() + possible_occurences >= min_sup_abs
                under_max_sup = hist.abs_support() < max_sup_abs

                if not (can_reach_conf and can_reach_sup and under_max_sup):
                    del hists[antecedent][consequent]
                    pruned_combinations[consequent].add(antecedent)
                    continue

                # actually add value to histogram
                hist.add_occurence(days_since_antecedent_occurence)

    del active_changes
    del pruned_combinations

    # remove antecedents without consequents
    # combinations with low min support / confidence may not have been removed previously
    useless_antecedents = set()
    useless_combinations = defaultdict(set)
    for antecedent, consequents in hists.items():
        num_useless_combinations = 0
        for consequent, hist in consequents.items():
            if hist.abs_support() < min_sup_abs or hist.confidence() < min_conf:
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

    for antecedent, consequents in hists.items():
        ant_support = len(antecedents[antecedent]) / len(days)
        for consequent, hist in consequents.items():
            hist.lift = hist.confidence() / ant_support

    return hists


def create_histograms(args):
    start = datetime.now()
    print("start program:", start)
    min_sup = args["min_sup"]
    max_sup = args["max_sup"]
    temp_dir = "change_partitions"
    partition_size = args["partition_size"]
    actual_days = sorted({file_name[:10] for file_name in os.listdir(args["change_dir"]) if file_name.startswith("20")})
    min_support_threshold = math.ceil(min_sup * len(actual_days))
    max_support_threshold = math.floor(max_sup * len(actual_days))
    # changes with num occurences > half of days are too frequent
    frequency_threshold = math.floor(0.5 * len(actual_days))

    # get index change -> dates
    with open(args["change_file"]) as f:
        all_changes = json.load(f)

    # build index date -> changes
    # remove change if min support to low
    daily_changes = defaultdict(set)
    too_infrequent_changes = set()
    for change, occurences in all_changes.items():
        if len(occurences) < min_support_threshold or len(occurences) > frequency_threshold:
            too_infrequent_changes.add(change)
            continue
        for date in occurences:
            daily_changes[date].add(change)
    for change in too_infrequent_changes:
        del all_changes[change]

    changes = list(all_changes.keys())
    print(f"input: {len(changes)} changes with {min_sup} <= sup(X) <= {max_sup}")
    partition_buckets = [
        min(partition_size * i, len(changes)) for i in range(math.ceil(len(changes) / partition_size) + 1)
    ]
    if not os.path.isdir(temp_dir):
        os.makedirs(temp_dir)
    time_stamp = time()
    partition_files = []
    for i in range(len(partition_buckets)):
        partition_start = 0 if i == 0 else partition_buckets[i - 1]
        partition_end = partition_buckets[i]
        partition_keys = set(changes[partition_start:partition_end])
        partition = {k: v for k, v in all_changes.items() if k in partition_keys}
        file_name = os.path.join(temp_dir, f"{time_stamp}_partition_{i}.json")
        partition_files.append(file_name)
        with open(file_name, "w") as f:
            json.dump(partition, f)

    with mp.Manager() as manager:
        job_queue = manager.Queue()
        mutex = mp.Lock()
        workers = [
            mp.Process(
                target=task_main,
                args=(
                    f"{n}".rjust(2),
                    job_queue,
                    min_support_threshold,
                    max_support_threshold,
                    args["min_conf"],
                    args["num_bins"],
                    args["output"],
                    actual_days,
                    mutex,
                ),
            )
            for n in range(args["threads"])
        ]

        for a, d in product(partition_files, repeat=2):
            job_queue.put(Job(a, d))

        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join()

    for file_name in partition_files:
        os.remove(file_name)
    end = datetime.now()
    print("end program:", end)
    print("duration:", end - start)


def task_main(
    my_id, jobs, min_support_threshold, max_support_threshold, min_conf, num_bins, result_file, all_days, mutex
):
    log(f"[Start Worker {my_id}]")
    while True:
        try:
            job = jobs.get_nowait()
        except queue.Empty:
            log(f"[Exit Worker {my_id}]")
            return

        antecedent_file = job.antecedents
        consequent_file = job.consequents

        log(f"Worker {my_id}: {antecedent_file} - {consequent_file}")

        # get indexes change -> dates
        with open(antecedent_file) as f:
            antecedents = json.load(f)
        with open(consequent_file) as f:
            consequents = json.load(f)

        # build indexes date -> changes
        daily_antecedents = defaultdict(set)
        for change, occurences in antecedents.items():
            for date in occurences:
                daily_antecedents[date].add(change)

        daily_consequents = defaultdict(set)
        for change, occurences in consequents.items():
            for date in occurences:
                daily_consequents[date].add(change)

        result = get_histograms_of_partitions(
            antecedents,
            daily_antecedents,
            consequents,
            daily_consequents,
            min_support_threshold,
            max_support_threshold,
            min_conf,
            all_days,
            num_bins,
        )

        del antecedents
        del consequents
        del daily_antecedents
        del daily_consequents

        with mutex:
            write_rules(result, result_file)
        del result


def write_rules(rules, result_file):
    with open(result_file, "a") as f:
        for antecedent, consequents in rules.items():
            for consequent, hist in consequents.items():
                hist_string = f"\"[{', '.join([str(x) for x in hist.bins()])}]\""
                f.write(
                    f"{antecedent};{consequent};{hist.abs_support()};{hist.confidence()};{hist.lift};{hist_string}\n"
                )


if __name__ == "__main__":
    create_histograms(parse_args())
