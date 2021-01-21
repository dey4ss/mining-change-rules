#!/usr/local/bin/python3

import argparse
import json
import math
import multiprocessing as mp
import os
from collections import defaultdict

from util import Entity


def parse_args():
    min_supp_default = 0.1
    max_supp_default = 0.5
    thread_default = 10

    ap = argparse.ArgumentParser(
        description="Generates a dictionary of changes with their occurences, filtered by support."
    )
    ap.add_argument("change_dir", type=str, help="Directory of the change files.")
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
    ap.add_argument("--pp", action="store_true", help=f"Pretty print output JSON. Default false")

    return vars(ap.parse_args())


# equally assign a number of days to a number of threads
def distribute_days(overall_num_days, num_threads):
    base_num_days = overall_num_days // num_threads
    remaining_days = overall_num_days % num_threads
    return [base_num_days if thread >= remaining_days else base_num_days + 1 for thread in range(num_threads)]


# aggregate changes with their occurences
def aggregate_occurences(change_dir, days, my_id):
    my_dict = defaultdict(list)
    my_num_days = len(days)
    print(f"[Start Worker {my_id}]")

    entities = [e for e in Entity.string_representations() if e != Entity.Field.to_str()]
    entities = ["table"]
    change_files = [f"_{entity}_changes_aggregated.csv" for entity in entities]

    # for all changes of given dates:
    # simply append date to list of occurences of change
    for date, num_day in zip(days, range(1, my_num_days + 1)):
        for change_file in change_files:
            file_path = os.path.join(change_dir, f"{date}{change_file}")
            with open(file_path) as f:
                for line in f:
                    if line.startswith("change_type"):
                        continue
                    change = line.strip().split(";")
                    change_type = change[0][0]
                    change_id = "_".join(change[1:] + [change_type])
                    my_dict[change_id].append(date)
        if num_day == my_num_days:
            print(f"Worker {my_id}: done counting")
        # elif len(entities) > 0:
        #    print(f"Worker {my_id}: done {num_day}/{my_num_days} days")
    print(f"Worker {my_id}: {len(my_dict)}")

    print(f"[Exit Worker {my_id}]")
    return my_dict


def main(change_dir, threads, min_supp, max_supp, pretty_print):
    actual_days = {file_name[:10] for file_name in os.listdir(change_dir) if file_name.startswith("20")}
    num_days = len(actual_days)
    days_list = list(actual_days)
    days_list.sort()
    days_per_thread = distribute_days(num_days, threads)
    tasks = list()

    for thread in range(threads):
        thread_num_days = days_per_thread[thread]
        thread_days = days_list[:thread_num_days]
        days_list = days_list[thread_num_days:]
        thread_id = str(thread + 1).rjust(2)
        tasks.append((change_dir, thread_days, thread_id))

    results = None
    with mp.Pool(processes=threads) as pool:
        results = pool.starmap(aggregate_occurences, tasks)

    min_days = math.ceil(min_supp * num_days)
    max_days = math.floor(max_supp * num_days)

    result = defaultdict(list)
    for partial_result, t_id in zip(results, range(1, len(results) + 1)):
        print(f"Merging result {t_id}/{threads}")
        if partial_result is None:
            print(f"Warning: No results from worker {t_id}!")
            continue
        for change, occurences in partial_result.items():
            result[change].extend(occurences)

    skipped_changes = set()
    for change, occurences in result.items():
        occurence_count = len(occurences)
        if occurence_count > max_days or occurence_count < min_days:
            skipped_changes.add(change)

    for change in skipped_changes:
        del result[change]

    print(f"{len(result)} changes remaining with min supp {min_supp} and max supp {max_supp}")
    indent = 4 if pretty_print else None

    with open("all_changes_aggregated.json", "w") as f:
        json.dump(result, f, indent=indent)


if __name__ == "__main__":
    args = parse_args()
    main(
        args["change_dir"],
        args["threads"],
        args["min_supp"],
        args["max_supp"],
        args["pp"],
    )
