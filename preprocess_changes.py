import argparse
import json
import math
import pandas as pd
from collections import defaultdict
from datetime import datetime


def parse_args():
    ap = argparse.ArgumentParser(description="Preprocesses aggregated changes.")
    ap.add_argument(
        "change_file",
        type=str,
        help="File with occurences per change (expect Python dict as .json)",
    )
    ap.add_argument(
        "--periodic_threshold",
        type=float,
        help="Threshold for filtering periodic changes, i.e. share of changes that happen on same day in month or week or within same interval",
        default=0.9,
    )
    return vars(ap.parse_args())


def start_day_representation(num_changes, occurences):
    if num_changes < 200000:
        return occurences[0]
    # if num_changes < 1000000:
    return f"{occurences[0]}-{occurences[1]}"
    # return f"{occurences[0]}-{occurences[1]}-{occurences[2]}"


def group_simultaneous_changes(all_changes):
    same_changes = dict()

    checks = {change: [False, occurences] for change, occurences in all_changes.items()}
    num_groups = 0

    # try grouping only if start date(s) is/are same
    start_days = defaultdict(list)
    for change, occurences in all_changes.items():
        start_days[start_day_representation(len(all_changes), occurences)].append(change)

    start_day_counts = defaultdict(int)
    for changes in start_days.values():
        start_day_counts[len(changes)] += 1

    for same_start_group in start_days.values():
        for i in range(len(same_start_group)):
            current_change = same_start_group[i]
            was_done, occurences = checks[current_change]
            if i == len(same_start_group) - 1 or was_done:
                continue

            group = list()
            for j in range(i + 1, len(same_start_group)):
                other_change = same_start_group[j]
                other_was_done, other_occurences = checks[other_change]
                if other_was_done:
                    continue
                if other_occurences == occurences:
                    checks[other_change][0] = True
                    group.append(other_change)
            if len(group) > 0:
                group.append(current_change)
                group.sort()
                same_changes[f"group{num_groups}"] = group
                num_groups += 1

    print(f"{num_groups} groups")
    return same_changes


def find_periodic_changes(all_changes, threshold):
    all_days = list()
    date_weekdays = dict()
    dates = pd.date_range("2019-11-02", "2020-11-01").to_series()
    weekdays = dates.dt.weekday
    for date, weekday in zip(dates, weekdays):
        iso_date = date.date().isoformat()
        all_days.append(iso_date)
        date_weekdays[iso_date] = weekday

    periodic_changes = set()

    for change, occurences in all_changes.items():
        if (
            same_weekday(occurences, date_weekdays, threshold)
            or same_difference(occurences, all_days, threshold)
            or same_date(occurences, all_days, threshold)
        ):
            periodic_changes.add(change)

    print(f"{len(periodic_changes)} periodic changes")
    return periodic_changes


def same_weekday(occurences, date_weekdays, threshold):
    sup = len(occurences) / len(date_weekdays)
    occurence_weekdays = [date_weekdays[occurence] for occurence in occurences]
    weekday_counts = defaultdict(int)
    for occurence_weekday in occurence_weekdays:
        weekday_counts[occurence_weekday] += 1
    sorted_weekday_counts = sorted(weekday_counts.items(), key=lambda x: x[1], reverse=True)
    minimum_days = math.ceil(sup * 7)
    amd_sum = sum([x[1] for x in sorted_weekday_counts[:minimum_days]])
    min_day_ratio = amd_sum / len(occurences)
    return min_day_ratio >= threshold


def same_difference(occurences, all_days, threshold):
    inds = [all_days.index(occurence) for occurence in occurences]
    diffs = [inds[i] - inds[i - 1] for i in range(1, len(inds))]
    counts = defaultdict(int)
    for diff in diffs:
        counts[diff] += 1
    sorted_counts = sorted(counts.items(), key=lambda x: x[1], reverse=True)
    return sorted_counts[0][1] / len(occurences) >= threshold


def same_date(occurences, all_days, threshold):
    sup = len(occurences) / len(all_days)
    occurence_dates = [occurence[-2:] for occurence in occurences]
    date_counts = defaultdict(int)
    for occurence_date in occurence_dates:
        date_counts[occurence_date] += 1
    sorted_date_counts = sorted(date_counts.items(), key=lambda x: x[1], reverse=True)
    minimum_days = math.ceil(sup * (len(all_days) / 12))
    amd_sum = sum([x[1] for x in sorted_date_counts[:minimum_days]])
    min_day_ratio = amd_sum / len(occurences)
    return min_day_ratio >= threshold


def main():
    start = datetime.now()
    print("start:", start)
    args = parse_args()

    # get index change -> dates
    with open(args["change_file"]) as f:
        all_changes = json.load(f)
    print(f"input: {len(all_changes)} changes")

    # filter changes that always happen together
    simultaneous_changes = group_simultaneous_changes(all_changes)
    for group, group_items in simultaneous_changes.items():
        all_changes[group] = all_changes[group_items[0]]
        for item in group_items:
            del all_changes[item]

    # filter periodic_changes
    periodic_changes = find_periodic_changes(all_changes, args["periodic_threshold"])
    for change in periodic_changes:
        del all_changes[change]

    print(f"{len(all_changes)} changes remaining")

    prefix = ".".join(args["change_file"].split(".")[:-1])
    with open(f"{prefix}_change_groups.json", "w") as f:
        json.dump(simultaneous_changes, f)

    with open(f"{prefix}_grouped.json", "w") as f:
        json.dump(all_changes, f)

    end = datetime.now()
    print("end:", end)
    print("duration:", end - start)


if __name__ == "__main__":
    main()
