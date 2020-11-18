"""
A script to calculate the changes that happened on row-, column- and table level from the changes that
happened on the field level.
Input: directory with field change files
Output: for now: console output about number of changes and plots
"""

import argparse
import matplotlib.pyplot as plt
import os
import pickle


def plot_distribution(changes, granularity, change_type, dates, out_path):
    # TODO: combine figures nicely?
    plt.xlabel('Day')
    plt.ylabel('# Changes')
    plt.title(f"Distribution of {change_type}s in {granularity}s over time")
    plt.bar(dates, changes)
    plt.savefig(os.path.join(out_path, f"Distribution_{granularity}_{change_type}"))
    plt.close()
    pass


def get_changetype(file):
    return os.path.basename(file).split(".")[0]


def changes_per_file(file_name):
    # per file
    with open(file_name, "r", encoding="utf-8") as f:
        field_changes = f.readlines()
    # sets should take care of the duplicates
    table_changes = set()
    column_changes = set()
    row_changes = set()
    field_count = 0

    for field_change in field_changes:
        table_change, column_change, row_change = field_change.split(";")
        table_changes.add(table_change)
        column_changes.add(column_change)
        row_changes.add(row_change)
        field_count += 1

    return table_changes, column_changes, row_changes, field_count


def calculate_changes(in_path, out_path):
    change_dates = [changefile.split("_")[0] for changefile in os.listdir(os.path.join(in_path))]
    change_dates = list(set(change_dates))
    change_dates.sort()
    change_types = ["update", "add", "delete"]

    for change_type in change_types:
        table_changes_count = []
        col_changes_count = []
        row_changes_count = []
        field_changes_count = []

        for date in change_dates:
            print(date)
            change_file = os.path.join(in_path, f"{date}_{change_type}.csv")
            tc, cc, rc, fc = changes_per_file(change_file)
            table_changes_count.append(len(tc))
            col_changes_count.append(len(cc))
            row_changes_count.append(len(rc))
            field_changes_count.append(fc)
            print(f"{change_type} table:{len(tc)}, column:{len(cc)}, row:{len(rc)}, field: {fc}\n")

        # plot
        plot_distribution(table_changes_count, "table", change_type, change_dates, out_path)
        plot_distribution(col_changes_count, "column", change_type, change_dates, out_path)
        plot_distribution(row_changes_count, "row", change_type, change_dates, out_path)
        plot_distribution(field_changes_count, "field", change_type, change_dates, out_path)

        changes_to_save = {"table": table_changes_count,
                           "column": col_changes_count,
                           "row": row_changes_count,
                           "field": field_changes_count}
        with open(os.path.join(out_path, f"pickled_{change_type}"), "wb+") as save_file:
            pickle.dump(changes_to_save, save_file)


def parse_args():
    ap = argparse.ArgumentParser(description="Calculates changes for higher levels from the field level changes.")
    ap.add_argument("directory", type=str, help="Directory to the field change files.")
    ap.add_argument("--output", type=str, help="Output directory. Default ./calculated_changes", default="calculated_changes")
    return vars(ap.parse_args())


def main():
    args = parse_args()
    if not os.path.isdir(args["output"]):
        os.makedirs(args["output"])
    calculate_changes(args["directory"], args["output"])


if __name__ == "__main__":
    main()