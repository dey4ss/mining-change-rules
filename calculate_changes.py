"""
A script to calculate the changes that happened on row-, column- and table level from the changes that
happened on the field level.
Input: directory with field change files
Output: for now: plots and pickled df
"""

import argparse
import matplotlib.pyplot as plt
import os
import seaborn as sns
import pandas as pd


def plot_distribution(changes, granularity, change_type, dates, out_path):
    # TODO: maybe move this to the other script
    sns.set()
    sns.set_theme(style="whitegrid")
    plt.xlabel("Day")
    plt.xticks(rotation=45)
    plt.ylabel("# Changes")
    plt.title(f"Distribution of {change_type}s in {granularity}s over time")
    plt.tight_layout()
    plt.bar(dates, changes, color="orange")
    plt.savefig(os.path.join(out_path, f"Distribution_{granularity}_{change_type}"))
    plt.close()


def plot_distribution_sb(changes, granularity, change_type, dates, out_path):
    sns.set()
    dict = {"Days": dates, "Change Counts": changes}
    df = pd.DataFrame(dict)
    sns.set_theme(style="whitegrid")
    sns.barplot(x="Days", y="Change Counts", data=df, color="orange")
    plt.savefig(os.path.join(out_path, f"Distribution_{granularity}_{change_type}"))


def get_changetype(file):
    return os.path.basename(file).split(".")[0]


def changes_per_file(file_name):
    # per file
    with open(file_name, "r", encoding="utf-8") as f:
        field_changes = f.readlines()
    # sets should take care of the duplicates
    changes = set()
    table_changes = set()
    column_changes = set()
    row_changes = set()
    field_count = 0

    for field_change in field_changes:
        changes.add(field_change)

    changes = list(changes)
    for i in range(len(changes)):
        table_change, column_change, row_change = changes[i].split(";")
        table_changes.add(table_change)
        column_changes.add(column_change)
        row_changes.add(row_change)
        field_count += 1

    return table_changes, column_changes, row_changes, field_count


def count_whole_changes(path, date, entity):
    # count the changes from the files with whole rows/columns
    counted_additions = set()
    counted_deletions = set()

    change_file = os.path.join(path, f"{date}_{entity}_add_delete.csv")
    with open(change_file, "r", encoding="utf-8") as f:
        changes = f.readlines()

    for change in changes:
        type, table, ent = change.split(";")
        if type == "add":
            counted_additions.add(f"{table};{ent}")
        else:
            counted_deletions.add(f"{table};{ent}")

    return len(counted_additions), len(counted_deletions)


def count_whole_tables(path, date):
    # count the changes from the files with whole tables
    counted_additions = set()

    change_file = os.path.join(path, f"{date}_table_add.csv")
    with open(change_file, "r", encoding="utf-8") as f:
        changes = f.readlines()

    for change in changes:
        counted_additions.add(change)

    return len(counted_additions)


def whole_changes(path, date):
    # TODO table deletions

    entities = ["table", "column", "row"]
    for entity in entities:
        if entity == "table":
            table_additions = count_whole_tables(path, date)
        elif entity == "column":
            column_additions, column_deletions = count_whole_changes(
                path, date, "column"
            )
        else:
            row_additions, row_deletions = count_whole_changes(path, date, "row")

    df_whole_changes = pd.DataFrame(
        [
            [date, "add", table_additions, column_additions, row_additions],
            [date, "delete", None, column_deletions, row_deletions],
        ],
        columns=["dates", "change_type", "whole_table", "whole_column", "whole_row"],
    )
    return df_whole_changes


def calculate_changes(in_path, out_path, plots):
    change_dates = [
        changefile.split("_")[0] for changefile in os.listdir(os.path.join(in_path))
    ]
    change_dates = list(set(change_dates))
    change_dates.sort()
    change_types = ["update", "add", "delete"]

    df_column_names = ["dates", "change_type", "table", "column", "row", "field"]
    df_all = pd.DataFrame(columns=df_column_names)

    for change_type in change_types:
        df_round = pd.DataFrame(columns=df_column_names)
        df_round["dates"] = change_dates
        df_round["change_type"] = [change_type for i in range(len(change_dates))]
        df_whole_changes = pd.DataFrame(
            columns=["dates", "change_type", "whole_table", "whole_column", "whole_row"]
        )
        table_changes_count = []
        col_changes_count = []
        row_changes_count = []
        field_changes_count = []

        for date in change_dates:
            change_file = os.path.join(in_path, f"{date}_{change_type}.csv")
            tc, cc, rc, fc = changes_per_file(change_file)
            table_changes_count.append(len(tc))
            col_changes_count.append(len(cc))
            row_changes_count.append(len(rc))
            field_changes_count.append(fc)

            # whole changes
            w_changes = whole_changes(in_path, date)
            if df_whole_changes.empty:
                df_whole_changes = w_changes.copy(deep=False)
            else:
                df_whole_changes = df_whole_changes.append(w_changes, ignore_index=True)

            # print(f"{change_type} table:{len(tc)}, column:{len(cc)}, row:{len(rc)}, field: {fc}\n")

        if plots:
            plot_distribution(
                table_changes_count, "table", change_type, change_dates, out_path
            )
            plot_distribution(
                col_changes_count, "column", change_type, change_dates, out_path
            )
            plot_distribution(
                row_changes_count, "row", change_type, change_dates, out_path
            )
            plot_distribution(
                field_changes_count, "field", change_type, change_dates, out_path
            )

        df_round["table"] = table_changes_count
        df_round["column"] = col_changes_count
        df_round["row"] = row_changes_count
        df_round["field"] = field_changes_count

        df_round = df_round.merge(df_whole_changes, on=["dates", "change_type"])

        if df_all.empty:
            df_all = df_round.copy(deep=False)
        else:
            df_all = df_all.append(df_round, ignore_index=True)

        print(df_all)

    pd.to_pickle(df_all, os.path.join(out_path, f"pickled_df"))


def parse_args():
    ap = argparse.ArgumentParser(
        description="Calculates changes for higher levels from the field level changes."
    )
    ap.add_argument("directory", type=str, help="Directory to the field change files.")
    ap.add_argument(
        "--output",
        type=str,
        help="Output directory. Default ./calculated_changes",
        default="calculated_changes",
    )
    ap.add_argument("--plots", help="Whether bar plots should be created")
    return vars(ap.parse_args())


def main():
    args = parse_args()
    if not os.path.isdir(args["output"]):
        os.makedirs(args["output"])
    calculate_changes(args["directory"], args["output"], args["plots"])


if __name__ == "__main__":
    main()
