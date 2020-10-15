import argparse
import json
import os
import pandas as pd
import sys


def date_range(start, end):
    timestamps = pd.date_range(start, end).tolist()
    return [timestamp.date().isoformat() for timestamp in timestamps]


def file_extension():
    return ".json_" if sys.platform.startswith("win") else ".json?"


def find_row_match(rows, row_id):
    for row in rows:
        if row_id == row["id"]:
            return row
    return None


def find_column_change(file_name, new_dir, old_dir):
    with open(f"{new_dir}{os.sep}{file_name}", encoding="utf-8") as f:
        n = json.loads(f.read())
    with open(f"{old_dir}{os.sep}{file_name}", encoding="utf-8") as f:
        o = json.loads(f.read())
    n_rows = n["rows"]
    o_rows = o["rows"]

    n_attributes = n["attributes"]
    o_attributes = o["attributes"]
    n_attr_map = {}
    for attr in n_attributes:
        n_attr_map[attr["position"]] = attr["name"]
    o_attr_map = {}
    for attr in o_attributes:
        o_attr_map[attr["name"]] = attr["position"]
    changed_cols = set()

    for n_row in n_rows:
        o_row = find_row_match(o_rows, n_row["id"])
        if not o_row is None and not n_row == o_row:
            n_fields = n_row["fields"]
            o_fields = o_row["fields"]
            for field_index in range(len(n_fields)):
                if not n_attr_map[field_index] in o_attr_map:
                    continue
                o_field = o_fields[o_attr_map[n_attr_map[field_index]]]
                if not n_fields[field_index] == o_field:
                    attr = n_attr_map[field_index]
                    changed_cols.add(attr)
    return changed_cols


def find_insert_delete(file_name, new_dir, old_dir):
    with open(f"{new_dir}{os.sep}{file_name}", encoding="utf-8") as f:
        n = json.loads(f.read())
    with open(f"{old_dir}{os.sep}{file_name}", encoding="utf-8") as f:
        o = json.loads(f.read())
    n_rows = n["rows"]
    o_rows = o["rows"]

    n_row_ids = [row["id"] for row in n_rows]
    o_row_ids = [row["id"] for row in o_rows]

    insert = False
    delete = False

    for n_id in n_row_ids:
        if n_id not in o_row_ids:
            insert = True
            break
    for o_id in o_row_ids:
        if o_id not in n_row_ids:
            delete = True
            break
    return insert, delete


def find_older_subdir(file_name, path, subdirs):
    subdirs_rev = subdirs[::-1]
    for subdir in subdirs_rev:
        subdir_path = os.path.join(path, subdir)
        subdir_files = os.listdir(subdir_path)
        if file_name in subdir_files:
            return subdir_path
    return None


def save_change_transactions(changes, file_name):
    with open(file_name, "w", encoding="utf-8") as f:
        for transaction in changes:
            f.write("\t".join(transaction))
            f.write("\n")


def load_change_transactions(file_name):
    with open(file_name, "r", encoding="utf-8") as f:
        plain_transactions = f.readlines()
    transactions = []
    for plain_transaction in plain_transactions:
        if plain_transaction.endswith("\n"):
            plain_transaction = plain_transaction[:-1]
        transaction = plain_transaction.split("\t")
        transactions.append(transaction)
    return transactions


def find_changes(subdirs, path, output, num_tables):
    table_changes = []
    column_changes = []
    insert_changes = []
    delete_changes = []

    for subdir_index in range(len(subdirs)):
        if subdir_index == 0:
            continue
        subdir = subdirs[subdir_index]
        print(subdir)
        current_subdir_path = os.path.join(path, subdir)
        files_current_subdir = [f for f in os.listdir(current_subdir_path) if f.endswith(file_extension())]
        if not num_tables == -1:
            files_current_subdir = files_current_subdir[0:num_tables]
        daily_column_changes = []
        daily_table_changes = set()
        daily_insert_changes = set()
        daily_delete_changes = set()
        for file_name in files_current_subdir:
            old_subdir_path = find_older_subdir(file_name, path, subdirs[:subdir_index])
            if old_subdir_path is None:
                continue
            file_id = file_name[:-6]
            changed_cols = find_column_change(file_name, current_subdir_path, old_subdir_path)
            if len(changed_cols) > 0:
                table_changed_cols = [f"{file_id}_{column}" for column in changed_cols]
                daily_column_changes += table_changed_cols
                daily_table_changes.add(file_id)
            insert, delete = find_insert_delete(file_name, current_subdir_path, old_subdir_path)
            if insert:
                daily_insert_changes.add(file_id)
            if delete:
                daily_delete_changes.add(file_id)

        column_changes.append(list(daily_column_changes))
        table_changes.append(list(daily_table_changes))
        insert_changes.append(list(daily_insert_changes))
        delete_changes.append(list(daily_delete_changes))

    save_change_transactions(column_changes, os.path.join(output, "column_changes"))
    save_change_transactions(table_changes, os.path.join(output, "table_changes"))
    save_change_transactions(insert_changes, os.path.join(output, "insert_changes"))
    save_change_transactions(delete_changes, os.path.join(output, "delete_changes"))


def parse_args():
    ap = argparse.ArgumentParser(description="Extracts change transactions")
    ap.add_argument("directory", type=str, help="Directory to the change files.")
    ap.add_argument("--start", type=str, help="Start date. Default 2019-11-01", default="2019-11-01")
    ap.add_argument("--end", type=str, help="Start date. Default 2019-11-08", default="2019-11-08")
    ap.add_argument("--output", type=str, help="Output directory. Default ./transactions", default="transactions")
    ap.add_argument("--num_tables", type=int, help="Number of tables per date. -1 means all. Default -1", default=-1)
    return vars(ap.parse_args())


def main():
    args = parse_args()
    if not os.path.isdir(args["output"]):
        os.makedirs(args["output"])
    subdirs = date_range(args["start"], args["end"])
    find_changes(subdirs, args["directory"], args["output"], args["num_tables"])


if __name__ == "__main__":
    main()
