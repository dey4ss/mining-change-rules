#!/usr/bin/python3

import argparse
import json
import multiprocessing as mp
import os
import queue
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + f"{os.sep}..")
from util.util import date_range, file_extension


def parse_args():
    ap = argparse.ArgumentParser(description="Extracts change transactions")
    ap.add_argument("directory", type=str, help="Directory of the change files.")
    ap.add_argument("--start", type=str, help="Start date. Default 2020-05-01", default="2020-05-01")
    ap.add_argument("--end", type=str, help="End date. Default 2020-11-01", default="2020-11-01")
    ap.add_argument("--base_date", type=str, help="Date of whole data start. Default 2019-11-01", default="2019-11-01")
    ap.add_argument("--output", type=str, help="Output directory. Default ./changes", default="changes")
    ap.add_argument("--num_tables", type=int, help="Number of tables per date. -1 means all. Default -1", default=-1)
    ap.add_argument("--threads", type=int, help="Number of threads. Default 2", default=2)
    ap.add_argument("--distinguish_null", action="store_true", help="Count null value changes as insert/delete.")
    ap.add_argument(
        "--included_tables", type=str, default="", help="File with tables to include. If not specified, use all."
    )
    ap.add_argument("--excluded_tables", type=str, default="", help="File with tables to exclude.")
    return vars(ap.parse_args())


def find_row_match(rows, row_id):
    for row in rows:
        if row_id == row["id"]:
            return row
    return None


def is_null(value):
    if value is None:
        return True
    candidates = ["-", "/", "", "–", "—", "%"]
    if type(value) == str:
        val_strip = value.strip()
        if val_strip in candidates or val_strip.lower() == "null":
            return True
    return False


def find_field_changes(table_id, new_dir, old_dir, distinguish_null):
    with open(f"{new_dir}{os.sep}{table_id}{file_extension()}", encoding="utf-8") as f:
        n = json.loads(f.read())

    n_rows = n["rows"]
    n_attributes = n["attributes"]
    n_attr_map = {}
    n_attr_map_inv = {}
    for attr_index in range(len(n_attributes)):
        attr = n_attributes[attr_index]
        n_attr_map[attr_index] = attr["id"]
        n_attr_map_inv[attr["id"]] = attr_index
    update_fields = list()
    delete_fields = list()
    insert_fields = list()
    column_add_delete = list()
    row_add_delete = list()

    if old_dir is None:
        for n_row in n_rows:
            for attr in n_attr_map_inv:
                insert_fields.append([table_id, str(attr), str(n_row["id"])])
            row_add_delete.append(["insert", table_id, str(n_row["id"])])
        for attr in n_attr_map_inv:
            column_add_delete.append(["insert", table_id, str(attr)])
        return update_fields, delete_fields, insert_fields, table_id, column_add_delete, row_add_delete

    with open(f"{old_dir}{os.sep}{table_id}{file_extension()}", encoding="utf-8") as f:
        o = json.loads(f.read())

    o_rows = o["rows"]
    o_attributes = o["attributes"]
    o_attr_map = {}
    o_attr_map_inv = {}
    for attr_index in range(len(o_attributes)):
        attr = o_attributes[attr_index]
        o_attr_map[attr["id"]] = attr_index
        o_attr_map_inv[attr_index] = attr["id"]

    for attr in n_attr_map_inv:
        if not attr in o_attr_map:
            column_add_delete.append(["insert", table_id, str(attr)])
    for attr in o_attr_map:
        if not attr in n_attr_map_inv:
            column_add_delete.append(["delete", table_id, str(attr)])

    for n_row in n_rows:
        o_row = find_row_match(o_rows, n_row["id"])
        n_fields = n_row["fields"]
        if not o_row is None:
            if not n_row == o_row:
                o_fields = o_row["fields"]
                for field_index in range(len(n_fields)):
                    attr = n_attr_map[field_index]
                    if not attr in o_attr_map:
                        insert_fields.append([table_id, str(attr), str(n_row["id"])])
                        continue
                    o_field = o_fields[o_attr_map[n_attr_map[field_index]]]
                    n_field = n_fields[field_index]
                    if not n_field == o_field:
                        if distinguish_null:
                            n_is_null = is_null(n_field)
                            o_is_null = is_null(o_field)
                            if n_is_null and o_is_null:
                                continue
                            if n_is_null:
                                delete_fields.append([table_id, str(attr), str(n_row["id"])])
                                continue
                            if o_is_null:
                                insert_fields.append([table_id, str(attr), str(n_row["id"])])
                                continue
                        update_fields.append([table_id, str(attr), str(n_row["id"])])
        else:
            for field_index in range(len(n_fields)):
                attr = n_attr_map[field_index]
                insert_fields.append([table_id, str(attr), str(n_row["id"])])
            row_add_delete.append(["insert", table_id, str(n_row["id"])])
    for o_row in o_rows:
        n_row = find_row_match(n_rows, o_row["id"])
        o_fields = o_row["fields"]
        if not n_row is None:
            n_fields = n_row["fields"]
            for field_index in range(len(o_fields)):
                attr = o_attr_map_inv[field_index]
                if not attr in n_attr_map_inv:
                    delete_fields.append([table_id, str(attr), str(o_row["id"])])
        else:
            for field_index in range(len(o_fields)):
                attr = o_attr_map_inv[field_index]
                delete_fields.append([table_id, str(attr), str(o_row["id"])])
            row_add_delete.append(["delete", table_id, str(o_row["id"])])
    return update_fields, delete_fields, insert_fields, None, column_add_delete, row_add_delete


def find_older_subdir(file_name, path, subdirs):
    subdirs_rev = subdirs[::-1]
    for subdir in subdirs_rev:
        subdir_path = os.path.join(path, subdir)
        if not os.path.isdir(subdir_path):
            continue
        subdir_files = os.listdir(subdir_path)
        if file_name in subdir_files:
            return subdir_path
    return None


def save_changes(changes, file_name):
    with open(file_name, "w", encoding="utf-8") as f:
        for change in changes:
            if not type(change) == list:
                f.write(change)
            else:
                f.write(";".join(change))
            f.write("\n")


class DateJob:
    subdir: str
    subdir_index: int

    def __init__(self, subdir, subdir_index):
        self.subdir = subdir
        self.subdir_index = subdir_index


class NewTables:
    date: str
    num: int

    def __init__(self, date, num):
        self.date = date
        self.num = num


def find_changes(
    subdirs, path, output, num_tables, threads, distinguish_null, offset, included_tables, excluded_tables
):
    with mp.Manager() as manager:
        job_queue = manager.Queue()
        new_table_queue = manager.Queue()
        workers = [
            mp.Process(
                target=find_daily_changes,
                args=(
                    job_queue,
                    path,
                    num_tables,
                    output,
                    subdirs,
                    new_table_queue,
                    distinguish_null,
                    included_tables,
                    excluded_tables,
                    f"{n}".rjust(2),
                ),
            )
            for n in range(threads)
        ]

        for subdir_index in range(offset, len(subdirs)):
            if subdir_index == 0:
                continue

            job_queue.put(DateJob(subdirs[subdir_index], subdir_index))

        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join()

        with open(os.path.join(output, "new_tables.csv"), "w") as f:
            f.write("date;num_new_tables\n")
            while True:
                try:
                    new_tables = new_table_queue.get_nowait()
                    f.write(f"{new_tables.date};{new_tables.num}\n")
                except queue.Empty:
                    return


def get_tables_from_file(file_name):
    if not file_name:
        return set()
    tables = set()
    with open(file_name, "r") as f:
        for line in f:
            line_cleaned = line.strip()
            if line_cleaned in ["table_id", ""]:
                continue
            tables.add(line_cleaned)
    return tables


def find_daily_changes(
    jobs, path, num_tables, output, subdirs, new_table_queue, distinguish_null, included_tables, excluded_tables, n
):
    print(f"[Start Worker {n}]")
    while True:
        job = None
        try:
            job = jobs.get_nowait()
        except queue.Empty:
            print(f"[Exit Worker {n}]")
            return
        if job is None:
            print(f"[Exit Worker {n}]")
            return

        updates = list()
        deletes = list()
        inserts = list()
        new_tables = set()
        column_add_delete = list()
        row_add_delete = list()

        current_subdir_path = os.path.join(path, job.subdir)
        if not os.path.isdir(current_subdir_path):
            continue

        table_files = [f for f in os.listdir(current_subdir_path) if f.endswith(file_extension())]
        print(f"{job.subdir} [Worker {n}, {len(table_files)} tables]")
        if not num_tables == -1:
            table_files = table_files[0:num_tables]

        for file_name in table_files:
            old_subdir_path = find_older_subdir(file_name, path, subdirs[: job.subdir_index])
            file_id = file_name[: -len(file_extension())]
            if (len(included_tables) > 0 and not file_id in included_tables) or file_id in excluded_tables:
                continue
            t_updates, t_deletes, t_inserts, t_table, t_columns, t_rows = find_field_changes(
                file_id, current_subdir_path, old_subdir_path, distinguish_null
            )
            updates += t_updates
            deletes += t_deletes
            inserts += t_inserts
            column_add_delete += t_columns
            row_add_delete += t_rows
            if not t_table is None:
                new_tables.add(t_table)

        save_changes(updates, os.path.join(output, f"{job.subdir}_update.csv"))
        save_changes(inserts, os.path.join(output, f"{job.subdir}_insert.csv"))
        save_changes(deletes, os.path.join(output, f"{job.subdir}_delete.csv"))
        save_changes(column_add_delete, os.path.join(output, f"{job.subdir}_column_insert_delete.csv"))
        save_changes(row_add_delete, os.path.join(output, f"{job.subdir}_row_insert_delete.csv"))
        save_changes(new_tables, os.path.join(output, f"{job.subdir}_table_insert.csv"))
        new_table_queue.put(NewTables(job.subdir, len(new_tables)))


def main():
    args = parse_args()
    not_considered_subdirs = date_range(args["base_date"], args["start"])
    if not os.path.isdir(args["output"]):
        os.makedirs(args["output"])
    subdirs = date_range(args["base_date"], args["end"])
    included_tables = get_tables_from_file(args["included_tables"])
    excluded_tables = get_tables_from_file(args["excluded_tables"])
    find_changes(
        subdirs,
        args["directory"],
        args["output"],
        args["num_tables"],
        args["threads"],
        args["distinguish_null"],
        len(not_considered_subdirs) - 1,
        included_tables,
        excluded_tables,
    )


if __name__ == "__main__":
    mp.set_start_method("spawn")
    main()
