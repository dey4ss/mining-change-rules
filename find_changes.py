import argparse
import json
import os
import pandas as pd
import sys
import queue
import multiprocessing as mp


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


def find_field_changes(table_id, new_dir, old_dir):
    with open(f"{new_dir}{os.sep}{table_id}{file_extension()}", encoding="utf-8") as f:
        n = json.loads(f.read())

    n_rows = n["rows"]
    n_attributes = n["attributes"]
    n_attr_map = {}
    n_attr_map_inv = {}
    for attr in n_attributes:
        n_attr_map[attr["position"]] = attr["name"]
        n_attr_map_inv[attr["name"]] = attr["position"]
    update_fields = list()
    delete_fields = list()
    insert_fields = list()

    if old_dir is None:
        for n_row in n_rows:
            for attr in n_attributes:
                insert_fields.append([table_id, attr["name"], str(n_row["id"])])
        return update_fields, delete_fields, insert_fields, table_id

    with open(f"{old_dir}{os.sep}{table_id}{file_extension()}", encoding="utf-8") as f:
        o = json.loads(f.read())

    o_rows = o["rows"]
    o_attributes = o["attributes"]
    o_attr_map = {}
    o_attr_map_inv = {}
    for attr in o_attributes:
        o_attr_map[attr["name"]] = attr["position"]
        o_attr_map_inv[attr["position"]] = attr["name"]

    for n_row in n_rows:
        o_row = find_row_match(o_rows, n_row["id"])
        n_fields = n_row["fields"]
        if not o_row is None:
            if not n_row == o_row:
                o_fields = o_row["fields"]
                for field_index in range(len(n_fields)):
                    attr = n_attr_map[field_index]
                    if not attr in o_attr_map:
                        insert_fields.append([table_id, attr, str(n_row["id"])])
                        continue
                    o_field = o_fields[o_attr_map[n_attr_map[field_index]]]
                    if not n_fields[field_index] == o_field:
                        update_fields.append([table_id, attr, str(n_row["id"])])
        else:
            for field_index in range(len(n_fields)):
                attr = n_attr_map[field_index]
                insert_fields.append([table_id, attr, str(n_row["id"])])
    for o_row in o_rows:
        n_row = find_row_match(n_rows, o_row["id"])
        o_fields = o_row["fields"]
        if not n_row is None:
            n_fields = n_row["fields"]
            for field_index in range(len(o_fields)):
                attr = o_attr_map_inv[field_index]
                if not attr in n_attr_map_inv:
                    delete_fields.append([table_id, attr, str(o_row["id"])])
        else:
            for field_index in range(len(o_fields)):
                attr = o_attr_map_inv[field_index]
                delete_fields.append([table_id, attr, str(o_row["id"])])
    return update_fields, delete_fields, insert_fields, None


def find_older_subdir(file_name, path, subdirs):
    subdirs_rev = subdirs[::-1]
    num_days = 0
    for subdir in subdirs_rev:
        num_days += 1
        if num_days > 7:
            return None
        subdir_path = os.path.join(path, subdir)
        subdir_files = os.listdir(subdir_path)
        if file_name in subdir_files:
            return subdir_path
    return None


def save_changes(changes, file_name):
    with open(file_name, "w", encoding="utf-8") as f:
        for change in changes:
            f.write(";".join(change))
            f.write("\n")


class DateJob:
    subdir: str
    subdir_index: int

class NewTables:
    date: str
    num: int


def find_changes(subdirs, path, output, num_tables, threads):
    job_queue = mp.Queue()
    new_table_queue = mp.Queue()
    workers = [
        mp.Process(target=find_daily_changes, args=(job_queue, path, num_tables, output, subdirs, new_table_queue))
        for _ in range(threads)
    ]

    for subdir_index in range(len(subdirs)):
        if subdir_index == 0:
            continue
        task = DateJob()
        task.path = path
        task.output = output
        task.subdir = subdirs[subdir_index]
        task.subdir_index = subdir_index
        job_queue.put(task)

    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join()

    with open(os.path.join(output, "new_tables.csv"), "w") as f:
        f.write("date;num_new_tables\n")
        while not new_table_queue.empty():
            new_tables = new_table_queue.get()
            f.write(f"{new_tables.date};{new_tables.num}\n")


def find_daily_changes(jobs, path, num_tables, output, subdirs, new_table_queue):
    while True:
        job = None
        try:
            job = jobs.get_nowait()
        except queue.Empty:
            return

        updates = list()
        deletes = list()
        inserts = list()
        new_tables = set()

        print(job.subdir)
        current_subdir_path = os.path.join(path, job.subdir)
        table_files = [f for f in os.listdir(current_subdir_path) if f.endswith(file_extension())]
        if not num_tables == -1:
            table_files = table_files[0:num_tables]

        for file_name in table_files:
            old_subdir_path = find_older_subdir(file_name, path, subdirs[: job.subdir_index])
            file_id = file_name[: -len(file_extension())]
            t_updates, t_deletes, t_inserts, t_table = find_field_changes(file_id, current_subdir_path, old_subdir_path)
            updates += t_updates
            deletes += t_deletes
            inserts += t_inserts
            if not t_table is None:
                new_tables.add(t_table)

        save_changes(updates, os.path.join(output, f"{job.subdir}_update.csv"))
        save_changes(inserts, os.path.join(output, f"{job.subdir}_add.csv"))
        save_changes(deletes, os.path.join(output, f"{job.subdir}_delete.csv"))
        new_tables_summary = NewTables()
        new_tables_summary.date = job.subdir
        new_tables_summary.num = len(new_tables)
        new_table_queue.put(new_tables_summary)


def parse_args():
    ap = argparse.ArgumentParser(description="Extracts change transactions")
    ap.add_argument("directory", type=str, help="Directory of the change files.")
    ap.add_argument("--start", type=str, help="Start date. Default 2019-11-01", default="2019-11-01")
    ap.add_argument("--end", type=str, help="End date. Default 2019-11-08", default="2019-11-08")
    ap.add_argument("--output", type=str, help="Output directory. Default ./changes", default="changes_3")
    ap.add_argument("--num_tables", type=int, help="Number of tables per date. -1 means all. Default -1", default=-1)
    ap.add_argument("--threads", type=int, help="Threads. Default 2", default=2)
    return vars(ap.parse_args())


def main():
    args = parse_args()
    if not os.path.isdir(args["output"]):
        os.makedirs(args["output"])
    subdirs = date_range(args["start"], args["end"])
    find_changes(subdirs, args["directory"], args["output"], args["num_tables"], args["threads"])


if __name__ == "__main__":
    mp.set_start_method("spawn")
    main()
