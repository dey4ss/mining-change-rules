#!/usr/bin/python3

import argparse
import json
import multiprocessing as mp
import os
import queue
import sys
from time import time
from collections import defaultdict

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + f"{os.sep}..")
from util.util import file_extension


def parse_args():
    ap = argparse.ArgumentParser(description="Gets table information")
    ap.add_argument("directory", type=str, help="Directory of the change files.")
    ap.add_argument("--threads", "-t", type=int, help="Number of threads. Default 2", default=2)
    return vars(ap.parse_args())


class TableInfo:
    def __init__(self):
        self.columns = 0
        self.rows = 0


def main(change_dir, threads):
    subdirs = sorted(os.listdir(change_dir))  # [1:11]

    with mp.Manager() as manager:
        job_queue = manager.Queue()
        result_lock = manager.RLock()
        tables = manager.dict()
        workers = [
            mp.Process(
                target=daily_statistics,
                args=(
                    job_queue,
                    change_dir,
                    tables,
                    result_lock,
                    f"{n}".rjust(2),
                ),
            )
            for n in range(threads)
        ]

        for subdir in subdirs:
            job_queue.put(subdir)

        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join()

        column_sum = 0
        row_sum = 0

        print(len(tables))

        for _, table_info in tables.items():
            column_sum += table_info.columns
            row_sum += table_info.rows

        avg_columns = column_sum / len(tables)
        avg_rows = row_sum / len(tables)

        print(f"{len(tables)} tables")
        print(f"{avg_columns} avg columns")
        print(f"{avg_rows} avg rows")


def daily_statistics(jobs, path, tables, table_lock, my_id):
    shares = {"get": 0, "list": 0, "work": 0, "merge": 0}
    last_t = 0
    last_c = 0
    last_r = 0
    last_p = 0
    while True:
        start = time()
        date = None
        try:
            date = jobs.get_nowait()
        except queue.Empty:
            print(f"[Exit Worker {my_id}]")
            return

        current_dir = os.path.join(path, date)
        if not os.path.isdir(current_dir):
            continue
        l = time()
        files = [f for f in os.listdir(current_dir) if f.endswith(file_extension())]
        print(
            f"{date} [Worker {my_id}] ({len(files)}) {last_t}, {last_c}, {last_r} w:{shares['work']}, p:{last_p}, m:{shares['merge']}"
        )
        last_t = len(files)

        my_tables = defaultdict(TableInfo)
        w = time()
        last_c = 0
        last_r = 0
        last_p = 0
        for file_name in files:
            p_s = time()
            with open(os.path.join(current_dir, file_name), "r", encoding="utf-8") as f:
                table = json.load(f)
                p_e = time()
                last_p += p_e - p_s
                table_info = my_tables[table["id"]]
                columns = len(table["attributes"])
                rows = len(table["rows"])
                last_c += columns
                last_r += rows
                table_info.columns = max(table_info.columns, columns)
                table_info.rows = max(table_info.rows, rows)
        m = time()
        combine_results(tables, my_tables, table_lock)
        e = time()
        shares["get"] = l - start
        shares["list"] = w - l
        shares["work"] = m - w
        shares["merge"] = e - m


def combine_results(all_tables, my_tables, table_lock):
    with table_lock:
        for table, my_table_info in my_tables.items():
            table_info = all_tables.setdefault(table, TableInfo())
            table_info.columns = max(table_info.columns, my_table_info.columns)
            table_info.rows = max(table_info.rows, my_table_info.rows)
            all_tables[table] = table_info


if __name__ == "__main__":
    args = parse_args()
    main(args["directory"], args["threads"])
