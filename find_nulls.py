#!/usr/local/bin/python3

import argparse
import json
import os
import queue
import multiprocessing as mp

from find_changes import date_range, file_extension


def find_nulls(path, start, end, threads):
    with mp.Manager() as manager:
        null_values = manager.dict()
        job_queue = manager.Queue()
        null_values_lock = manager.RLock()
        workers = [
            mp.Process(target=find_daily_nulls, args=(job_queue, path, null_values, null_values_lock, f"{n}".rjust(2)))
            for n in range(threads)
        ]

        dates = date_range(start, end)
        for date in dates:
            job_queue.put(date)

        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join()

        with open("null_values.csv", "w", encoding="utf-8") as f:
            f.write("value;count\n")
            null_values_lock.acquire()
            for val in null_values:
                f.write(f"{val};{null_values[val]}\n")
            null_values_lock.release()


def find_daily_nulls(job_queue, path, null_values, null_values_lock, n):
    print(f"[Start Worker {n}]")
    candidates = ["-", "/", "", "–", "—", "%"]
    while True:
        date = None
        try:
            date = job_queue.get_nowait()
        except queue.Empty:
            print(f"[Exit Worker {n}]")
            return
        if date is None:
            print(f"[Exit Worker {n}]")
            return

        current_dir = os.path.join(path, date)
        if not os.path.isdir(current_dir):
            continue
        print(f"{date} [Worker {n}]")

        files = [f for f in os.listdir(current_dir) if f.endswith(file_extension())]
        my_null_values = dict()

        for file_name in files:
            with open(os.path.join(current_dir, file_name), "r", encoding="utf-8") as f:
                data = json.loads(f.read())

            for row in data["rows"]:
                for value in row["fields"]:
                    null_val = 1
                    if value is None:
                        null_val = value
                    elif type(value) == str:
                        val_strip = value.strip()
                        if val_strip in candidates or val_strip.lower() == "null":
                            null_val = value
                    if not null_val == 1:
                        if not null_val in my_null_values:
                            my_null_values[null_val] = 0
                        my_null_values[null_val] += 1
        null_values_lock.acquire()
        for null_val in my_null_values:
            if not null_val in null_values:
                null_values[null_val] = 0
            null_values[null_val] += my_null_values[null_val]
        null_values_lock.release()


def parse_args():
    ap = argparse.ArgumentParser(description="Finds null values")
    ap.add_argument("directory", type=str, help="Directory of the change files.")
    ap.add_argument("--threads", type=int, help="Number of threads. Default 2", default=2)
    ap.add_argument("--start", type=str, help="Start date. Default 2019-11-01", default="2019-11-01")
    ap.add_argument("--end", type=str, help="End date. Default 2019-11-08", default="2019-11-08")
    return vars(ap.parse_args())


def main():
    args = parse_args()
    find_nulls(args["directory"], args["start"], args["end"], args["threads"])


if __name__ == "__main__":
    mp.set_start_method("spawn")
    main()
