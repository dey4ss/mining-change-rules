#!/usr/bin/python3

import argparse
import json
import multiprocessing as mp
import os
import py7zr
import queue
import sys
from datetime import datetime

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + f"{os.sep}..")
from util.util import date_range, file_extension, Entity, Field


def parse_args():
    ap = argparse.ArgumentParser(description="Maps template keys to page IDs.")
    ap.add_argument("change_dir", type=str, help="Directory of the change archives.")
    ap.add_argument("out", type=str, help="Output file.")
    ap.add_argument("--threads", "-t", type=int, help="Number of threads. Default 2", default=2)
    return vars(ap.parse_args())


def extract_changes(dir, file_queue, my_id):
    key_page_ids = dict()

    print(f"[Start Worker {my_id}]")
    while True:
        try:
            file_name = file_queue.get_nowait()
        except queue.Empty:
            print(f"[Exit Worker {my_id}]")
            return key_page_ids

        file_path = os.path.join(dir, file_name)
        with py7zr.SevenZipFile(file_path) as archive:
            meta_files = archive.readall().items()
        for meta_file in meta_files:
            print(f"{meta_file[0]} (Worker {my_id})")
            for line in meta_file[1]:
                item = json.loads(line)
                if not "template" in item:
                    continue
                template = item["template"]
                if not template.startswith("infobox"):
                    continue

                template_key = item["key"]
                page_id = item["pageID"]
                key_page_ids[template_key] = page_id


def main(change_dir, out, threads):
    key_page_ids = dict()
    with mp.Manager() as manager:
        files = sorted([f for f in os.listdir(change_dir) if f.endswith(".7z")])
        job_queue = manager.Queue()
        for f in files:
            job_queue.put(f)

        tasks = list()
        for thread in range(threads):
            thread_id = str(thread + 1).rjust(2)
            tasks.append((change_dir, job_queue, thread_id))

        results = None
        with mp.Pool(processes=threads) as pool:
            results = pool.starmap(extract_changes, tasks)

        for partial_result, t_id in zip(results, range(1, len(results) + 1)):
            print(f"Merging result {t_id}/{threads}")
            if partial_result is None:
                print(f"Warning: No results from worker {t_id}!")
                continue
            key_page_ids.update(partial_result)

        print("Saving")
        del results
        with open(out, "w", encoding="utf-8") as f:
            json.dump(key_page_ids, f)


if __name__ == "__main__":
    args = parse_args()
    start = datetime.now()
    print("start:", start)
    main(args["change_dir"], args["out"], args["threads"])

    end = datetime.now()
    print("end:", end)
    print("duration:", end - start)
