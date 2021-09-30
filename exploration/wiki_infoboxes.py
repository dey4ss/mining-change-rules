#!/usr/bin/python3

import argparse
import json
import multiprocessing as mp
import os
import py7zr
import queue
import sys
from collections import defaultdict, Counter
from datetime import datetime

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + f"{os.sep}..")
from util.util import date_range, file_extension, Entity, Field


def parse_args():
    ap = argparse.ArgumentParser(description="Discovers how often Wikipedia infbobox templates are used.")
    ap.add_argument("change_dir", type=str, help="Directory of the change archives.")
    ap.add_argument("out", type=str, help="Output file.")
    ap.add_argument(
        "--entity",
        "-e",
        type=str,
        help=f"Entity to aggregate. Default: {Entity.Field.to_str()}",
        choices=[e.to_str() for e in Entity],
        default=Entity.Field.to_str(),
    )
    ap.add_argument("--threads", "-t", type=int, help="Number of threads. Default 2", default=2)
    ap.add_argument("--start_year", "-s", type=int, help="Start year. Default 2003", default=2003)
    ap.add_argument("--final_year", "-f", type=int, help="Final year. Default 2019", default=2019)
    return vars(ap.parse_args())


def check_null_value(change, key):
    if key not in change:
        return None
    value = change[key].strip()
    if value:
        return value
    return None


def sanitize(string):
    return string.strip("\n\t -_").replace(" ", "-").replace("\n", "-").replace("_", "-").replace("\t", "")


def extract_changes(dir, file_queue, my_id, entity, start, end):
    infoboxes = Counter()
    current_page_id = None
    current_infoboxes = dict()

    print(f"[Start Worker {my_id}]")
    while True:
        try:
            file_name = file_queue.get_nowait()
        except queue.Empty:
            print(f"[Exit Worker {my_id}]")
            return infoboxes

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
                year = item["validFrom"][:4]
                if (not "infobox" in template.lower()) or (item["type"] == "CREATE"):
                    continue
                key = item["key"]
                page_id = item["pageID"]
                if current_page_id is None:
                    current_page_id = page_id
                elif current_page_id != page_id:
                    for inf in current_infoboxes.values():
                        infoboxes[inf] += 1
                    current_page_id = page_id
                    current_infoboxes = dict()

                infobox = sanitize(template.lower())
                current_infoboxes[key] = infobox


def main(change_dir, out, entity, threads, start, end):
    with mp.Manager() as manager:
        infoboxes = Counter()

        files = sorted([f for f in os.listdir(change_dir) if f.endswith(".7z")])
        job_queue = manager.Queue()
        for f in files:
            job_queue.put(f)

        tasks = list()
        for thread in range(threads):
            thread_id = str(thread + 1).rjust(2)
            tasks.append((change_dir, job_queue, thread_id, entity, start, end))

        results = None
        with mp.Pool(processes=threads) as pool:
            results = pool.starmap(extract_changes, tasks)

        for partial_result, t_id in zip(results, range(1, len(results) + 1)):
            print(f"Merging result {t_id}/{threads}")
            if partial_result is None:
                print(f"Warning: No results from worker {t_id}!")
                continue
            for i, c in partial_result.items():
                infoboxes[i] += c
        print("Saving")

        del results

        with open(out, "w", encoding="utf-8") as f:
            json.dump(infoboxes, f)
        for i, c in infoboxes.most_common():
            if c > 100:
                print(f"{i}\t{c}")
        with open(f"{out}.mc.txt", "w", encoding="utf-8") as f:
            for i, c in infoboxes.most_common():
                f.write(f"{i}\t{c}\n")


if __name__ == "__main__":
    args = parse_args()
    start = datetime.now()
    print("start:", start)

    entity = [e for e in Entity if e.to_str() == args["entity"]][0]
    main(args["change_dir"], args["out"], entity, args["threads"], str(args["start_year"]), str(args["final_year"]))

    end = datetime.now()
    print("end:", end)
    print("duration:", end - start)
