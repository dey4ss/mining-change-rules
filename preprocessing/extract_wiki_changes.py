#!/usr/bin/python3

import argparse
import json
import multiprocessing as mp
import os
import py7zr
import queue
import sys
from collections import defaultdict
from datetime import datetime

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + f"{os.sep}..")
from util.util import date_range, file_extension, Entity, Field

def parse_args():
    ap = argparse.ArgumentParser(description="Extracts changes for Wikipedia infoboxes.")
    ap.add_argument("change_dir", type=str, help="Directory of the change archives.")
    ap.add_argument("out", type=str, help="Output file.")
    ap.add_argument(
        "--entity", "-e", type=str, help=f"Entity to aggregate. Default: {Entity.Field.to_str()}", choices=[e.to_str() for e in Entity], default=Entity.Field.to_str()
    )
    ap.add_argument("--threads", "-t", type=int, help="Number of threads. Default 2", default=2)
    return vars(ap.parse_args())


def check_null_value(change, key):
    if key not in change:
        return None
    value = change[key].strip()
    if value:
        return value
    return None


def sanitize(string):
    return string.strip("\n\t -_").replace(" ","-").replace("\n","-").replace("_","-")


def extract_changes(dir, file_queue, my_id, entity):
    changes = defaultdict(set)
    store_pages = entity in [Entity.Row, Entity.Field]
    pages = defaultdict(set)

    print(f"[Start Worker {my_id}]")
    while True:
        try:
            file_name = file_queue.get_nowait()
        except queue.Empty:
            print(f"[Exit Worker {my_id}]")
            return changes, pages

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

                infobox = sanitize(template[len("infobox"):])
                page_id = item["pageID"]
                if store_pages:
                    page_name = item["pageTitle"]
                    pages[page_id].add(page_name)
                item_changes = item["changes"]
                timestamp = item["validFrom"]
                date = timestamp[:10]
                for item_change in item_changes:
                    column = sanitize(item_change["property"]["name"])
                    old_value = check_null_value(item_change, "previousValue")
                    new_value = check_null_value(item_change, "currentValue")
                    if old_value != new_value:
                        change_type = None
                        if old_value is not None and new_value is not None:
                            change_type = "u"
                        elif old_value is None and new_value is not None:
                            change_type = "i"
                        elif old_value is not None and new_value is None:
                            change_type = "d"
                        change = Field.get_with_level(entity, infobox, column, str(page_id))
                        change_id = f"{change.get_id(entity)}_{change_type}"
                        changes[change_id].add(date)

def main(change_dir, out, entity, threads):
    with mp.Manager() as manager:
        changes = defaultdict(set)
        pages = dict()
        files = sorted([f for f in os.listdir(change_dir) if f.endswith(".7z")])
        job_queue = manager.Queue()
        for f in files:
            job_queue.put(f)

        tasks = list()
        for thread in range(threads):
            thread_id = str(thread + 1).rjust(2)
            tasks.append((change_dir, job_queue, thread_id, entity))

        results = None
        with mp.Pool(processes=threads) as pool:
            results = pool.starmap(extract_changes, tasks)

        for partial_result, t_id in zip(results, range(1, len(results) + 1)):
            print(f"Merging result {t_id}/{threads}")
            if partial_result is None:
                print(f"Warning: No results from worker {t_id}!")
                continue
            partial_changes, partial_pages = partial_result
            for change, occurences in partial_changes.items():
                changes[change] = changes[change].union(occurences)
            if partial_pages:
                pages.update(partial_pages)

        print("Saving")

        del results
        for change, occurrences in changes.items():
            changes[change] = sorted(occurrences)

        with open(out, "w", encoding="utf-8") as f:
            json.dump(changes, f)
        if len(pages) > 0:
            for page_id, page_names in pages.items():
                pages[page_id] = sorted(page_names)
            with open(f"{out}.pages.json", "w", encoding="utf-8") as f:
                json.dump(pages, f)


if __name__ == "__main__":
    args = parse_args()
    start = datetime.now()
    print("start:", start)

    entity = [e for e in Entity if e.to_str() == args["entity"]][0]
    main(args["change_dir"], args["out"], entity, args["threads"])

    end = datetime.now()
    print("end:", end)
    print("duration:", end - start)
