#!/usr/bin/python3

import argparse
import json
import multiprocessing as mp
import os
import py7zr
import queue
import sys
import yaml
from collections import defaultdict, Counter
from datetime import datetime

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + f"{os.sep}..")
from util.util import date_range, file_extension, Entity, Field


def parse_args():
    ap = argparse.ArgumentParser(description="Extracts changes for Wikipedia infoboxes.")
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
    ap.add_argument(
        "--granularity", "-g", type=str, help="Time granularity. Default d", default="d", choices=["d", "h"]
    )
    ap.add_argument(
        "--whitelist", "-w", type=str, help="YAML file containing categorized infobox names. Default None", default=None
    )
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


def extract_changes(dir, file_queue, my_id, entity, start, end, granularity, whitelist):
    changes = defaultdict(set)
    store_pages = True  # entity in [Entity.Row, Entity.Field]
    pages = defaultdict(set)
    template_aliases = defaultdict(lambda: [None, Counter()])
    current_changes = list()
    current_page_id = None
    latest_infobox_names = dict()
    key_to_page = dict()
    timestamp_extractors = {"d": lambda x: x[:10], "h": lambda x: x[:13]}
    get_timestamp = timestamp_extractors[granularity]

    print(f"[Start Worker {my_id}]")
    while True:
        try:
            file_name = file_queue.get_nowait()
        except queue.Empty:
            print(f"[Exit Worker {my_id}]")
            return changes, pages, dict(template_aliases), key_to_page

        file_path = os.path.join(dir, file_name)
        with py7zr.SevenZipFile(file_path) as archive:
            meta_files = archive.readall().items()
        for meta_file in meta_files:
            print(f"{meta_file[0]} (Worker {my_id})")
            # i = 0
            for line in meta_file[1]:
                item = json.loads(line)
                if not "template" in item:
                    continue
                template = item["template"].lower()
                timestamp = item["validFrom"]
                year = timestamp[:4]
                if (not "infobox" in template) or (year < start) or (year > end) or (item["type"] == "CREATE"):
                    continue

                # i += 1
                key = item["key"]
                page_id = item["pageID"]
                if current_page_id is None:
                    current_page_id = page_id
                elif current_page_id != page_id:
                    for change in current_changes:
                        change.table = latest_infobox_names[change.row]
                        change_id = f"{change.get_id(entity)}_{change.type}"
                        if (not whitelist) or (whitelist and change.table in whitelist):
                            changes[change_id].add(change.date)
                    # print(i, latest_infobox_name, key, current_infobox_key)
                    for infobox_key, alias in latest_infobox_names.items():
                        template_aliases[infobox_key][0] = alias
                        key_to_page[infobox_key] = current_page_id
                    # print({f"{change.get_id(entity)}_{change.type}" for change in current_changes})
                    # print(template_aliases[current_infobox_key][0], template_aliases[current_infobox_key][1].most_common())
                    current_changes = list()
                    current_page_id = page_id
                    latest_infobox_names = dict()
                    # return changes, pages, dict(template_aliases)

                infobox = sanitize(template)
                latest_infobox_names[key] = infobox
                template_aliases[key][1][infobox] += 1
                # print("add", 1, "to", infobox, "for", key)
                # skip if this is the initial insert of the infobox
                # opt = [len(template_aliases[key][1]) == 0 and item["type"] == "CREATE"]
                # if len(template_aliases[key][1]) == 0 and item["type"] == "CREATE":
                # if all(opt):
                #    continue
                # elif any(opt):
                #    print(key, item["pageID"], opt)

                if store_pages:
                    page_name = item["pageTitle"]
                    pages[page_id].add(page_name)
                item_changes = item["changes"]
                ts = get_timestamp(timestamp)
                # year = date[:4]
                # if year < start or year > end:
                #    print(start, end, date)
                #    continue
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
                        change_obj = Field(infobox, column, key)
                        change_obj.date = ts
                        change_obj.type = change_type
                        current_changes.append(change_obj)


def main(change_dir, out, entity, threads, start, end, granularity, whitelist):
    with mp.Manager() as manager:
        changes = defaultdict(set)
        pages = dict()
        aliases = dict()
        keys_to_pages = dict()
        files = sorted([f for f in os.listdir(change_dir) if f.endswith(".7z")])  # [-3:]
        job_queue = manager.Queue()
        for f in files:
            job_queue.put(f)

        tasks = list()
        for thread in range(threads):
            thread_id = str(thread + 1).rjust(2)
            tasks.append((change_dir, job_queue, thread_id, entity, start, end, granularity, whitelist))

        results = None
        with mp.Pool(processes=threads) as pool:
            results = pool.starmap(extract_changes, tasks)

        for partial_result, t_id in zip(results, range(1, len(results) + 1)):
            print(f"Merging result {t_id}/{threads}")
            if partial_result is None:
                print(f"Warning: No results from worker {t_id}!")
                continue
            partial_changes, partial_pages, partial_aliases, partial_keys_to_pages = partial_result
            if partial_changes:
                for change, occurences in partial_changes.items():
                    changes[change] = changes[change].union(occurences)
            if partial_pages:
                pages.update(partial_pages)
            if partial_aliases:
                aliases.update(partial_aliases)
            if partial_keys_to_pages:
                keys_to_pages.update(partial_keys_to_pages)

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
        with open(f"{out}.template_aliases.json", "w", encoding="utf-8") as f:
            json.dump(aliases, f)
        with open(f"{out}.keys_to_pages.json", "w", encoding="utf-8") as f:
            json.dump(keys_to_pages, f)


def parse_whitelist(whitelist_path):
    if not whitelist_path:
        return None
    with open(whitelist_path, encoding="utf-8") as f:
        whitelist_categorized = yaml.load(f, Loader=yaml.CLoader)
    whitelist = list()
    for whitelist_part in whitelist_categorized.values():
        whitelist += whitelist_part
    return whitelist


if __name__ == "__main__":
    args = parse_args()
    start = datetime.now()
    print("start:", start)

    entity = [e for e in Entity if e.to_str() == args["entity"]][0]
    whitelist = parse_whitelist(args["whitelist"])
    main(
        args["change_dir"],
        args["out"],
        entity,
        args["threads"],
        str(args["start_year"]),
        str(args["final_year"]),
        args["granularity"],
        whitelist,
    )

    end = datetime.now()
    print("end:", end)
    print("duration:", end - start)
