#!/usr/bin/python3

import argparse
import os
import sys
from collections import defaultdict

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + f"{os.sep}..")
from util.util import date_range, file_extension, Entity, Field


def parse_args():
    ap = argparse.ArgumentParser(description="Counts entity changes")
    ap.add_argument("change_dir", type=str, help="Directory of the change files")
    return vars(ap.parse_args())


def main(change_dir):
    dates = sorted(list({file_name[:10] for file_name in os.listdir(change_dir) if file_name.startswith("20")}))
    print(len(dates))
    change_types = ["insert", "delete", "update"]
    entities = Entity.string_representations()
    res = {e: {t: set() for t in change_types} for e in entities}

    for date in dates:
        print(date)
        for change_type in change_types:
            with open(os.path.join(change_dir, f"{date}_{change_type}.csv")) as f:
                for line in f:
                    if not line:
                        continue
                    table, column, row = line.strip().split(";")
                    res["table"][change_type].add(table)
                    res["column"][change_type].add(f"{table}_{column}")
                    res["row"][change_type].add(f"{table}_{row}")
                    res["field"][change_type].add(f"{table}_{column}_{row}")

    print("")
    for e, change_counts in res.items():
        print(e)
        for t, count in change_counts.items():
            print(f"\t{t}: {len(count)}")


if __name__ == "__main__":
    args = parse_args()
    main(args["change_dir"])
