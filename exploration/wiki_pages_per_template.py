#!/usr/bin/python3

import argparse
import json
import os
import sys
from collections import Counter
from datetime import datetime

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + f"{os.sep}..")
from util.util import date_range, file_extension, Entity, Field


def parse_args():
    ap = argparse.ArgumentParser(description="Counts Wikipedia infobox templates.")
    ap.add_argument("alias_file", type=str, help="File with infobox template matching.")
    ap.add_argument("out", type=str, help="Output file.")
    return vars(ap.parse_args())


def main(alias_file, out):

    with open(alias_file, encoding="utf-8") as f:
        aliases = json.load(f)
    templates_per_name = Counter()

    for alias_info in aliases.values():
        chosen_template = alias_info[0]
        templates_per_name[chosen_template] += 1

    with open(out, "w", encoding="utf-8") as f:
        for template, count in templates_per_name.most_common():
            f.write(f"{template}: {count}\n")


if __name__ == "__main__":
    args = parse_args()
    start = datetime.now()
    print("start:", start)
    main(args["alias_file"], args["out"])

    end = datetime.now()
    print("end:", end)
    print("duration:", end - start)
