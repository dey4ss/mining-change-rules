#!/usr/bin/python3

import argparse
import json
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + f"{os.sep}..")
from util.util import read_rule


def parse_args():
    ap = argparse.ArgumentParser(
        description="Filter change dependencies s.t. left and right side share the same domain"
    )
    ap.add_argument("rule_file", type=str, help="File with rules stored as csv")
    ap.add_argument("table_domain_file", type=str, help="File with an index table -> domain, python dict as JSON file")
    ap.add_argument(
        "change_groups_file", type=str, help="File with an index change group -> changes, python dict as JSON file"
    )
    return vars(ap.parse_args())


def domains_from_id(change, table_domains, change_groups):
    domains = set()
    table_id = lambda x: x[:9]
    if not change.startswith("group"):
        domains.add(table_domains[table_id(change)])
    else:
        for item in change_groups[change]:
            domains.add(table_domains[table_id(item)])
    return domains


def main(rule_file, table_domain_file, change_groups_file):
    with open(table_domain_file) as f:
        table_domains = json.load(f)

    with open(change_groups_file) as f:
        change_groups = json.load(f)

    result = list()
    num_before = 0

    with open(rule_file) as f:
        for line in f:
            num_before += 1
            parts = line.split(";")
            left_id = parts[0]
            right_id = parts[1]
            left_domains = domains_from_id(left_id, table_domains, change_groups)
            right_domains = domains_from_id(right_id, table_domains, change_groups)
            domain = left_domains & right_domains
            if len(domain) == 0:
                continue
            line_strip = line.strip()
            if len(domain) > 1:
                print(domain)
            result.append(line_strip + ";" + list(domain)[0] + "\n")
    print("before:", num_before)
    print("now", len(result))

    with open(f"{rule_file}_domain_filtered.csv", "w") as f:
        for l in result:
            f.write(l)


if __name__ == "__main__":
    args = parse_args()
    main(args["rule_file"], args["table_domain_file"], args["change_groups_file"])
