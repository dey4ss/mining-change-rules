#!/usr/bin/python3
import json
import os

from util import read_rule


def domains_from_id(change, table_domains, change_groups):
    domains = set()
    table_id = lambda x: x[:9]
    if not change.startswith("group"):
        domains.add(table_domains[table_id(change)])
    else:
        for item in change_groups[change]:
            domains.add(table_domains[table_id(item)])
    return domains


def main():
    path = "histograms_columns_not_periodic_filtered"
    with open("table_to_domain.json") as f:
        table_domains = json.load(f)

    with open(os.path.join(path, "change_groups.json")) as f:
        change_groups = json.load(f)

    result = list()
    num_before = 0

    with open(os.path.join(path, "histogram_columns_9_05_1.csv")) as f:
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

    with open(os.path.join(path, "histogram_columns_9_05_1_domain_filtered.csv"), "w") as f:
        for l in result:
            f.write(l)


if __name__ == "__main__":
    main()
