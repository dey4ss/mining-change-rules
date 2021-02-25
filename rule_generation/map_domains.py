#!/usr/bin/python3

import argparse
import json
import os
import zipfile
from collections import defaultdict


def parse_args():
    ap = argparse.ArgumentParser(
        description="Reads log files and creates mappings domain -> tables and table -> domain"
    )
    ap.add_argument("log_dir", type=str, help="Path to the log files")
    ap.add_argument("output_dir", type=str, help="Output directory")
    return vars(ap.parse_args())


def main(log_dir, output_dir):
    log_files = sorted(os.listdir(log_dir))
    domains = defaultdict(set)
    for log_file in log_files:
        print(log_file)
        with zipfile.ZipFile(os.path.join(log_dir, log_file)) as log_zip:
            domain_logs = [file_name for file_name in log_zip.namelist() if file_name.endswith(".log")]
            for domain_log in domain_logs:
                domain = domain_log.split(os.sep)[-1][: -(len("_urls.log"))]
                with log_zip.open(domain_log) as domain_log_file:
                    lines = domain_log_file.readlines()
                    for line in lines:
                        decoded_line = line.decode("utf-8")
                        if not decoded_line.startswith("Saving to:"):
                            continue
                        table = decoded_line.split(os.sep)[-1][:9]
                        domains[domain].add(table)

    domains_serializable = {k: list(v) for k, v in domains.items()}

    with open(os.path.join(output_dir, "domain_to_tables.json"), "w") as f:
        json.dump(domains_serializable, f)

    tables_to_domain = dict()
    for domain, tables in domains.items():
        for table in tables:
            tables_to_domain[table] = domain

    with open(os.path.join(output_dir, "table_to_domain.json"), "w") as f:
        json.dump(tables_to_domain, f)


if __name__ == "__main__":
    args = parse_args()
    main(args["log_dir"], args["output_dir"])
