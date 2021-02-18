#!/usr/bin/python3

import zipfile
import os
import json
from collections import defaultdict


def main():
    log_path = "/san2/data/change-exploration/socrata/logs/"
    log_files = sorted(os.listdir(log_path))
    domains = defaultdict(set)
    for log_file in log_files:
        print(log_file)
        with zipfile.ZipFile(os.path.join(log_path, log_file)) as log_zip:
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

    with open("domain_to_tables.json", "w") as f:
        json.dump(domains_serializable, f)

    tables_to_domain = dict()
    for domain, tables in domains.items():
        for table in tables:
            tables_to_domain[table] = domain

    with open("table_to_domain.json", "w") as f:
        json.dump(tables_to_domain, f)


if __name__ == "__main__":
    main()
