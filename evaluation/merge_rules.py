#!/usr/bin/python3

import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import datetime

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + f"{os.sep}..")
from util.util import read_rule


def parse_args():
    ap = argparse.ArgumentParser(description="Finds dependencies with time difference")
    ap.add_argument("dependency_dir", type=str, help="Folder with change dependencies.")
    ap.add_argument("category", type=str, help="Infobox category.")
    ap.add_argument("--start", "-s", type=int, help="Start year. Default 2016", default=2016)
    ap.add_argument("--end", "-e", type=int, help="End year. Default 2017", default=2017)
    ap.add_argument("--list", "-l", action="store_true", help="List external dependencies")
    ap.add_argument("--clusters", "-c", action="store_true", help="List clusters")
    ap.add_argument(
        "--all_changes",
        "-a",
        type=str,
        help="File containing all changes. If present, change occurrences are printed. Only effective if -l.",
        default=None,
    )
    return vars(ap.parse_args())


def main(dependency_dir, category, start, end, list_external, change_file, print_clusters):
    w = 24
    period = f"{start}-{end}"
    files = [f for f in os.listdir(dependency_dir) if all(c in f for c in [f"{w}_bins", period, category])]

    with open("data/wiki/hourly/page_ids.k_to_p.json", encoding="utf-8") as f:
        key_to_page = json.load(f)

    with open("data/wiki/hourly/page_ids.p_to_name.json", encoding="utf-8") as f:
        page_to_name = json.load(f)

    show_changes = list_external and not change_file is None
    if show_changes:
        with open(change_file, encoding="utf-8") as f:
            all_changes = json.load(f)

    key_from_id = lambda key: key.split("_")[-2]
    out_file_name = os.path.join(dependency_dir, f"{category}_{period}_merged.csv")
    overall_rules = 0
    external_rules = 0
    clusters = dict()
    current_cluster_id = 0

    with open(out_file_name, "w") as o:
        o.write(
            f"antecedent;antecedent_page_id;antecedent_page_name;consequent;consequent_page_id;consequent_page_name;support;confidence;histogram\n"
        )

        for f in files:
            file_name = os.path.join(dependency_dir, f)
            # print("\n", file_name, sep="")
            with open(file_name, encoding="utf-8") as f:
                mylines = list()
                for l in f:
                    rule = read_rule(l)
                    overall_rules += 1
                    # print(rule)
                    a = rule[0]
                    c = rule[1]
                    a_page_id = str(key_to_page[key_from_id(a)])
                    c_page_id = str(key_to_page[key_from_id(c)])
                    hist = rule[2][3]
                    # print(a_page_id, c_page_id)
                    h_str = str(hist)
                    a_page_name = page_to_name[a_page_id]
                    c_page_name = page_to_name[c_page_id]
                    # print(a_page_name, c_page_name)
                    conts = [
                        a,
                        a_page_id,
                        a_page_name,
                        c,
                        c_page_id,
                        c_page_name,
                        str(rule[2][0]),
                        str(rule[2][1]),
                        h_str,
                    ]
                    conts_str = ";".join(conts)
                    o.write(f"{conts_str}\n")
                    # if sum(hist[1:]) / sum(hist) > 0.5:
                    if a_page_id != c_page_id:
                        external_rules += 1
                        # print(a_page_id, c_page_id)
                        if list_external:  # and sum(hist[1:]) / sum(hist) > 0.7:
                            a_link = f"http://en.wikipedia.org/?curid={a_page_id}"
                            c_link = f"http://en.wikipedia.org/?curid={c_page_id}"
                            print("\n", a_page_name, "\t-->\t", c_page_name, "\n", a_link, "\t", c_link, sep="")
                            print(
                                a, c, str(rule[2][0]), str(rule[2][1]), hist, sum(hist[1:]) / sum(hist), sep="\t"
                            )  # )
                            if show_changes:
                                print("antecedent:", sorted(list({occ[:-3] for occ in all_changes[a]})))
                                print("consequent:", sorted(list({occ[:-3] for occ in all_changes[c]})))
                        # if not print_clusters:
                        #    continue
                        hit_ids = list()
                        # kw = ["pcupdate", "club-update"]
                        # if any([any([k in x for k in kw]) for x in [a, c]]):
                        #     continue
                        for cluster_id, cluster in clusters.items():
                            if a in cluster or c in cluster:
                                cluster.add(a)
                                cluster.add(c)
                                hit_ids.append(cluster_id)
                        if len(hit_ids) == 0:
                            clusters[current_cluster_id] = {a, c}
                            current_cluster_id += 1
                        elif len(hit_ids) > 1:
                            # print(f"merge due {a_page_name}\t-->\t{c_page_name}")
                            # print(a, c, sep="\t")
                            new_set = set()
                            for hit_id in hit_ids:
                                # print(f"\t{sorted({page_to_name[str(key_to_page[key_from_id(change)])] for change in clusters[hit_id]})}")
                                new_set = new_set | clusters[hit_id]
                            clusters[hit_ids[0]] = new_set
                            for hit_id in hit_ids[1:]:
                                del clusters[hit_id]
                o.writelines(f.readlines())
    print(
        f"\n{external_rules} / {overall_rules} external ({0 if overall_rules == 0 else round(external_rules / overall_rules * 100, 2)} %)"
    )

    if external_rules < 1:
        return
    print("\nClusters:")
    small_clusters = 0
    large_clusters = list()
    cluster_sizes = defaultdict(int)
    for cluster in clusters.values():
        cluster_pages = {str(key_to_page[key_from_id(change)]) for change in cluster}
        cluster_sizes[len(cluster_pages)] += 1
        # if len(cluster_pages) == 2:
        #    small_clusters += 1
        #    continue
        # titles = [page_to_name[page_id] for page_id in cluster]
        # links = [f"http://en.wikipedia.org/?curid={page_id}" for page_id in cluster]
        # print(len(cluster))
        # print("   ", [page_to_name[page_id] for page_id in cluster])
        # print("   ", [f"http://en.wikipedia.org/?curid={page_id}" for page_id in cluster])
        large_clusters.append((len(cluster_pages), cluster_pages))
    if print_clusters:
        for size, cluster in sorted(large_clusters, key=lambda x: x[0], reverse=True):
            print(f"\nCluster size {size}:")
            for page_id in cluster:
                print(f"    {page_to_name[page_id]}    http://en.wikipedia.org/?curid={page_id}")
        # print(f"\n{small_clusters} clusters with size 2")
    print("\n - Cluster sizes:")
    len_max_size = len(str(max(cluster_sizes.keys())))
    count_max_size = len(str(max(cluster_sizes.values())))
    for size, count in sorted(cluster_sizes.items(), key=lambda x: x[0], reverse=True):
        print(f"    size {str(size).rjust(len_max_size)} - {str(count).rjust(count_max_size)}")


if __name__ == "__main__":
    args = parse_args()
    start = datetime.now()
    print("start:", start)
    main(
        args["dependency_dir"],
        args["category"],
        args["start"],
        args["end"],
        args["list"],
        args["all_changes"],
        args["clusters"],
    )

    end = datetime.now()
    print("end:", end)
    print("duration:", end - start)
