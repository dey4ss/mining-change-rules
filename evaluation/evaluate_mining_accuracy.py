#!/usr/bin/python3

from collections import defaultdict
import json
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + f"{os.sep}..")
from util.util import read_rule

# The CSV file containing change dependencies stems fom the application of rule_generation/create_histograms
# executed data generated by generate_synthetic_dataset.
# These change dependencies are ranked by interestingness/histogram2pdf.py


class DependencyCount:
    def __init__(self):
        self._supports = list()
        self._confidences = list()
        self._histograms = list()

    def add_observation(self, sup, conf, hist):
        self._supports.append(sup)
        self._confidences.append(conf)
        self._histograms.append(hist)

    def count(self):
        assert len(self._supports) == len(self._confidences)
        assert len(self._supports) == len(self._histograms)
        return len(self._supports)

    def supports(self):
        return self._supports

    def confidences(self):
        return self._confidences

    def histograms(self):
        return self._histograms

    def statistics(self):
        return [(sup, conf, hist) for sup, conf, hist in zip(self._supports, self._confidences, self._histograms)]


def main():
    with open("test_data/injected_dependencies.json") as f:
        injected_dependencies = json.load(f)

    with open("test_data/rejected_dependencies.json") as f:
        rejected_dependencies = json.load(f)

    found_dependencies = defaultdict(lambda: DependencyCount())
    with open("test_data/result.csv") as f:
        for line in f:
            antecedent, consequent, result = read_rule(line)
            found_dependencies[f"{antecedent}_{consequent}"].add_observation(result[0], result[1], result[3])

    print("\ninjected")
    num_failed_injected = 0
    for dep, stats in injected_dependencies.items():
        print(dep)
        count_object = found_dependencies[dep]
        if count_object.count() != 1:
            print(f"\tcount is {count_object.count()}, expected 1")
            num_failed_injected += 1
            continue
        sup = count_object.supports()[0]
        if sup != stats[0]:
            print(f"\tsup is {sup}, expected {stats[0]}")
            num_failed_injected += 1
            continue
        conf = count_object.confidences()[0]
        if conf != stats[1]:
            print(f"\tconf is {sup}, expected {stats[1]}")
            num_failed_injected += 1
            continue
        hist = count_object.histograms()[0]
        if hist != stats[2]:
            print(f"\thistogram is {hist}, expected {stats[2]}")
            num_failed_injected += 1
            continue

    print("\nrejected")
    num_failed_rejected = 0
    for dep, stats in rejected_dependencies.items():
        print(dep)
        count_object = found_dependencies[dep]
        if count_object.count() != 0:
            print(f"\tcount is {count_object.count()}, expected 0")
            print(f"\t{dep}", count_object.statistics(), stats)
            num_failed_rejected += 1

    print("\nRanking")
    with open("test_data/highscores.txt") as f:
        ranked_dependencies = json.load(f)

    num_top_ranked_dependencies = 0
    for rank, ranked_dependency in zip(
        range(1, len(injected_dependencies) + 1), ranked_dependencies[: len(injected_dependencies)]
    ):
        identifier = ranked_dependency[0]
        canonical_identifier = "_".join(identifier.split(" => "))
        if canonical_identifier in injected_dependencies:
            num_top_ranked_dependencies += 1
            print(f"Rank {rank}: {canonical_identifier}")

    print("\nSuccess" if not any([num_failed_rejected, num_failed_injected]) else "\nFailed")
    print(
        f"{len(injected_dependencies) - num_failed_injected} / {len(injected_dependencies)} injected dependencies correct"
    )
    print(
        f"{len(rejected_dependencies) - num_failed_rejected} / {len(rejected_dependencies)} rejected dependencies correct"
    )
    print(
        f"{num_top_ranked_dependencies} / {len(injected_dependencies)} injected dependencies ranked top {len(injected_dependencies)}"
    )


if __name__ == "__main__":
    main()