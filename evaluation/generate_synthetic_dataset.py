#!/usr/bin/python3

import random
import json
import math


def main(num_timepoints, num_changes, min_sup, max_sup, injection_ratio, rejection_ratio, window_size):
    time_domain = list(range(num_timepoints))
    time_domain2 = list(range(0, num_timepoints, 2))
    abs_min_sup = math.ceil(num_timepoints * min_sup)
    abs_max_sup = math.floor(num_timepoints * max_sup)
    print(abs_min_sup, abs_max_sup)
    num_injected_values = round(num_changes * injection_ratio)
    num_rejected_values = round(num_changes * rejection_ratio)

    print(f"add noise with {num_changes - num_injected_values} changes")
    change_occurrences = dict()
    for timepoint in range(num_changes - num_injected_values - num_rejected_values):
        # equally random distributed, maybe we want to change this
        num_occurrences = random.randint(abs_min_sup, abs_max_sup)
        occurrences = sorted(random.sample(time_domain, num_occurrences))
        change_occurrences[f"noise_{timepoint}"] = occurrences

    print("\ninject dependencies")
    injected_values = dict()
    for timepoint in range(0, num_injected_values, 2):
        t1 = f"inject_{timepoint}"
        t2 = f"inject_{timepoint + 1}"
        num_peaks = random.randint(1, 2)
        peaks = sorted(random.sample(range(1, window_size - 1), num_peaks))
        # print(peaks)
        start = random.sample(range(window_size), 1)[0]
        # print(start)
        max_peak = max(peaks)
        # restrict t1 occur after peak to freely choose t2 later on
        domain_t1 = range(start, num_timepoints - max_peak, max_peak + 1)
        max_possible_occurrences = math.floor((num_timepoints - start - max_peak) / (max_peak + 1))
        num_occurrences = random.randint(abs_min_sup, min(abs_max_sup, max_possible_occurrences))
        occurrences_t1 = sorted(random.sample(domain_t1, num_occurrences))
        # print(len(occurrences_t1))
        histogram = [0 for _ in range(window_size)]

        assert occurrences_t1[0] >= 0

        planned_conf = random.uniform(0.9, 1.0)
        planned_sup = max(abs_min_sup, min(abs_max_sup, int(round(planned_conf * len(occurrences_t1)))))
        planned_conf = planned_sup / len(occurrences_t1)
        # print(planned_conf, planned_sup)

        # occurrences of t1 that will have valid rule occurrences
        chosen_t1 = sorted(random.sample(occurrences_t1, planned_sup))

        # pre-allocate to prevent .append()
        occurrences_t2 = [0 for _ in range(planned_sup)]
        for i in range(planned_sup):
            t1_value = occurrences_t1[i]
            value = t1_value + random.sample(peaks, 1)[0]
            histogram[value - t1_value] += 1
            occurrences_t2[i] = value
        while len(occurrences_t2) < abs_min_sup:
            occurrences_t2.append(max(occurrences_t2[-1] + 1, num_timepoints + 1))
        change_occurrences[t1] = occurrences_t1
        change_occurrences[t2] = occurrences_t2
        injected_values[f"{t1}_{t2}"] = (planned_sup, planned_conf, histogram)
        print(
            f"\nsup {planned_sup} ({round(planned_sup / num_timepoints, 2)}), conf {planned_sup / len(occurrences_t1) }\n    {t1} {occurrences_t1}\n    {t2} {occurrences_t2}"
        )

    print("\nreject dependencies")
    rejected_values = dict()
    reject_reasons = ["min_sup", "min_conf"]
    num_iteration = 0
    for timepoint in range(0, num_rejected_values, 2):
        t1 = f"reject_{timepoint}"
        t2 = f"reject_{timepoint + 1}"
        reject_reason = reject_reasons[num_iteration % len(reject_reasons)]
        if reject_reason == "min_sup":
            planned_sup = random.randint(1, abs_min_sup - 1)
            planned_conf = 1
        elif reject_reason == "min_conf":
            planned_conf = random.uniform(0.1, 0.8)
            planned_sup = random.randint(abs_min_sup, abs_max_sup)
        else:
            raise ValueError(f"Invalid reject reason: {reject_reason}")

        occurrences_t1 = sorted(random.sample(time_domain2, num_occurrences))

        assert occurrences_t1[0] >= 0

        failure_sup = random.randint(0, 1) == 1

        if failure_sup:
            planned_sup = random.randint(1, abs_min_sup - 1)
        else:
            planned_conf = random.uniform(0.1, 0.8)
            planned_sup = min(int(round(planned_conf * len(occurrences_t1))), abs_max_sup)
            planned_conf = planned_conf = planned_sup / len(occurrences_t1)

        # pre-allocate to prevent .append()
        occurrences_t2 = [0 for _ in range(planned_sup)]
        for i in range(planned_sup):
            next_elem = occurrences_t1[i + 1] if i < len(occurrences_t1) - 1 else num_timepoints
            t1_value = occurrences_t1[i]
            value = random.randint(t1_value, min(next_elem - 1, t1_value + window_size - 1))
            # value = t1_value
            occurrences_t2[i] = value
        assert len(occurrences_t2) == planned_sup
        while len(occurrences_t2) < abs_min_sup:
            occurrences_t2.append(max(occurrences_t2[-1] + 1, num_timepoints + 1))
        change_occurrences[t1] = occurrences_t1
        change_occurrences[t2] = occurrences_t2
        rejected_values[f"{t1}_{t2}"] = (planned_sup, planned_conf)
        print(
            f"\nsup {planned_sup} ({round(planned_sup / num_timepoints, 2)}), conf {planned_sup / len(occurrences_t1) }\n    {t1} {occurrences_t1}\n    {t2} {occurrences_t2}"
        )

    print("\nsave")

    with open("test_data/synthetic_changes.json", "w", encoding="utf-8") as f:
        json.dump(change_occurrences, f)

    with open("test_data/synthetic_timepoints.json", "w", encoding="utf-8") as f:
        json.dump(time_domain, f)

    with open("test_data/injected_dependencies.json", "w", encoding="utf-8") as f:
        json.dump(injected_values, f)

    with open("test_data/rejected_dependencies.json", "w", encoding="utf-8") as f:
        json.dump(rejected_values, f)


if __name__ == "__main__":
    num_timepoints = 500
    num_changes = 1000
    min_sup = 0.05
    max_sup = 0.4
    injection_ratio = 0.01
    rejection_ratio = 0.01
    window_size = 7

    main(num_timepoints, num_changes, min_sup, max_sup, injection_ratio, rejection_ratio, window_size)
