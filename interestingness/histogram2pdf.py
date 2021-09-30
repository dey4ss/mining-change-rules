from operator import itemgetter
import json
import random
from scipy.special import logsumexp
import utils
import numpy as np
import os

######## Kullback Leiber Divergence ########
def kl_divergence(p, q):
    return sum(
        p[i] * logsumexp(p[i] / max(0.000000001, q[i])) for i in range(len(p))
    )  # edited because of 0 division error to logsumexp


######## Jenson Shannon Divergence ########
def js_divergence(p, q):
    m = 0.5 * (p + q)
    return 0.5 * kl_divergence(p, m) + 0.5 * kl_divergence(q, m)


######## Calculates probability distribution and interestingness scores ########
def hist2pdf(data_path, output_dir, days_t=365.0, conf_t=0.9, min_sup_t=0.05, max_sup_t=0.1, antecent=True):

    data = utils.read_rules(data_path)  # use `json.loads` to do the reverse
    high_scores = []
    # Sample one rule within thresholds from each ancedent and sample
    general_sample = [
        random.sample(
            {
                k: v
                for (k, v) in entities_j.items()
                if v[1] > conf_t and (v[0] / days_t) > min_sup_t and (v[0] / days_t) < max_sup_t
            }.items(),
            1,
        )[0][1][3]
        for entity_i, entities_j in data.items()
        if len(
            {
                k: v
                for (k, v) in entities_j.items()
                if v[1] > conf_t and (v[0] / days_t) > min_sup_t and (v[0] / days_t) < max_sup_t
            }
        )
        > 0
    ]
    days = len(general_sample[0])
    general_pdf_absolute_support_days = [0] * days
    for day in range(days):
        for sample in general_sample:
            general_pdf_absolute_support_days[day] = general_pdf_absolute_support_days[day] + sample[day]
    general_pdf_absolute_support_sum = sum(general_pdf_absolute_support_days)
    general_pdf = [day_support / general_pdf_absolute_support_sum for day_support in general_pdf_absolute_support_days]

    # PDF Generation and interestingness score
    for entity_i, entities_j in data.items():
        # Sup/Conf Thresholds
        for entity_j, histogram in entities_j.items():
            if (
                ((histogram[0] / days_t) < min_sup_t)
                or ((histogram[0] / days_t) > max_sup_t)
                or (histogram[1] < conf_t)
            ):
                continue
            domain_rule = histogram[4]
            histogram = histogram[3]
            rule_absolute_support = sum(histogram)
            days_relative_support = []
            for day_support in histogram:
                days_relative_support.append(day_support / rule_absolute_support)
            score = js_divergence(np.array(general_pdf), np.array(days_relative_support))
            high_scores.append((str(entity_i + " => " + entity_j), days_relative_support, score, domain_rule))

    high_scores = sorted(high_scores, key=itemgetter(2), reverse=True)

    with open(os.path.join(output_dir, "highscores.txt"), "w+") as file:
        file.write(json.dumps(high_scores))  # use `json.loads` to do the reverse
    with open(os.path.join(output_dir, "general_pdf.txt"), "w+") as file:
        file.write(json.dumps(general_pdf))  # use `json.loads` to do the reverse
