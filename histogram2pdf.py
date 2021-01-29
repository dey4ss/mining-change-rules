from operator import itemgetter

import pandas as pd
import os
import json
import sklearn
import numpy as np
import random
import math
from scipy.special import logsumexp

######## Kullback Leiber Divergence ########
from sklearn.neighbors import KernelDensity


def kl_divergence(p, q):
    return sum(p[i] * logsumexp(p[i] / max(0.000000001,q[i])) for i in range(len(p))) # edited because of 0 division error to logsumexp
######## Jenson Shannon Divergence ########
def js_divergence(p, q):
    m = 0.5 * (p + q)
    return 0.5 * kl_divergence(p, m) + 0.5 * kl_divergence(q, m)

######## Probability Distritbutions are calculated with the help of KDE ########
######## Warning: Currently is gaussian kernel KDE implemented,         ########
######## This will change according to new notation from overleaf       ########

def main(data_path):
    with open(data_path, 'r') as file:
        data = json.loads(file.read())  # use `json.loads` to do the reverse

        high_scores = []

        # General PDF #
        # Sample and calculate
        sample_size = len(data.items())
        #for entity_i, entities_j in data.items():
         #   print(random.sample(entities_j.items(), 1)[0][1][2])
          #  return
            #print(random.sample(entities_j.items(), 1)[0][1])
        general_sample = [random.sample(entities_j.items(), 1)[0][1][2] for entity_i, entities_j in data.items()]
        print(general_sample)
        days = len(general_sample[0])
        print(days)
        general_pdf_absolute_support_days = [0] * days
        for day in range(days):
            for sample in general_sample:
                general_pdf_absolute_support_days[day] = general_pdf_absolute_support_days[day] + sample[day]
        general_pdf_absolute_support_sum = sum(general_pdf_absolute_support_days)
        general_pdf = [day_support / general_pdf_absolute_support_sum for day_support in general_pdf_absolute_support_days]
        print(general_pdf)
        #Rule PDF
        for entity_i, entities_j in data.items():
            for entity_j, histogram in entities_j.items():

                histogram = histogram[2]
                # This part must change because we do not need to use gaussian kde anymore
                # we can find less complex solution for formula computation from overleaf
                #print(entity_j)
                #print(histogram)
                # Calc Rule PDF #
                rule_absolute_support = sum(histogram)
                days_relative_support = []
                for day_support in histogram:
                    days_relative_support.append(day_support/rule_absolute_support)

                #print(np.array(general_pdf))
                #print(np.array(days_relative_support))

                score = js_divergence(np.array(general_pdf), np.array(days_relative_support))
                #.reshape(-1, 1)
                high_scores.append((str(entity_i + " => " + entity_j), days_relative_support, score))

        high_scores = sorted(high_scores, key=itemgetter(2), reverse=True)[:100]
        print(high_scores)
        print(general_pdf)
        with open(os.path.join("/Users/nicolasalder/Downloads/table_changes_agg/histograms", 'highscores.txt'), 'w+') as file:
            file.write(json.dumps(high_scores))  # use `json.loads` to do the reverse
        with open(os.path.join("/Users/nicolasalder/Downloads/table_changes_agg/histograms", 'general_pdf.txt'), 'w+') as file:
            file.write(json.dumps(general_pdf))  # use `json.loads` to do the reverse


if __name__ == '__main__':
    main("/Users/nicolasalder/Downloads/table_changes_agg/histograms/histograms_tables-act.json")