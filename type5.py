import pandas as pd
import os
import json




def generateTablesPDFAgg(changes_directory, entity,z, restrict=[]):
    #entity = "column_changes_aggregated.csv"
    #make sure that list is sorted from earliest to latest date
    change_files = sorted([f for f in os.listdir(changes_directory) if entity in f])
    no_days = len(change_files)
    max_count = no_days-int(z)
    element_hashmap = {}

    for day in change_files:
        print(day)
        #catch empty transactions
        try:
            changes = pd.read_csv(os.path.join(changes_directory, day),sep=";")
        except:
            continue

        #print(changes.head())
        changes.columns = ["change_type", "table"]

        # since we only want to consider the first occurence within z days, we observe already seen values
        already_seen_in_z = {}

        # combine each element i
        for index, change_i in changes.iterrows():

            element_i = str(change_i["change_type"])+str(change_i["table"])
            already_seen_in_z[element_i] = set()

            if (restrict != []) and (element_i not in restrict):
                continue

            # with each distinct element j on the following z days
            current_day = change_files.index(day)


            for following_day in range(0, z + 1):
                #if z is greater than available data, do not include for consistency reasons
                if ((current_day + z + 1) + 1) > len(change_files):
                    break
                changes_following_day_file = change_files[current_day + following_day]
                # catch empty transactions
                try:
                    changes_following_day = pd.read_csv(os.path.join(changes_directory, changes_following_day_file), sep=";")
                except:
                    continue
                changes_following_day.columns =["change_type", "table"]
                for index, change_j in changes_following_day.iterrows():
                    element_j = str(change_j["change_type"])+str(change_j["table"])
                    if (element_i != element_j) and (element_j not in already_seen_in_z[element_i]) :
                        already_seen_in_z[element_i].add(element_j)
                        # print(element_i)
                        # print(element_j)
                        # create element i dictionary if not present
                        histogram_i = element_hashmap.get(element_i, {element_j: [0] * ((z + 1)+1)})
                        # print(element_hashmap)
                        # print(histogram_i)

                        # create element j dictionary if not present
                        histogram_i[element_j] = histogram_i.get(element_j, [0] * ((z + 1)+1))
                        # print(histogram_i)
                        histogram_i[element_j][following_day] = histogram_i[element_j][following_day] + 1
                        # print(histogram_i)
                        element_hashmap[element_i] = histogram_i
                        # print(element_hashmap)

    for entity_i, entities_j in element_hashmap.items():
        for entity_j, histogram_j in entities_j.items():
            histogram_j[-1] = max_count - sum(histogram_j)

    return element_hashmap



def main():

    restrict0 = pd.read_csv("/Users/nicolasalder/Downloads/table_changes_agg/2019-11-02_table_changes_aggregated.csv",
                           sep=";")[:10]

    restrict = pd.concat([restrict0])
    restrict = list(restrict.apply(lambda input: str(input[0]) + str(input[1]), axis=1))
    #print(restrict)

    histograms = generateTablesPDFAgg("/Users/nicolasalder/Downloads/table_changes_agg", "table_changes_aggregated.csv", 10,restrict)
    with open(os.path.join("/Users/nicolasalder/Downloads/table_changes_agg", '1_histograms.txt'), 'w+') as file:
        file.write(json.dumps(histograms))  # use `json.loads` to do the reverse



if __name__ == '__main__':
    main()