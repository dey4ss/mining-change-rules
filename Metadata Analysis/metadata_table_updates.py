#!/usr/local/bin/python3

import json
import os


def write_from_json_to_changes_file(json_filepath, changes_file, day):
    metadata_json_file = open(json_filepath, 'r')
    metadata_dict = json.load(metadata_json_file)

    for entry in metadata_dict:
        day = day
        resource = entry["resource"]
        id = resource["id"].encode('utf-8').decode('utf-8')
        name = resource["name"].encode('utf-8').decode('utf-8')
        updatedAt = resource["updatedAt"].encode('utf-8').decode('utf-8')
        createdAt = resource["createdAt"].encode('utf-8').decode('utf-8')
        metadata_updated_at = resource["metadata_updated_at"]
        data_updated_at = resource["data_updated_at"]

        columns_field_name = str([x.encode('UTF8').decode('utf-8') for x in resource["columns_field_name"]]).encode('utf-8').decode('utf-8')
        columns_name = str([x.encode('UTF8').decode('utf-8') for x in resource["columns_name"]]).encode('utf-8').decode('utf-8')

        line = str(str(day) + ";" + str(id) + ";" + str(name) + ";" + str(updatedAt)[:10] + ";" + str(createdAt)[:10] + ";" + str(metadata_updated_at)[:10] + ";" + str(data_updated_at)[:10] + ";" + str(columns_field_name) + ";" + str(columns_name) + "\n")
    changes_file.write(line)

def read_metadata_directory(change_path,output_file_path ):
    changes_file = open(os.path.join(output_file_path, "metadata_table_updates.txt"), "a")
    changes_file.write("day; id; name; updatedAt; createdAt; metadata_updated_at; data_updated_at; columns_field_name; columns_name\n")
    for currentpath, folders, files in os.walk(change_path):
        for file in files:
            #make sure to read json files only, otherwise ignore
            if ".json" not in file:
                continue
            # Read file and add contents
            filename = file
            day = currentpath.split("/")[-1]
            json_filepath = os.path.join(currentpath, filename)

            # Write to file
            write_from_json_to_changes_file(json_filepath, changes_file, day)

    changes_file.close()
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # Read all metadata files recursively with corresponding days associated in a dictionary
    #read_metadata_directory("/Users/nicolasalder/Dropbox/Uni/Master/WS2020-2021/Masterprojekt/socrata/metadata", "/Users/nicolasalder/Dropbox/Uni/Master/WS2020-2021/Masterprojekt/socrata/")
    read_metadata_directory("/san2/data/change-exploration/socrata/metadata", "/home/nicolas.alder/Masterprojekt")
    # Open for each day the corresponding files and read each resource object
    # for each resource: save day,id (table id), updatedAt, createdAt, metadata_updated_at, data_updated_at, columns_field_name, columns_name and SAVE linewise to file



# See PyCharm help at https://www.jetbrains.com/help/pycharm/
