#!/usr/local/bin/python3

import zipfile
import os


def make_list(lines):
    return [line.decode("UTF-8").split(".json")[0] for line in lines]


def read_metadata_directory(change_path, output_file_path):
    changes_file = open(os.path.join(output_file_path, "metadata_table_add_delete.txt"), "a")
    changes_file.write("day; add; delete\n")
    meta_base_path = "san2/data/change-exploration/socrata/workingDir/diffs/"

    for currentpath, folders, files in os.walk(change_path):
        for file in files:
            # make sure to read json files only, otherwise ignore
            if ".zip" not in file:
                continue
            # Read files
            print(file)
            archive = zipfile.ZipFile(os.path.join(currentpath, file), "r")
            directory_name = file[:10] + "_diff"
            day = file[:10]
            add_path = os.path.join(meta_base_path, directory_name, "created.meta")
            delete_path = os.path.join(meta_base_path, directory_name, "deleted.meta")

            try:
                add_lines = archive.open(add_path).readlines()
                add_files = make_list(add_lines)
            except:
                add_files = []

            try:
                delete_lines = archive.open(delete_path).readlines()
                delete_files = make_list(delete_lines)
            except:
                delete_files = []

            changes_file.write(str(day) + ";" + str(add_files) + ";" + str(delete_files) + "\n")
            archive.close()

    changes_file.close()


# Press the green button in the gutter to run the script.
if __name__ == "__main__":
    # Read all metadata files recursively with corresponding days associated in a dictionary
    # read_metadata_directory("/Users/nicolasalder/Dropbox/Uni/Master/WS2020-2021/Masterprojekt/socrata/diff", "/Users/nicolasalder/Dropbox/Uni/Master/WS2020-2021/Masterprojekt/socrata/")
    read_metadata_directory("/san2/data/change-exploration/socrata/diff", "/home/nicolas.alder/Masterprojekt")
    # Open for each day the corresponding files and read each resource object
    # for each resource: save day,id (table id), updatedAt, createdAt, metadata_updated_at, data_updated_at, columns_field_name, columns_name and SAVE linewise to file


# See PyCharm help at https://www.jetbrains.com/help/pycharm/
