#!/usr/bin/python3

import argparse
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + f"{os.sep}..")
from util.util import date_range, file_extension, Entity, Field


def parse_args():
    ap = argparse.ArgumentParser(description="Counts entity changes")
    ap.add_argument("change_dir", type=str, help="Directory of the change files.")
    ap.add_argument("data_dir", type=str, help="Directory of the original files.")
    ap.add_argument("level", type=str, help="Entity to aggregate.", choices=Entity.string_representations())
    return vars(ap.parse_args())


def get_new_tables(path, date):
    file_name = os.path.join(path, f"{date}_table_add.csv")
    if not os.path.isfile(file_name):
        return set()
    tables = set()
    with open(file_name, "r") as f:
        for line in f:
            if line:
                table_id = line.strip()
                if ";" in line:
                    table_id = "".join([character for character in line if not character == ";"])
                tables.add(Field(table_id, None, None))
    return tables


def get_added_deleted_fields(keyword, path, date):
    result = set()
    file_name = os.path.join(path, f"{date}_{keyword}.csv")
    if not os.path.isfile(file_name):
        return set()
    with open(file_name, "r") as f:
        for line in f:
            line_split = line.strip().split(";")
            item = Field(line_split[0], line_split[1], line_split[2])
            result.add(item)
    return result


def get_added_deleted_rows_columns(entity, path, date):
    adds = set()
    deletes = set()
    file_name = os.path.join(path, f"{date}_{entity.to_str()}_add_delete.csv")
    if not os.path.isfile(file_name):
        return set(), set()
    with open(file_name, "r") as f:
        for line in f:
            line_split = line.strip().split(";")
            keyword = line_split[0]
            table_id = line_split[1]
            item_id = line_split[2]
            item = Field(table_id, item_id, None) if entity == Entity.Column else Field(table_id, None, item_id)
            if keyword == "add":
                adds.add(item)
            else:
                deletes.add(item)
    return adds, deletes


def get_added_deleted_entities(entity, path, date):
    if entity == Entity.Field:
        return get_added_deleted_fields("add", path, date), get_added_deleted_fields("delete", path, date)
    if entity == Entity.Table:
        return get_new_tables(path, date), set()
    return get_added_deleted_rows_columns(entity, path, date)


def main():
    args = parse_args()
    changes = dict()
    dates = date_range("2019-11-02", "2020-04-30")

    entity = [entity for entity in list(Entity) if entity.to_str() == args["level"]][0]

    if entity == Entity.Table:
        base_tables = [
            filename[: -len(file_extension())]
            for filename in os.listdir(os.path.join(args["data_dir"], "2019-11-01"))
            if filename.endswith(file_extension())
        ]
        for table_id in base_tables:
            changes[Field(table_id, None, None)] = [0, 0, 0]

    for date in dates:
        daily_updates = set()
        file_names = [f"{date}_update.csv"]
        if not entity == Entity.Field:
            file_names += [f"{date}_delete.csv", f"{date}_add.csv"]
        print(date)
        new_entities, deleted_entities = get_added_deleted_entities(entity, args["change_dir"], date)
        for file_name in file_names:
            if not os.path.isfile(os.path.join(args["change_dir"], file_name)):
                continue

            with open(os.path.join(args["change_dir"], file_name), "r", encoding="utf-8") as f:
                for line in f:
                    field_id = line.strip().split(";")
                    field = Field.get_with_level(entity, field_id[0], field_id[1], field_id[2])
                    if not (field in new_entities or field in deleted_entities):
                        daily_updates.add(field)
        all_daily_changes = [daily_updates, new_entities, deleted_entities]
        for offset in range(len(all_daily_changes)):
            daily_changes = all_daily_changes[offset]
            for field in daily_changes:
                if not field in changes:
                    changes[field] = [0, 0, 0]
                changes[field][offset] += 1
    with open(f"{entity.to_str()}_change_counts.csv", "w") as f:
        f.write(f"{Field.get_csv_header(entity)};updates;adds;deletes\n")
        for field in changes:
            field_changes = [str(count) for count in changes[field]]
            f.write(f"{field.get_csv(entity)};{';'.join(field_changes)}\n")


if __name__ == "__main__":
    main()
