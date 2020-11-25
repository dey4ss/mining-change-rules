import argparse
import os

from util import date_range, file_extension, Entity, Field


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
                tables.add(line)
    return tables


def main():
    args = parse_args()
    changes = dict()
    dates = date_range("2019-11-01", "2020-04-30")

    entity = [entity for entity in list(Entity) if entity.to_str() == args["level"]][0]
    base_tables = [
        filename[: -len(file_extension())] for filename in os.listdir(os.path.join(args["data_dir"], dates[0]))
    ]

    if entity == Entity.Table:
        for table_id in base_tables:
            changes[Field(table_id, None, None)] = 0

    for date in dates:
        daily_changes = set()
        file_names = [f"{date}_delete.csv", f"{date}_add.csv", f"{date}_update.csv"]
        print(date)
        new_tables = get_new_tables(args["change_dir"], date)
        for file_name in file_names:
            if not os.path.isfile(os.path.join(args["change_dir"], file_name)):
                continue

            with open(os.path.join(args["change_dir"], file_name), "r", encoding="utf-8") as f:
                for line in f:
                    field_id = line.split(";")
                    field = Field.get_with_level(entity, field_id[0], field_id[1], field_id[2])
                    if not field_id[0] in new_tables:
                        daily_changes.add(field)
                    else:
                        changes[field] = 0
        for field in daily_changes:
            if not field in changes:
                changes[field] = 0
            changes[field] += 1
    with open(f"{entity}_change_counts.csv", "w") as f:
        f.write(f"{Field.get_csv_header(entity)};num_changes\n")
        for field in changes:
            f.write(f"{field.get_csv(entity)};{changes[field]}\n")


if __name__ == "__main__":
    main()
