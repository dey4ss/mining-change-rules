#!/usr/bin/python3

import argparse
import queue
import os
import multiprocessing as mp

from util import date_range, file_extension, Entity, Field


def parse_args():
    ap = argparse.ArgumentParser(description="Counts entity changes")
    ap.add_argument("change_dir", type=str, help="Directory of the change files.")
    ap.add_argument(
        "entity", type=str, help="Entity to aggregate.", choices=[e.to_str() for e in Entity if not e == Entity.Field]
    )
    ap.add_argument("--start", type=str, help="Start date. Default 2019-11-02", default="2020-03-01")
    ap.add_argument("--end", type=str, help="End date. Default 2020-11-01", default="2020-11-01")
    ap.add_argument("--threads", type=int, help="Number of threads. Default 2", default=2)
    return vars(ap.parse_args())


def main():
    args = parse_args()
    entity = [entity for entity in list(Entity) if entity.to_str() == args["entity"]][0]

    with mp.Manager() as manager:
        job_queue = manager.Queue()
        dates = date_range(args["start"], args["end"])
        for date in dates:
            job_queue.put(date)

        workers = [
            mp.Process(target=aggregate_daily_changes, args=(job_queue, args["change_dir"], entity, f"{n}".rjust(2)))
            for n in range(args["threads"])
        ]

        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join()


def aggregate_daily_changes(date_queue, path, entity, n):
    print(f"[Start Worker {n}]")
    while True:
        date = None
        try:
            date = date_queue.get_nowait()
        except queue.Empty:
            print(f"[Exit Worker {n}]")
            return

        daily_changes = {"update": set(), "delete": set(), "insert": set()}
        print(f"{date} [Worker {n}]")
        for change_type in daily_changes:
            file_name = os.path.join(path, f"{date}_{change_type}.csv")
            if not os.path.isfile(file_name):
                continue

            with open(file_name, "r", encoding="utf-8") as f:
                for line in f:
                    field_id = line.strip().split(";")
                    field = Field.get_with_level(entity, field_id[0], field_id[1], field_id[2])
                    if not field in daily_changes[change_type]:
                        daily_changes[change_type].add(field)

        with open(os.path.join(path, f"{date}_{entity.to_str()}_changes_aggregated.csv"), "w") as f:
            f.write(f"change_type;{Field.get_csv_header(entity)}\n")
            for change_type in daily_changes:
                for change in daily_changes[change_type]:
                    f.write(f"{change_type};{change.get_csv(entity)}\n")


if __name__ == "__main__":
    mp.set_start_method("spawn")
    main()
