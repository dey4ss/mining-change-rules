import argparse
import json
from pympler import asizeof


def parse_args():
    ap = argparse.ArgumentParser(description="Loads a JSON serialized Python object and prints its memory consumption.")
    ap.add_argument("file", type=str, help="File path")
    return vars(ap.parse_args())


def main():
    args = parse_args()
    with open(args["file"]) as f:
        data_structure = json.load(f)

    data_type = type(data_structure)
    print(f"Data type: {data_type}", end="")
    print(f" ({len(data_structure)} entries)" if data_type in [dict, list] else "")

    structure_size = asizeof.asizeof(data_structure)
    print(f"Size: {structure_size} B")
    for div, units in {1000: ["kB", "MB", "GB"], 1024: ["kiB", "MiB", "GiB"]}.items():
        print(
            "",
            " / ".join(
                [f"{round(structure_size / div ** n, 2)} {unit}" for unit, n in zip(units, range(1, len(units) + 1))]
            ),
            sep="\t",
        )


if __name__ == "__main__":
    main()
