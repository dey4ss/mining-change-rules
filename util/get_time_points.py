import argparse
import json
import os


def parse_args():
    ap = argparse.ArgumentParser(description="Gets all dates in change directory, saves them as JSON")
    ap.add_argument("change_dir", type=str, help="Change directory path")
    ap.add_argument("output", type=str, help="Output file path")
    return vars(ap.parse_args())


def main(change_dir, output):
    actual_days = sorted({file_name[:10] for file_name in os.listdir(change_dir) if file_name.startswith("20")})
    with open(output, "w") as f:
        json.dump(actual_days, f)


if __name__ == "__main__":
    args = parse_args()
    main(args["change_dir"], args["output"])
