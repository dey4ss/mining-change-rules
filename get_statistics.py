# load dataframe from counting and produce nice plots

import argparse
import matplotlib.pyplot as plt
import os
import seaborn as sns
import pandas as pd


def make_boxplot(df, entity, out_path):
    sns.set()
    sns.set_theme(style="whitegrid")
    plt.xlabel("Change Type")
    plt.ylabel("Changes")
    plt.title(f"Amount of changes in {entity}s")
    plt.tight_layout()
    sns.boxplot(x=df["change_type"], y=df[entity], width=0.3)
    plt.savefig(os.path.join(out_path, f"Boxplot_{entity}"))
    plt.close()


def collect_statistic(df, alphabet):
    # return overall and average amount of changes for every granularity

    pass


def calculate_alphabet_size(df):
    # calculates the number of possible events for a given granularity
    pass


def make_plots(in_path, out_path):
    count_df = load_dataframe(in_path)
    print(count_df)
    granularity = ["table", "column", "row", "field"]  # add "whole_table", "whole_column", "whole_row"
    for entity in granularity:
        make_boxplot(count_df, entity, out_path)
    # alphabet = calculate_alphabet_size(count_df)
    # collect_statistic(count_df, alphabet)


def load_dataframe(path):
    return pd.read_pickle(os.path.join(path))


def parse_args():
    ap = argparse.ArgumentParser(
        description="Generates plots and statistics from a dataframe which contains " "change counts."
    )
    ap.add_argument("file", type=str, help="Path to the pickled dataframe")
    ap.add_argument("--output", type=str, help="Output directory. Default ./plots", default="plots")
    return vars(ap.parse_args())


def main():
    args = parse_args()
    if not os.path.isdir(args["output"]):
        os.makedirs(args["output"])
    make_plots(args["file"], args["output"])


if __name__ == "__main__":
    main()
