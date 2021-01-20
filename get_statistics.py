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


def make_plots(count_df, granularity, out_path):
    for entity in granularity:
        make_boxplot(count_df, entity, out_path)


def save_stats(df, granularity):
    for change_type in ["update", "add", "delete"]:
        sums = ["sum", change_type]
        avgs = ["avg", change_type]
        for entity in granularity:
            sums.append((df.loc[df["change_type"] == change_type])[entity].sum())
            avgs.append((df.loc[df["change_type"] == change_type])[entity].mean())
        df_stats = pd.DataFrame(
            [sums, avgs],
            columns=[
                "dates",
                "change_type",
                "table",
                "column",
                "row",
                "field",
                "whole_table",
                "whole_column",
                "whole_row",
            ],
        )
        df = df.append(
            df_stats,
            ignore_index=True,
        )
    return df


def load_dataframe(path):
    return pd.read_pickle(os.path.join(path))


def parse_args():
    ap = argparse.ArgumentParser(
        description="Generates plots and statistics from a dataframe which contains "
        "change counts."
    )
    ap.add_argument("file", type=str, help="Path to the pickled dataframe")
    ap.add_argument(
        "--output", type=str, help="Output directory. Default ./plots", default="plots"
    )
    return vars(ap.parse_args())


def main():
    args = parse_args()
    if not os.path.isdir(args["output"]):
        os.makedirs(args["output"])

    granularity = [
        "table",
        "column",
        "row",
        "field",
        "whole_table",
        "whole_column",
        "whole_row",
    ]

    count_df = load_dataframe(args["file"])
    stats = save_stats(count_df, granularity)
    make_plots(count_df, granularity, args["output"])

    pd.to_pickle(stats, os.path.join(args["output"], f"stats_df"))


if __name__ == "__main__":
    main()
