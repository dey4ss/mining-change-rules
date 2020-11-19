import os
from pyspark.ml.fpm import FPGrowth
from pyspark.sql import SparkSession

from find_changes import load_change_transactions


def main():
    TRANSACTION_FILE = os.path.join("transactions_", "table_changes")
    transactions = load_change_transactions(TRANSACTION_FILE)

    transaction_tuples = []
    for transaction_id in range(len(transactions)):
        transaction_tuples.append((transaction_id, transactions[transaction_id]))

    spark = SparkSession.builder.master("local").appName("CDCs").getOrCreate()
    df = spark.createDataFrame(transaction_tuples, ["id", "items"])

    fpGrowth = FPGrowth(itemsCol="items", minSupport=0.5, minConfidence=0.6)
    model = fpGrowth.fit(df)

    # Display frequent itemsets.
    model.freqItemsets.show(n=50, truncate=100)


if __name__ == "__main__":
    main()
