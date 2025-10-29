#!/usr/bin/env python3
import argparse
import logging
import os
import time
import shutil
import glob

from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, rand, col, count


# ---------------- Spark Session Builders ----------------
def build_spark_cloud(master_url: str, app_name: str = "Log_File_Cluster") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master(master_url)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.driver.maxResultSize", "2g")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        # S3A / Hadoop
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.maximum", "200")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.speculation", "false")
        .getOrCreate()
    )


def build_spark_local(app_name: str = "Log_File_Cluster"):
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.driver.maxResultSize", "2g")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )


# ---------------- Output Helpers ----------------
def flatten_s3_output(spark: SparkSession, s3_temp_dir: str, final_s3_path: str, log: logging.Logger):
    """
    Flatten a Spark S3 output folder to a single .csv file by renaming part-*.csv to a top-level file.
    """
    log.info(f"Flattening S3 output from {s3_temp_dir} to {final_s3_path}")
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    path = spark._jvm.org.apache.hadoop.fs.Path(s3_temp_dir)
    files = fs.listStatus(path)
    for file in files:
        name = file.getPath().getName()
        if name.startswith("part-") and name.endswith(".csv"):
            src = file.getPath()
            dst = spark._jvm.org.apache.hadoop.fs.Path(final_s3_path)
            fs.rename(src, dst)
            fs.delete(path, True)
            log.info(f"Flattened S3 output to {final_s3_path}")
            return
    log.warning(f"No part-*.csv found to flatten in {s3_temp_dir}")


def write_df(df, outdir: str, name: str, spark: SparkSession, log: logging.Logger):
    """
    Writes a DataFrame to local or S3, flattening automatically.
    """
    if outdir.startswith("s3a://"):
        temp_dir = os.path.join(outdir, f"{name}_temp")
        final_csv = os.path.join(outdir, f"{name}.csv")
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_dir)
        flatten_s3_output(spark, temp_dir, final_csv, log)
        return final_csv
    else:
        os.makedirs(outdir, exist_ok=True)
        temp_dir = os.path.join(outdir, f"{name}_temp")
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_dir)
        for file in os.listdir(temp_dir):
            if file.startswith("part-") and file.endswith(".csv"):
                shutil.move(os.path.join(temp_dir, file), os.path.join(outdir, f"{name}.csv"))
                break
        shutil.rmtree(temp_dir, ignore_errors=True)
        return os.path.join(outdir, f"{name}.csv")


def write_summary_file(text: str, outdir: str, filename: str, spark: SparkSession, log: logging.Logger):
    """
    Writes summary text locally or to S3.
    """
    if outdir.startswith("s3a://"):
        temp_dir = os.path.join(outdir, f"{filename}_temp")
        final_path = os.path.join(outdir, filename)
        df = spark.createDataFrame([(text,)], ["content"])
        df.coalesce(1).write.mode("overwrite").text(temp_dir)
        flatten_s3_output(spark, temp_dir, final_path, log)
        return final_path
    else:
        path = os.path.join(outdir, filename)
        with open(path, "w") as f:
            f.write(text)
        return path


# ---------------- Main ----------------
def main():
    ap = argparse.ArgumentParser(description="Log Level Analysis (S3A-compatible).")
    ap.add_argument("master_url", help="Spark master URL, e.g. spark://10.0.0.5:7077")
    ap.add_argument("--input", required=True, help="Input path (local or S3A)")
    ap.add_argument("--outdir", required=True, help="Output path (local or S3A)")
    ap.add_argument("--location", required=True, help="Execution environment: local or cloud")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s,p%(process)s,{%(filename)s:%(lineno)d},%(levelname)s,%(message)s",
    )
    log = logging.getLogger("log_file_cluster")

    t0 = time.time()
    spark = build_spark_cloud(args.master_url) if args.location == "cloud" else build_spark_local()
    print("✅ Spark session created successfully!")

    input_path = args.input
    if not input_path.endswith(".log"):
        if not input_path.endswith("/"):
            input_path += "/"
        input_path += "*/*.log"

    log.info(f"Reading from: {input_path}")
    df = spark.read.text(input_path)
    total_lines = df.count()
    log.info(f"Total lines read: {total_lines:,}")

    # Extract log levels
    level_pattern = r"\b(INFO|WARN|ERROR|DEBUG)\b"
    df_levels = df.withColumn("log_level", regexp_extract(col("value"), level_pattern, 1)).persist()

    lines_with_levels = df_levels.filter(col("log_level") != "").count()
    unique_levels = [row["log_level"] for row in df_levels.select("log_level").distinct().collect()]

    # Log level counts
    counts_df = (
        df_levels.groupBy("log_level")
        .agg(count("*").alias("count"))
        .orderBy(col("count").desc())
    )
    write_df(counts_df, args.outdir, "problem1_counts", spark, log)

    # 10-sample
    sample_df = (
        df_levels.filter(col("log_level") != "")
        .orderBy(rand())
        .limit(10)
        .select(col("value").alias("log_entry"), col("log_level"))
    )
    write_df(sample_df, args.outdir, "problem1_sample", spark, log)

    # Summary text
    counts = counts_df.collect()
    total_with_levels = sum(row["count"] for row in counts)
    pct = lambda c: (c / total_with_levels) * 100 if total_with_levels > 0 else 0
    summary_lines = [
        f"Total log lines processed: {total_lines:,}",
        f"Total lines with log levels: {lines_with_levels:,}",
        f"Unique log levels found: {len(unique_levels)}",
        "",
        "Log level distribution:",
    ]
    for row in counts:
        summary_lines.append(f"  {row['log_level']:<6}: {row['count']:>10,} ({pct(row['count']):6.2f}%)")

    summary_text = "\n".join(summary_lines)
    write_summary_file(summary_text, args.outdir, "problem1_summary.txt", spark, log)

    log.info(f"✅ Completed in {time.time() - t0:.2f}s")
    spark.stop()


if __name__ == "__main__":
    main()
