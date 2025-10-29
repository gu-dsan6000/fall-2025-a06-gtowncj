#!/usr/bin/env python3
import argparse
import logging
import os
import time
import re
from datetime import datetime
import pprint

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from collections import defaultdict
from urllib.parse import urlparse

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import (
    col, regexp_extract, regexp_replace, when,
    try_to_timestamp, min as spark_min, max as spark_max,
    countDistinct, input_file_name, row_number, lpad, col as spark_col,
    sum as spark_sum, avg as spark_avg, desc as spark_desc
)
from pyspark.sql.window import Window


timestamp_pattern = re.compile(r"^\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}")

def parse_timestamp_to_unix(ts_str):
    """Convert 'YY/MM/DD HH:MM:SS' to Unix epoch seconds."""
    try:
        dt = datetime.strptime(ts_str, "%y/%m/%d %H:%M:%S")
        return int(dt.timestamp())
    except Exception:
        return None


def get_latest_timestamp_in_log(filepath, log, spark):
    """
    Reads a file from local or S3A (via Hadoop FS) and returns the latest Unix timestamp.
    """
    max_ts = None
    try:
        sc = spark.sparkContext
        hadoop_conf = sc._jsc.hadoopConfiguration()
        Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
        FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
        URI = sc._gateway.jvm.java.net.URI

        if filepath.startswith("s3a:"):
            # Use Hadoop FS for S3A path
            fs = FileSystem.get(URI(filepath), hadoop_conf)
            path = Path(filepath)

            if not fs.exists(path):
                log.warning(f"File not found: {filepath}")
                return None

            # Open the S3A file as a Java FSDataInputStream
            stream = fs.open(path)
            reader = sc._gateway.jvm.java.io.BufferedReader(
                sc._gateway.jvm.java.io.InputStreamReader(stream, "UTF-8")
            )

            # Iterate through lines
            line = reader.readLine()
            while line:
                match = timestamp_pattern.match(line)
                if match:
                    unix_ts = parse_timestamp_to_unix(match.group(0))
                    if unix_ts is not None:
                        max_ts = unix_ts if max_ts is None else max(max_ts, unix_ts)
                line = reader.readLine()

            reader.close()
            stream.close()

        else:
            # Fallback for local files
            with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    match = timestamp_pattern.match(line)
                    if match:
                        unix_ts = parse_timestamp_to_unix(match.group(0))
                        if unix_ts is not None:
                            max_ts = unix_ts if max_ts is None else max(max_ts, unix_ts)

    except Exception as e:
        log.error(f"Error reading {filepath}: {e}")

    return max_ts * 1000

def extract_max_ts_parallel(item):
    """
    Small wrapper for Spark parallelization.
    Each item = (application_id, full_path, log_file)
    Returns: (application_id, log_file, max_ts)
    """
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    import logging
    log = logging.getLogger("problem2")

    app, full_path, log_file = item
    try:
        max_ts = get_latest_timestamp_in_log(full_path, log, spark)
        return (app, log_file, max_ts)
    except Exception as e:
        log.error(f"Error processing {full_path}: {e}")
        return (app, log_file, None)


# ---------------- Spark Session Builder ----------------
def build_spark(master_url: str, app_name: str = "Cluster_Usage_Analysis") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master(master_url)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.driver.maxResultSize", "2g")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        # S3A setup
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.hadoop.fs.s3a.committer.name", "directory")
        .config("spark.hadoop.fs.s3a.committer.magic.enabled", "true")
        .config("spark.hadoop.fs.s3a.committer.staging.conflict-mode", "replace")
        .getOrCreate()
    )


def convert_app_list_to_cluster_map(input_contents):
    cluster_map = defaultdict(list)

    for name in input_contents:
        # Split on '_' and extract parts
        parts = name.split('_')
        if len(parts) == 3:
            _, cluster_id, app_number = parts
            cluster_map[cluster_id].append(app_number)

    # Convert to normal dict if desired
    return dict(cluster_map)


def dict_to_df(app_log_dict):
    """
    Convert the nested dict of application logs into a pandas DataFrame.
    Example input:
    {
        "application_123_1": [("log1.txt", 1730123456000), ("log2.txt", 1730123470000)],
        "application_123_2": [("log1.txt", 1730123555000)]
    }
    """
    rows = [
        {"application_id": app, "log_file": log_file, "timestamp": ts}
        for app, logs in app_log_dict.items()
        for log_file, ts in logs
    ]
    return pd.DataFrame(rows)


def enrich_app_df(df):
    """Extract cluster_id, app_number, and convert timestamps to datetime."""
    df["cluster_id"] = df["application_id"].str.extract(r"application_(\d+)_\d+")
    df["app_number"] = df["application_id"].str.extract(r"application_\d+_(\d+)")
    df["datetime"] = pd.to_datetime(df["timestamp"] / 1000, unit="s")
    return df


def make_timeline(df):
    """Compute min/max datetime per application."""
    timeline = (
        df.groupby(["cluster_id", "application_id", "app_number"], as_index=False)
          .agg(start_time=("datetime", "min"), end_time=("datetime", "max"))
          .sort_values(["cluster_id", "app_number"])
    )
    return timeline


def make_cluster_summary(timeline_df):
    """Compute per-cluster aggregates."""
    summary = (
        timeline_df.groupby("cluster_id", as_index=False)
                   .agg(
                       num_applications=("application_id", "nunique"),
                       cluster_first_app=("start_time", "min"),
                       cluster_last_app=("end_time", "max")
                   )
                   .sort_values("num_applications", ascending=False)
    )
    return summary


def make_stats(summary_df):
    """Compute small aggregate stats and return a formatted string report."""
    total_clusters = summary_df["cluster_id"].nunique()
    total_apps = summary_df["num_applications"].sum()
    avg_apps = summary_df["num_applications"].mean()

    # Top clusters
    top_clusters = summary_df.nlargest(5, "num_applications")

    report_lines = [
        f"Total unique clusters: {total_clusters}",
        f"Total applications: {total_apps}",
        f"Average applications per cluster: {avg_apps:.2f}",
        "",
        "Most heavily used clusters:"
    ]
    for _, row in top_clusters.iterrows():
        report_lines.append(f"  Cluster {row['cluster_id']}: {row['num_applications']} applications")

    return "\n".join(report_lines)


def process_app_logs_in_memory(app_log_dict):
    """
    End-to-end local computation pipeline.
    Returns: (timeline_df, summary_df, stats_report)
    """
    df = dict_to_df(app_log_dict)
    df = enrich_app_df(df)
    timeline = make_timeline(df)
    summary = make_cluster_summary(timeline)
    stats_report = make_stats(summary)
    return timeline, summary, stats_report


def plot_applications_per_cluster(summary_df, outdir="."):
    """
    Creates and saves a bar chart of the number of applications per cluster.
    """
    plt.figure(figsize=(10, 6))
    sns.barplot(
        data=summary_df,
        x="cluster_id",
        y="num_applications",
        palette="viridis"
    )
    plt.title("Number of Applications per Cluster", fontsize=14, weight="bold")
    plt.xlabel("Cluster ID")
    plt.ylabel("Number of Applications")
    plt.xticks(rotation=45, ha="right")

    # Annotate bar values
    for i, val in enumerate(summary_df["num_applications"]):
        plt.text(i, val + 0.1, f"{val}", ha="center", va="bottom", fontsize=9)

    plt.tight_layout()
    output_path = os.path.join(outdir, "problem2_bar_chart.png")
    plt.savefig(output_path, dpi=300)
    plt.close()
    print(f"✅ Saved bar chart to {output_path}")


def plot_job_duration_density(timeline_df, summary_df, outdir="."):
    """
    Creates and saves a histogram + KDE density plot of job durations for the largest cluster.
    """
    # Calculate durations
    timeline_df["duration_sec"] = (timeline_df["end_time"] - timeline_df["start_time"]).dt.total_seconds()

    # Find cluster with most applications
    top_cluster_id = (
        summary_df.sort_values("num_applications", ascending=False)
        .iloc[0]["cluster_id"]
    )
    top_cluster = timeline_df[timeline_df["cluster_id"] == top_cluster_id]

    plt.figure(figsize=(10, 6))
    sns.histplot(
        data=top_cluster,
        x="duration_sec",
        kde=True,
        bins=30,
        color="steelblue",
        alpha=0.7
    )
    plt.xscale("log")
    plt.xlabel("Job Duration (seconds, log scale)")
    plt.ylabel("Frequency")
    plt.title(f"Job Duration Distribution – Cluster {top_cluster_id} (n={len(top_cluster)})", fontsize=14, weight="bold")
    plt.tight_layout()

    output_path = os.path.join(outdir, "problem2_density_plot.png")
    plt.savefig(output_path, dpi=300)
    plt.close()
    print(f"✅ Saved density plot to {output_path}")



# ---------------- Main ----------------
def main():
    ap = argparse.ArgumentParser(description="Cluster Usage Analysis with Spark and Visualization")
    ap.add_argument("master_url", nargs="?", help="Spark master URL, e.g. spark://10.0.0.5:7077")
    ap.add_argument("--net_id", required=True, help="Net Id for Input File path (S3A or local) containing cluster logs")
    ap.add_argument("--skip-spark", default=False, help="Skip Spark and regenerate visualizations only")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s,p%(process)s,{%(filename)s:%(lineno)d},%(levelname)s,%(message)s",
    )
    log = logging.getLogger("problem2")

    t0 = time.time()

    # Resolve outdir to an absolute path and create it if needed
    # if "s3a:" not in args.outdir:
    #     outdir = os.path.abspath(args.outdir)
    #     os.makedirs(outdir, exist_ok=True)
    # else:
    #     outdir = args.outdir
    
    spark = build_spark(args.master_url)    

    net_id = args.net_id
    input =  f"s3a://{net_id}-assignment-spark-cluster-logs/data/"

    log.info(f"Master URL: {args.master_url}")
    log.info(f"Input (S3A): {input}")
    # log.info(f"Output dir: {outdir}")

    # List all files and folders in the specified directory (args.input)
    if "s3a:" not in input:
        # Local directory: use os.listdir
        if os.path.isdir(input):
            input_contents = os.listdir(input)
            # log.info(f"Files/folders found in local input directory '{args.input}':\n" + "\n".join(sorted(input_contents)))
        else:
            log.warning(f"Input path '{input}' is not a directory.")
    else:
        # S3A path: list files using Spark's Hadoop API
        sc = spark.sparkContext
        hadoop_conf = sc._jsc.hadoopConfiguration()
        Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
        FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
        URI = sc._gateway.jvm.java.net.URI

        try:
            fs = FileSystem.get(URI(input), hadoop_conf)
            path = Path(input)
            if fs.exists(path) and fs.isDirectory(path):
                statuses = fs.listStatus(path)
                input_contents = [status.getPath().getName() for status in statuses]
                # log.info(f"Files/folders found in cloud input S3A directory '{args.input}':\n" + "\n".join(sorted(input_contents)))
            else:
                log.warning(f"S3A path '{input}' is not a directory or does not exist.")
        except Exception as e:
            log.error(f"Unable to list S3A input directory '{input}': {e}")

    # Given input_contents[0] is a filename or directory name inside args.input
    # Let's construct the full S3A path, list its files (if it's a directory), and log them.

    if "s3a:" in input and input_contents:
        sc = spark.sparkContext
        hadoop_conf = sc._jsc.hadoopConfiguration()
        Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
        FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
        URI = sc._gateway.jvm.java.net.URI

        for dir_name in input_contents:
            # Construct the full subdirectory path (assume slash)
            if not input.endswith("/"):
                s3_dir = input + "/" + dir_name
            else:
                s3_dir = input + dir_name
            try:
                fs = FileSystem.get(URI(s3_dir), hadoop_conf)
                sub_path = Path(s3_dir)
                if fs.exists(sub_path) and fs.isDirectory(sub_path):
                    sub_statuses = fs.listStatus(sub_path)
                    sub_files = [status.getPath().getName() for status in sub_statuses if status.isFile()]
                    if 'subdirectory_files_dict' not in locals():
                        subdirectory_files_dict = {}
                    subdirectory_files_dict[dir_name] = sorted(sub_files)
                else:
                    log.warning(f"S3A path '{s3_dir}' is not a directory or does not exist.")
            except Exception as e:
                log.error(f"Unable to list files in S3A subdirectory '{s3_dir}': {e}")

    # cluster_dict = convert_app_list_to_cluster_map(input_contents)
    
    # log.info("\n" + pprint.pformat(subdirectory_files_dict, indent=2) + "\n")

    # Convert dictionary of app -> list of log files into app -> list of (log, max_timestamp)
    app_log_max_timestamps = {}

    # for app, log_files in subdirectory_files_dict.items():
    total_apps = len(subdirectory_files_dict)
    log.info(f"Total number of apps found: {total_apps}")
    processed_apps = 0
    

    for app, log_files in list(subdirectory_files_dict.items()):
        result_list = []
        log.info(f"Application '{app}' has {len(log_files)} log files.")
        processed_logs = 0

        for log_file in log_files:
            full_path = os.path.join(input, app, log_file)

            processed_logs += 1
            if len(log_files) > 0 and processed_logs % max(1, len(log_files) // 25) == 0:
                pct_logs = int((processed_logs / len(log_files)) * 100)
                log.info(f"Application '{app}': processed {pct_logs}% ({processed_logs}/{len(log_files)}) of log files.")

            max_ts = get_latest_timestamp_in_log(full_path, log, spark)
            if max_ts:
                result_list.append((log_file, max_ts))
            # log.warning(f"Skipping timestamp extraction for S3A path: {full_path}")

        app_log_max_timestamps[app] = result_list

        processed_apps += 1
        pct_complete = int((processed_apps / total_apps) * 100)
        log.info(f"Processed {pct_complete}% ({processed_apps}/{total_apps}) of app directories.")
        last_logged_pct = pct_complete
    

    # Replace the old dict for output / logging
    # log.info("\nProcessed app log timestamp summary:\n" + pprint.pformat(app_log_max_timestamps, indent=2))

    timeline_df, summary_df, stats_report = process_app_logs_in_memory(app_log_max_timestamps)

    print("=== TIMELINE ===")
    print(timeline_df)
    print("\n=== SUMMARY ===")
    print(summary_df)
    print("\n=== REPORT ===")
    print(stats_report)

    plot_applications_per_cluster(summary_df, outdir=".")
    plot_job_duration_density(timeline_df, summary_df, outdir=".")

    # Save reports locally in '.' as well as in outdir if needed
    timeline_df.to_csv("./problem2_timeline.csv", index=False)
    summary_df.to_csv("./problem2_cluster_summary.csv", index=False)
    with open("./problem2_stats.txt", "w") as f:
        f.write(stats_report)

    log.info(f"✅ Completed Problem 2 in {time.time() - t0:.2f}s")


if __name__ == "__main__":
    main()
