#!/usr/bin/env python
"""
Performs basic cleaning on the data and saves the results in Weights & Biases
"""
import argparse
import logging
import wandb

import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    logger.info("Downloading artifact: %s", args.input_artifact)
    artifact_local_path = run.use_artifact(args.input_artifact).file()

    df = pd.read_csv(artifact_local_path)

    # Get min and max price
    min_price = args.min_price
    max_price = args.max_price

    # Remove outliers
    logger.info("Removing outliers")
    idx = df['price'].between(min_price, max_price)
    df = df[idx].copy()
    # Convert last_review to datetime
    df['last_review'] = pd.to_datetime(df['last_review'])

    # Drop rows that are not in proper geolocation
    idx = df['longitude'].between(-74.25, -73.50) & df['latitude'].between(40.5, 41.2)
    df = df[idx].copy()

    # Save clean dataset
    df.to_csv('clean.csv', index = False)
    artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description
    )

    # Upload to wandb
    logger.info("Uploading artifact")
    artifact.add_file('clean.csv')
    run.log_artifact(artifact)

    artifact.wait()
    logger.info("Artifact uploaded")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="This steps cleans the data")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="Name of input artifact",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="Name of output artifact",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="Type of output artifact",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="Name of output description",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="Min price",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="Max price",
        required=True
    )


    args = parser.parse_args()

    go(args)
