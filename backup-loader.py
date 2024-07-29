#!/usr/bin/env python3

# Re-load data from backup files back into InfluxDB

import os
import sys
import json
import argparse
from datetime import datetime

from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

from loguru import logger

from dynaconf import Dynaconf

from sems_utils import parse_data, create_point

config = Dynaconf(
    envvar_prefix="CONFIG",
    settings_files=["config.toml"],
)


class BackupLoader:
    def __init__(self, config):
        self.config = config

        if not self.config.dry_run:
            influx_client = InfluxDBClient(
                url=self.config.influxdb.url,
                organization=self.config.influxdb.organization,
                token=self.config.influxdb.token,
            )
            influx_ready = influx_client.ready()
            if influx_ready.status != "ready":
                logger.error(f"Failed to connect to InfluxDB: {self.config.influxdb.url}")
                sys.exit(1)
            logger.success(f"Connected to InfluxDB at {self.config.influxdb.url} (server uptime: {influx_ready.up})")

            self.influx_writer = influx_client.write_api(write_options=SYNCHRONOUS)

    def load_data(self, f):
        n_records = 0
        try:
            for json_data in f:
                sems_data = json.loads(json_data)
                if not sems_data:
                    continue
                timestamp, out_data = parse_data(sems_data)
                logger.info(f"{timestamp} {out_data}")

                n_records += 1

                if self.config.dry_run:
                    continue

                # Write to InfluxDBv2
                point = create_point(self.config.influxdb.measurement, timestamp, out_data)
                self.influx_writer.write(self.config.influxdb.bucket, self.config.influxdb.organization, point)

        except Exception as ex:
            logger.exception(ex)

        return n_records

def parse_arguments(config):
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", action="store_true", help="Print debug messages")
    parser.add_argument("--file", metavar="FILE", help="Input JSON/JSONL file to load. Read from STDIN if not specified.")
    parser.add_argument("--dry-run", action="store_true", default=False, help="Don't write to InfluxDB")

    group_influxdb = parser.add_argument_group("InfluxDB options")
    group_influxdb.add_argument("--influxdb-url", metavar="URL", default=config.influxdb.url or "http://localhost:8086", help="InfluxDB connection URL. Default is 'http://localhost:8086'. Also $CONFIG_INFLUXDB__HOST")
    group_influxdb.add_argument("--influxdb-token", metavar="TOKEN", default=config.influxdb.token, type=str, help="InfluxDB access token. Also $CONFIG_INFLUXDB__TOKEN")
    group_influxdb.add_argument("--influxdb-organization", metavar="ORG", default=config.influxdb.organization, help="InfluxDB organization name. Also $CONFIG_INFLUXDB__ORGANIZATION")
    group_influxdb.add_argument("--influxdb-bucket", metavar="BUCKET", default=config.influxdb.bucket, help="InfluxDB bucket name. Also $CONFIG_INFLUXDB__BUCKET")
    group_influxdb.add_argument("--influxdb-measurement", metavar="MEASUREMENT", default=config.influxdb.measurement, help="InfluxDB measurement name. Also $CONFIG_INFLUXDB__MEASUREMENT")

    args = parser.parse_args()

    config.influxdb.url = args.influxdb_url
    config.influxdb.organization = args.influxdb_organization
    config.influxdb.bucket = args.influxdb_bucket
    config.influxdb.token = args.influxdb_token
    config.influxdb.measurement = args.influxdb_measurement

    config.dry_run = args.dry_run

    # Update logging level (loguru default is DEBUG, ie. don't do anything if --debug)
    if not args.debug:
        logger_format = (
            "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
            "<level>{level: <8}</level> | "
            #"<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
            "<level>{message}</level>"
        )
        logger.remove()
        logger.add(sys.stderr, level="INFO", format=logger_format)
    
    return args


if __name__ == "__main__":
    args = parse_arguments(config) # Update 'config' inline
    backup_loader = BackupLoader(config)

    data_file = sys.stdin
    if args.file and args.file != "-":
        data_file = open(args.file, "rt")

    n_records = backup_loader.load_data(data_file)
    print(f"Loaded {n_records} records")