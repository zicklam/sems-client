#!/usr/bin/env python3

# GoodWe SEMS Portal client
# Used to download Solar power plant performance data
# and store the selected fields into InfluxDB

import os
import sys
import json
import time
import argparse
from datetime import datetime

import requests
import jmespath

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

from rocketry import Rocketry
from loguru import logger

from dynaconf import Dynaconf

config = Dynaconf(
    envvar_prefix="CONFIG",
    settings_files=["config.toml"],
)


class SemsApi:
    """Interface to the SEMS API."""

    _LoginURL = "https://www.semsportal.com/api/v2/Common/CrossLogin"
    _PowerStationURLPart = "/v2/PowerStation/GetMonitorDetailByPowerstationId"
    _RequestTimeout = 30  # seconds

    _DefaultHeaders = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "token": '{"version":"","client":"ios","language":"en"}',
    }

    def __init__(self, username, password, plant_id):
        """Init dummy hub."""
        self._username = username
        self._password = password
        self._plant_id = plant_id
        self._token = None

    def test_authentication(self) -> bool:
        """Test if we can authenticate with the host."""
        try:
            self._token = self.getLoginToken()
            return self._token is not None
        except Exception as exception:
            logger.exception("SEMS Authentication exception " + exception)
            return False

    def login(self):
        logger.debug("Login to SEMS portal")
        self._token = self.getLoginToken()
        logger.success(f"Logged into GoodWe SEMS Portal {self._token['api']} as {self._username}")

    def getLoginToken(self):
        """Get the login token for the SEMS API"""
        try:
            # Get our Authentication Token from SEMS Portal API
            logger.debug("SEMS - Getting API token")

            # Prepare Login Data to retrieve Authentication Token
            # Dict won't work here somehow, so this magic string creation must do.
            login_data = '{"account":"' + self._username + '","pwd":"' + self._password + '"}'
            # login_data = {"account": userName, "pwd": password}

            # Make POST request to retrieve Authentication Token from SEMS API
            login_response = requests.post(
                self._LoginURL,
                headers=self._DefaultHeaders,
                data=login_data,
                timeout=self._RequestTimeout,
            )
            logger.debug(f"Login Response: {login_response.text}")

            login_response.raise_for_status()

            # Process response as JSON
            jsonResponse = login_response.json()
            # Get all the details from our response, needed to make the next POST request (the one that really fetches the data)
            # Also store the api url send with the authentication request for later use
            tokenDict = jsonResponse["data"]
            tokenDict["api"] = jsonResponse["api"]

            logger.debug(f"SEMS - API Token received: {tokenDict}")
            return tokenDict

        except Exception as exception:
            logger.exception(f"Unable to fetch login token from SEMS API. {exception}")
            return None

    def getData(self, powerStationId=None, renewToken=False, maxTokenRetries=2):
        """Get the latest data from the SEMS API and updates the state."""
        try:
            # Get the status of our SEMS Power Station
            logger.debug("SEMS - Making Power Station Status API Call")
            if maxTokenRetries <= 0:
                logger.warning(
                    "SEMS - Maximum token fetch tries reached, aborting for now"
                )
                raise OutOfRetries
            if self._token is None or renewToken:
                logger.debug("API token not set or new token requested, fetching")
                self.login()

            # Prepare Power Station status Headers
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "token": json.dumps(self._token),
            }

            powerStationURL = self._token["api"] + self._PowerStationURLPart
            if powerStationId is None:
                powerStationId = self._plant_id
            logger.debug(
                f"Querying SEMS API {powerStationURL} for power station id {powerStationId}"
            )

            data = '{"powerStationId":"' + powerStationId + '"}'

            response = requests.post(
                powerStationURL,
                headers=headers,
                data=data,
                timeout=self._RequestTimeout,
            )
            jsonResponse = response.json()
            # try again and renew token is unsuccessful
            if jsonResponse["msg"] != "success" or jsonResponse["data"] is None:
                logger.debug(f"Query not successful: {jsonResponse['msg']}")
                logger.debug(
                    f"Retrying with new token, {maxTokenRetries} retries remaining"
                )
                return self.getData(
                    powerStationId, renewToken=True, maxTokenRetries=maxTokenRetries - 1
                )

            return jsonResponse["data"]

        except Exception as exception:
            logger.exception(f"Unable to fetch data from SEMS: {exception}")


class OutOfRetries(Exception):
    """Error to indicate too many error attempts."""


class SemsProcessor:
    def __init__(self, config):
        self.config = config
        self.sems = SemsApi(
            self.config.sems.username,
            self.config.sems.password,
            self.config.sems.plant_id,
        )

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

    def run(self):
        app = Rocketry(execution="thread")
        app.task(
            f"every {self.config.sems.period} seconds",
            func=self.data_task,
        )
        app.run()

    def parse_data(self, sems_data):
        out_data = {}

        ## Parse all required values
        for key in self.config["values"]:
            value = self.config["values"][key]
            out_data[key] = jmespath.search(value, sems_data)

            # Powerflow has a specific format, e.g. "3550(W)",
            # and the power flow direction is in {...}Status field
            if value.startswith("powerflow") and out_data[key].endswith("(W)"):
                flow_direction = jmespath.search(value + "Status", sems_data)
                # Flow direction seems the other way around, fix it :)
                flow_direction *= -1
                out_data[key] = float(out_data[key][:-3]) * flow_direction

        ## Parse the timestamp
        info_time = jmespath.search("info.time", sems_data)
        # The time in 'info.time' is apparently in our local timezone
        timestamp = datetime.strptime(info_time, "%m/%d/%Y %H:%M:%S").timestamp()

        return int(timestamp), out_data

    def save_json(self, sems_data):
        if config.save_json_dir:
            today = datetime.now().strftime("%Y-%m-%d")
            filename = f"{config.save_json_dir}/{today}.jsonl"
            with open(filename, "at+") as f:
                print(json.dumps(sems_data), file=f)    # To ensure a newline at the end
            logger.info(f"Updated: {filename}")

    def data_task(self):
        try:
            sems_data = self.sems.getData()
            self.save_json(sems_data)
            timestamp, out_data = self.parse_data(sems_data)
            logger.info(f"{timestamp} {out_data}")

            # Write to InfluxDBv2
            point = Point(self.config.influxdb.measurement).time(timestamp, WritePrecision.S)
            for key in out_data:
                point.field(key, out_data[key])
            self.influx_writer.write(self.config.influxdb.bucket, self.config.influxdb.organization, point)

        except Exception as ex:
            logger.exception(ex)


def parse_arguments(config):
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", action="store_true", help="Print debug messages")
    parser.add_argument("--save-json-dir", metavar="DIR", help="Save the received JSON files to this directory")

    group_sems = parser.add_argument_group("GoodWe SEMS Portal options")
    group_sems.add_argument("--sems-username", metavar="USERNAME", default=config.sems.username, help="SEMS Portal username (preferably a unique 'visitor' email). Also $CONFIG_SEMS__USERNAME")
    group_sems.add_argument("--sems-password", metavar="PASSWORD", default=config.sems.password, help="SEMS Portal password. Also $CONFIG_SEMS__PASSWORD")
    group_sems.add_argument("--sems-plant-id", metavar="PLANT_ID", default=config.sems.plant_id, help="SEMS Portal Plant ID. Also $CONFIG_SEMS__PLANT_ID")
    group_sems.add_argument("--sems-period", metavar="PERIOD", default=config.sems.period, type=int, help="Query SEMS Portal this often (in seconds). Also $CONFIG_SEMS__PERIOD")

    group_influxdb = parser.add_argument_group("InfluxDB options")
    group_influxdb.add_argument("--influxdb-url", metavar="URL", default=config.influxdb.url or "http://localhost:8086", help="InfluxDB connection URL. Default is 'http://localhost:8086'. Also $CONFIG_INFLUXDB__HOST")
    group_influxdb.add_argument("--influxdb-token", metavar="TOKEN", default=config.influxdb.token, type=str, help="InfluxDB access token. Also $CONFIG_INFLUXDB__TOKEN")
    group_influxdb.add_argument("--influxdb-organization", metavar="ORG", default=config.influxdb.organization, help="InfluxDB organization name. Also $CONFIG_INFLUXDB__ORGANIZATION")
    group_influxdb.add_argument("--influxdb-bucket", metavar="BUCKET", default=config.influxdb.bucket, help="InfluxDB bucket name. Also $CONFIG_INFLUXDB__BUCKET")
    group_influxdb.add_argument("--influxdb-measurement", metavar="MEASUREMENT", default=config.influxdb.measurement, help="InfluxDB measurement name. Also $CONFIG_INFLUXDB__MEASUREMENT")

    args = parser.parse_args()

    # Update 'config' from 'args'
    config.sems.username = args.sems_username
    config.sems.password = args.sems_password
    config.sems.plant_id = args.sems_plant_id
    config.sems.period = args.sems_period

    config.influxdb.url = args.influxdb_url
    config.influxdb.organization = args.influxdb_organization
    config.influxdb.bucket = args.influxdb_bucket
    config.influxdb.token = args.influxdb_token
    config.influxdb.measurement = args.influxdb_measurement

    config.save_json_dir = args.save_json_dir

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

    # Handle --save-json-dir
    if args.save_json_dir:
        if not os.path.isdir(args.save_json_dir):
            parser.error(f"Invalid --save-json-dir parameter: {args.save_json_dir}: Not a directory")

if __name__ == "__main__":
    parse_arguments(config) # Update 'config' inline
    sems_processor = SemsProcessor(config)
    sems_processor.run()
