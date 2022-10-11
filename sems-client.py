#!/usr/bin/env python3

import json
from datetime import datetime

import toml
import requests
import jmespath

from influxdb import InfluxDBClient
from rocketry import Rocketry
from loguru import logger

CONFIG_TOML = "config.toml"


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
            self._token = self.getLoginToken(self._username, self._password)
            return self._token is not None
        except Exception as exception:
            logger.exception("SEMS Authentication exception " + exception)
            return False

    def getLoginToken(self, userName, password):
        """Get the login token for the SEMS API"""
        try:
            # Get our Authentication Token from SEMS Portal API
            logger.debug("SEMS - Getting API token")

            # Prepare Login Data to retrieve Authentication Token
            # Dict won't work here somehow, so this magic string creation must do.
            login_data = '{"account":"' + userName + '","pwd":"' + password + '"}'
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
                logger.info(
                    "SEMS - Maximum token fetch tries reached, aborting for now"
                )
                raise OutOfRetries
            if self._token is None or renewToken:
                logger.debug("API token not set or new token requested, fetching")
                self._token = self.getLoginToken(self._username, self._password)

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
    def __init__(self, config_toml):
        with open(config_toml, "rt") as f:
            self.config = toml.load(f)

        self.sems = SemsApi(
            self.config["sems"]["username"],
            self.config["sems"]["password"],
            self.config["sems"]["plant_id"],
        )

        self.influx = InfluxDBClient(host=self.config['influxdb']['host'], port=self.config['influxdb']['port'])
        self.influx.create_database(self.config['influxdb']['database'])
        self.influx.switch_database(self.config['influxdb']['database'])
        logger.info(f"Connected to InfluxDB {self.influx.ping()} at {self.config['influxdb']['host']}:{self.config['influxdb']['port']}")


    def run(self):
        app = Rocketry(execution="thread")
        app.task(
            f"every {self.config['sems']['interval']} seconds",
            func=self.data_task,
        )
        app.run()

    def parse_data(self, sems_data):
        out_data = {}

        ## Parse all required values
        for key in self.config['values']:
            out_data[key] = jmespath.search(self.config["values"][key], sems_data)

        ## Parse the timestamp
        info_time = jmespath.search('info.time', sems_data)
        # The time in 'info.time' is apparently in our local timezone
        timestamp = datetime.strptime(info_time, '%m/%d/%Y %H:%M:%S').timestamp()

        return int(timestamp), out_data

    def data_task(self):
        try:
            sems_data = self.sems.getData()
            timestamp, out_data = self.parse_data(sems_data)

            print(json.dumps(out_data, indent=2))
            influx_data = [{
                "measurement": self.config['influxdb']['measurement'],
                "time": timestamp,
                "fields": out_data,
            }]
            print(influx_data)
            self.influx.write_points(influx_data)
        except Exception as ex:
            logger.exception(ex)

if __name__ == "__main__":
   sems_processor = SemsProcessor(CONFIG_TOML)
   sems_processor.run()
