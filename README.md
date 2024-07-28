# GoodWe solar API to InfluxDB bridge

GoodWe solar invertors are a popular choice for residential PV installations,
mainly thanks to their price. Unfortunately the _hackability_ of these invertors
is somewhat sketchy - the API is not documented, and when we manage to retrieve
some information it's full of errors and spelling mistakes (e.g. "bettery"
instead of "battery", or is "bettery" a _better-battery_? who knows...).

## InfluxDB support

This script talks to the GoodWe _SEMS Portal_ API, retrieves the PV stats once a
minute and writes them to InfluxDB. Once there you can integrate it with Home
Assistant, create a Grafana dashboard, or show the key stats on a little
Rasbberry Pi powered display. The world is your oyster.

# Configuration

1. Copy `config.toml.sample` to `config.toml` and open it for editing.
2. Set the `username` and `password` to the values that you have been given
by your solar installer.
3. Log in to https://www.semsportal.com to find the _Plant ID_ - it's the last part
of the dashbord URL:

    ```
    https://www.semsportal.com/powerstation/powerstatussnmin/aabbbccc-0123-4567-9876-a1b2c3d4e5f6
                                                             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    ```

If you already have InfluxDB running fill the details under the `[influxdb]`
header. If not you can use the included `docker-compose.yml` to start one for
you.

## Docker compose

An example `docker-compose.yml` is included in the repository. Since the docker
container will be built from the source it's two-step process:

1. `docker compose build sems-client`
2. `docker compose up -d`

Now you can open the url http://localhost:18086 and login with the credentials
given in the `DOCKER_INFLUXDB_INIT_*` values in the docker compose file.

## Enjoy!

Let me know if you've found this useful. Pull requests with improvements are
welcome :)

Cheers

michael -at- logix.net.nz
