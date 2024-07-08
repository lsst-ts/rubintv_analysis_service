# This file is part of lsst_rubintv_analysis_service.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import argparse
import logging
import os
import pathlib

import sqlalchemy
import yaml
from lsst.daf.butler import Butler
from lsst.rubintv.analysis.service.data import DataCenter, DataMatch
from lsst.rubintv.analysis.service.database import ConsDbSchema
from lsst.rubintv.analysis.service.efd import EfdClient
from lsst.rubintv.analysis.service.utils import ServerFormatter
from lsst.rubintv.analysis.service.worker import Worker

default_config = os.path.join(pathlib.Path(__file__).parent.absolute(), "config.yaml")
default_joins = os.path.join(pathlib.Path(__file__).parent.absolute(), "joins.yaml")
logger = logging.getLogger("lsst.rubintv.analysis.server.worker")
sdm_schemas_path = os.path.join(os.path.expandvars("$SDM_SCHEMAS_DIR"), "yml")
credentials_path = os.path.join(os.path.expanduser("~"), ".lsst", "postgres-credentials.txt")


class UniversalToVisit(DataMatch):
    def get_join(self):
        return


def main():
    parser = argparse.ArgumentParser(description="Initialize a new RubinTV worker.")
    parser.add_argument(
        "-a", "--address", default="localhost", type=str, help="Address of the rubinTV web app."
    )
    parser.add_argument(
        "-p", "--port", default=8080, type=int, help="Port of the rubinTV web app websockets."
    )
    parser.add_argument(
        "-c", "--config", default=default_config, type=str, help="Location of the configuration file."
    )
    parser.add_argument("-j", "--joins", default=default_joins, type=str, help="Location of the joins file.")
    parser.add_argument(
        "-l",
        "--location",
        default="usdf",
        type=str,
        help="Location of the worker (either 'summit' or 'usdf')",
    )
    parser.add_argument(
        "--log",
        default="INFO",
        help="Set the logging level of the worker pod modules (DEBUG, INFO, WARNING, ERROR, CRITICAL).",
    )
    parser.add_argument(
        "--log-all",
        default="WARNING",
        help="Set the logging level of the remainder of packages (DEBUG, INFO, WARNING, ERROR, CRITICAL).",
    )
    args = parser.parse_args()

    # Configure logging for all modules
    log_level = getattr(logging, args.log_all.upper(), None)
    if not isinstance(log_level, int):
        raise ValueError(f"Invalid log level: {args.log}")
    logging.basicConfig(level=log_level)

    # Configure logging for the worker pod modules
    worker_log_level = getattr(logging, args.log.upper(), None)
    if not isinstance(worker_log_level, int):
        raise ValueError(f"Invalid log level: {args.log}")

    # Use custom formatting for the server logs
    handler = logging.StreamHandler()
    handler.setFormatter(ServerFormatter())
    for logger_name in [
        "lsst.rubintv.analysis.service.worker",
        "lsst.rubintv.analysis.service.client",
        "lsst.rubintv.analysis.service.server",
    ]:
        logger = logging.getLogger(logger_name)
        logger.setLevel(worker_log_level)
        logger.addHandler(handler)

    # Load the configuration and join files
    logger.info("Loading config")
    with open(args.config, "r") as file:
        config = yaml.safe_load(file)
    with open(args.joins, "r") as file:
        joins = yaml.safe_load(file)["joins"]

    # Set the database URL based on the location
    logger.info("Connecting to the database")
    server = ""
    if args.location.lower() == "summit":
        server = config["locations"]["summit"]
    elif args.location.lower() == "usdf":
        server = config["locations"]["usdf"]
    else:
        raise ValueError(f"Invalid location: {args.location}, must be either 'summit' or 'usdf'")

    with open(credentials_path, "r") as file:
        credentials = file.readlines()
    for credential in credentials:
        _server, _, database, user, password = credential.split(":")
        if _server == server:
            password = password.strip()
            break
    else:
        raise ValueError(f"Could not find credentials for {server}")
    database_url = f"postgresql://{user}:{password}@{server}/{database}"
    engine = sqlalchemy.create_engine(database_url)

    # Initialize the data center that provides access to various data sources
    schemas: dict[str, ConsDbSchema] = {}

    for name, filename in config["schemas"].items():
        full_path = os.path.join(sdm_schemas_path, filename)
        with open(full_path, "r") as file:
            schema = yaml.safe_load(file)
            schemas[name] = ConsDbSchema(schema=schema, engine=engine, join_templates=joins)

    # Load the Butler (if one is available)
    butlers: dict[str, Butler] | None = None
    if "butlers" in config:
        logger.info("Connecting to Butlers")
        for repo in config["butlers"]:
            butlers[repo] = Butler(repo)  # type: ignore

    # Load the EFD client (if one is available)
    efd_client: EfdClient | None = None
    if "efd" in config:
        logger.info("Connecting to EFD")
        raise NotImplementedError("EFD client not yet implemented")

    # Create the DataCenter that keeps track of all data sources.
    # This will have to be updated every time we want to
    # change/add a new data source.
    data_center = DataCenter(schemas=schemas, butlers=butlers, efd_client=efd_client)

    # Run the client and connect to rubinTV via websockets
    logger.info("Initializing worker")
    worker = Worker(args.address, args.port, data_center)
    worker.run()


if __name__ == "__main__":
    main()
