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
import os
import pathlib
from lsst.rubintv.analysis.service.command import DataCenter
from lsst.rubintv.analysis.service.database import DatabaseConnection
import yaml

import sqlalchemy

from lsst.daf.butler import Butler
from lsst.rubintv.analysis.service.data import DataMatch
from lsst.rubintv.analysis.service.efd import EfdClient
from lsst.rubintv.analysis.service.worker import Worker

default_config = os.path.join(pathlib.Path(__file__).parent.absolute(), "config.yaml")


class UniversalToVisit(DataMatch):
    def get_join(self):
        return


def main():
    parser = argparse.ArgumentParser(description="Initialize a new RubinTV worker.")
    parser.add_argument(
        "-a", "--address", default="localhost", type=str, help="Address of the rubinTV web app."
    )
    parser.add_argument(
        "-p", "--port", default=2000, type=int, help="Port of the rubinTV web app websockets."
    )
    parser.add_argument(
        "-c", "--config", default=default_config, type=str, help="Location of the configuration file."
    )
    args = parser.parse_args()

    # Load the configuration file
    with open(args.config, "r") as file:
        config = yaml.safe_load(file)

    # Initialize the data center that provides access to various data sources
    databases: dict[str, DatabaseConnection] = {}

    for name, info in config["databases"].items():
        with open(info["schema"], "r") as file:
            engine = sqlalchemy.create_engine(info["url"])
            schema = yaml.safe_load(file)
            databases[name] = DatabaseConnection(schema=schema, engine=engine)

    # Load the Butler (if one is available)
    butler: Butler | None = None
    if "butler" in config:
        repo = config["butler"].pop("repo")
        butler = Butler(repo, **config["butler"])

    # Load the EFD client (if one is available)
    efd_client: EfdClient | None = None
    if "efd" in config:
        raise NotImplementedError("EFD client not yet implemented")

    # Create the DataCenter that keeps track of all data sources.
    # This will have to be updated every time we want to
    # change/add a new data source.
    dataCenter = DataCenter(databases=databases, butler=butler, efd_client=efd_client)

    # Run the client and connect to rubinTV via websockets
    worker = Worker(args.address, args.port, dataCenter)
    worker.run()


if __name__ == "__main__":
    main()
