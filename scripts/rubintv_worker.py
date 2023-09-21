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

import yaml
from lsst.rubintv.analysis.service.client import run_worker

default_config = os.path.join(pathlib.Path(__file__).parent.absolute(), "config.yaml")


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

    # Run the client and connect to rubinTV via websockets
    run_worker(args.address, args.port, config)


if __name__ == "__main__":
    main()
