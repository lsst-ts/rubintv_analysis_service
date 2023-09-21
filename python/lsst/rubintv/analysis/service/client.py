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
import logging

import sqlalchemy
import yaml
from lsst.daf.butler import Butler
from websocket import WebSocketApp

from .command import DatabaseConnection, execute_command

logger = logging.getLogger("lsst.rubintv.analysis.service.client")


def on_error(ws: WebSocketApp, error: str) -> None:
    """Error received from the server."""
    print(f"\033[91mError: {error}\033[0m")


def on_close(ws: WebSocketApp, close_status_code: str, close_msg: str) -> None:
    """Connection closed by the server."""
    print("\033[93mConnection closed\033[0m")


def run_worker(address: str, port: int, connection_info: dict[str, dict]) -> None:
    """Run the worker and connect to the rubinTV server.

    Parameters
    ----------
    address :
        Address of the rubinTV web app.
    port :
        Port of the rubinTV web app websockets.
    connection_info :
        Connections .
    """
    # Load the database connection information
    databases: dict[str, DatabaseConnection] = {}

    for name, info in connection_info["databases"].items():
        with open(info["schema"], "r") as file:
            engine = sqlalchemy.create_engine(info["url"])
            schema = yaml.safe_load(file)
            databases[name] = DatabaseConnection(schema=schema, engine=engine)

    # Load the Butler (if one is available)
    butler: Butler | None = None
    if "butler" in connection_info:
        repo = connection_info["butler"].pop("repo")
        butler = Butler(repo, **connection_info["butler"])

    def on_message(ws: WebSocketApp, message: str) -> None:
        """Message received from the server."""
        response = execute_command(message, databases, butler)
        ws.send(response)

    print(f"\033[92mConnecting to rubinTV at {address}:{port}\033[0m")
    # Connect to the WebSocket server
    ws = WebSocketApp(
        f"ws://{address}:{port}/ws/worker", on_message=on_message, on_error=on_error, on_close=on_close
    )
    ws.run_forever()
    ws.close()
