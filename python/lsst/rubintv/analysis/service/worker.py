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
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from websocket import WebSocketApp

from .command import execute_command

if TYPE_CHECKING:
    from .command import DataCenter

logger = logging.getLogger("lsst.rubintv.analysis.service.client")


class Worker:
    """A worker that connects to the rubinTV server and executes commands.

    Attributes
    ----------
    _address :
        Address of the rubinTV web app websockets.
    _port :
        Port of the rubinTV web app websockets.
    _dataCenter :
        Data center for the worker.
    """

    _address: str
    _port: int
    _data_center: DataCenter

    def __init__(self, address: str, port: int, data_center: DataCenter):
        self._address = address
        self._port = port
        self._data_center = data_center

    @property
    def data_center(self) -> DataCenter:
        return self._data_center

    def on_error(self, ws: WebSocketApp, error: str) -> None:
        """Error received from the server."""
        logger.error(f"Error: {error}")

    def on_close(self, ws: WebSocketApp, close_status_code: str, close_msg: str) -> None:
        """Connection closed by the server."""
        logger.connection("Connection closed")

    def run(self) -> None:
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

        def on_message(ws: WebSocketApp, message: str) -> None:
            """Message received from the server."""
            response = execute_command(message, self.data_center)
            ws.send(response)

        logger.connection(f"Connecting to rubinTV at {self._address}:{self._port}")

        # Connect to the WebSocket server
        ws = WebSocketApp(
            f"ws://{self._address}:{self._port}/ws/worker",
            on_message=on_message,
            on_error=self.on_error,
            on_close=self.on_close,
        )
        ws.run_forever()
        ws.close()
