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

from websocket import WebSocketApp

from .command import execute_command
from .command import DataCenter
from .utils import Colors, printc

logger = logging.getLogger("lsst.rubintv.analysis.service.client")


class Worker:
    _address: str
    _port: int
    _dataCenter: DataCenter

    def __init__(self, address: str, port: int, dataCenter: DataCenter):
        self._address = address
        self._port = port
        self._dataCenter = dataCenter

    @property
    def dataCenter(self) -> DataCenter:
        return self._dataCenter

    def on_error(self, ws: WebSocketApp, error: str) -> None:
        """Error received from the server."""
        printc(f"Error: {error}", color=Colors.BRIGHT_RED)

    def on_close(self, ws: WebSocketApp, close_status_code: str, close_msg: str) -> None:
        """Connection closed by the server."""
        printc("Connection closed", Colors.BRIGHT_YELLOW)

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
            response = execute_command(message, self.dataCenter)
            ws.send(response)

        printc(f"Connecting to rubinTV at {self._address}:{self._port}", Colors.BRIGHT_GREEN)
        # Connect to the WebSocket server
        ws = WebSocketApp(
            f"ws://{self._address}:{self._port}/ws/worker",
            on_message=on_message,
            on_error=self.on_error,
            on_close=self.on_close,
        )
        ws.run_forever()
        ws.close()
