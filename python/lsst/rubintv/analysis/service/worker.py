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

# Path of the worker endpoint on the rubinTV web app. The v2 app serves this
# unprefixed at ``/ws/worker``; the v3 app moved it under its own path prefix,
# to ``/rubintv/internal/ddv/worker``.
#
# The default is the v2 path because a single deploy branch is shared by pods
# talking to both versions, so the default must be the one that leaves an
# un-migrated pod working. Pods pointed at a v3 web app set ``--path``
# explicitly in their deployment config.
DEFAULT_WS_PATH = "/ws/worker"
V3_WS_PATH = "/rubintv/internal/ddv/worker"


class WorkerConnectionError(RuntimeError):
    """The worker could not establish a usable connection to rubinTV.

    Raised for failures that will not resolve by retrying, so that the
    process can exit non-zero instead of looking like a clean shutdown.
    """


class Worker:
    """A worker that connects to the rubinTV server and executes commands.

    Attributes
    ----------
    _address :
        Address of the rubinTV web app websockets.
    _port :
        Port of the rubinTV web app websockets.
    _path :
        Path of the rubinTV worker websocket endpoint.
    _dataCenter :
        Data center for the worker.
    """

    _address: str
    _port: int
    _path: str
    _data_center: DataCenter
    _error: Exception | None

    def __init__(self, address: str, port: int, data_center: DataCenter, path: str = DEFAULT_WS_PATH):
        self._address = address
        self._port = port
        self._path = path if path.startswith("/") else f"/{path}"
        self._data_center = data_center
        self._error = None

    @property
    def data_center(self) -> DataCenter:
        return self._data_center

    def on_error(self, ws: WebSocketApp, error: Exception) -> None:
        """Error received from the server.

        ``run_forever`` reports errors here and then returns normally, so the
        error is recorded for ``run`` to act on once it has.
        """
        logger.error(f"Error: {error}")
        self._error = error

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

        url = f"ws://{self._address}:{self._port}{self._path}"
        logger.connection(f"Connecting to rubinTV at {url}")

        # Connect to the WebSocket server
        self._error = None
        ws = WebSocketApp(
            url,
            on_message=on_message,
            on_error=self.on_error,
            on_close=self.on_close,
        )
        ws.run_forever()
        ws.close()

        # run_forever returns rather than raising when it cannot connect, so
        # without this a worker that never reached the server would exit 0 and
        # be indistinguishable from a clean shutdown. Under a Kubernetes
        # restartPolicy that means a silent restart loop instead of a visible
        # failure.
        if self._error is not None:
            raise WorkerConnectionError(f"Connection to {url} failed: {self._error}") from self._error
