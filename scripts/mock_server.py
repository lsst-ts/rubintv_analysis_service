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

import argparse
import logging
import uuid
from dataclasses import dataclass
from enum import Enum

import tornado.httpserver
import tornado.ioloop
import tornado.web
import tornado.websocket
from lsst.rubintv.analysis.service.utils import ServerFormatter

logger = logging.getLogger("lsst.rubintv.analysis.service.server")


class WorkerPodStatus(Enum):
    """Status of a worker pod."""

    IDLE = "idle"
    BUSY = "busy"


class WebSocketHandler(tornado.websocket.WebSocketHandler):
    """
    Handler that handles WebSocket connections
    """

    workers: dict[str, WorkerPod] = dict()  # Keep track of connected worker pods
    clients: dict[str, WebSocketHandler] = dict()  # Keep track of connected clients
    queue: list[QueueItem] = list()  # Queue of messages to be processed
    max_message_size: int = 100 * 1024 * 1024  # Maximum message size

    @classmethod
    def urls(cls) -> list[tuple[str, type[tornado.web.RequestHandler], dict[str, str]]]:
        """url to handle websocket connections.

        Websocket URLs should either be followed by 'worker' for worker pods
        or client for clients.
        """
        return [
            (r"/ws/([^/]+)", cls, {}),  # Route/Handler/kwargs
        ]

    def open(self, client_type: str) -> None:
        """
        Client opens a websocket

        Parameters
        ----------
        type :
            The type of client that is connecting.
        """
        self.client_id = str(uuid.uuid4())
        if client_type == "worker":
            WebSocketHandler.workers[self.client_id] = WorkerPod(self.client_id, self)
            logger.worker(
                f"New worker {self.client_id} connected. Total workers: {len(WebSocketHandler.workers)}",
            )
        if client_type == "client":
            WebSocketHandler.clients[self.client_id] = self
            logger.client(
                f"New client {self.client_id} connected. Total clients: {len(WebSocketHandler.clients)}",
            )

    def on_message(self, message: str) -> None:
        """
        Message received from a client or worker.

        Parameters
        ----------
        message :
            The message received from the client or worker.
        """
        if self.client_id in WebSocketHandler.clients:
            logger.client(f"Message received from {self.client_id}")
            client = WebSocketHandler.clients[self.client_id]

            # Find an idle worker
            idle_worker = None
            for worker in WebSocketHandler.workers.values():
                if worker.status == WorkerPodStatus.IDLE:
                    idle_worker = worker
                    break

            if idle_worker is None:
                # No idle worker found, add to queue
                WebSocketHandler.queue.append(QueueItem(message, client))
                return
            idle_worker.process(message, client)
            return

        if self.client_id in WebSocketHandler.workers:
            worker = WebSocketHandler.workers[self.client_id]
            worker.on_finished(message)
            logger.worker(f"Message received from worker {self.client_id}. New status {worker.status}")

            # Check the queue for any outstanding jobs.
            if len(WebSocketHandler.queue) > 0:
                queue_item = WebSocketHandler.queue.pop(0)
                worker.process(queue_item.message, queue_item.client)
                return

    def on_close(self) -> None:
        """
        Client closes the connection
        """
        if self.client_id in WebSocketHandler.clients:
            del WebSocketHandler.clients[self.client_id]
            logger.client(f"Client disconnected. Active clients: {len(WebSocketHandler.clients)}")
            for worker in WebSocketHandler.workers.values():
                if worker.connected_client == self:
                    worker.on_finished("Client disconnected")
                    break
        if self.client_id in WebSocketHandler.workers:
            del WebSocketHandler.workers[self.client_id]
            logger.worker(f"Worker disconnected. Active workers: {len(WebSocketHandler.workers)}")

    def check_origin(self, origin):
        """
        Override the origin check if needed
        """
        return True


class WorkerPod:
    """State of a worker pod.

    Attributes
    ----------
    id :
        The id of the worker pod.
    ws :
        The websocket connection to the worker pod.
    status :
        The status of the worker pod.
    connected_client :
        The client that is connected to this worker pod.
    """

    status: WorkerPodStatus
    connected_client: WebSocketHandler | None

    def __init__(self, wid: str, ws: WebSocketHandler):
        self.wid = wid
        self.ws = ws
        self.status = WorkerPodStatus.IDLE
        self.connected_client = None

    def process(self, message: str, connected_client: WebSocketHandler):
        """Process a message from a client.

        Parameters
        ----------
        message :
            The message to process.
        connected_client :
            The client that is connected to this worker pod.
        """
        self.status = WorkerPodStatus.BUSY
        self.connected_client = connected_client
        logger.worker(f"Worker {self.wid} processing message from client {connected_client.client_id}")
        # Send the job to the worker pod
        self.ws.write_message(message)

    def on_finished(self, message):
        """Called when the worker pod has finished processing a message."""
        if (
            self.connected_client is not None
            and self.connected_client.ws_connection is not None
            and message != "Client disconnected"
        ):
            # Send the reply to the client that made the request.
            self.connected_client.write_message(message)
        else:
            logger.error(f"Worker {self.wid} finished processing, but no client was connected.")
        self.status = WorkerPodStatus.IDLE
        self.connected_client = None


@dataclass
class QueueItem:
    """An item in the client queue.

    Attributes
    ----------
    message :
        The message to process.
    client :
        The client that is making a request.
    """

    message: str
    client: WebSocketHandler


def main():
    parser = argparse.ArgumentParser(description="Initialize a new RubinTV worker.")
    parser.add_argument(
        "-a", "--address", default="localhost", type=str, help="Address of the rubinTV web app."
    )
    parser.add_argument(
        "-p", "--port", default=8080, type=int, help="Port of the rubinTV web app websockets."
    )
    parser.add_argument(
        "--log",
        default="INFO",
        help="Set the logging level of web app (DEBUG, INFO, WARNING, ERROR, CRITICAL).",
    )
    args = parser.parse_args()

    # Configure logging
    log_level = getattr(logging, args.log.upper(), None)
    if not isinstance(log_level, int):
        raise ValueError(f"Invalid log level: {args.log}")

    # Use custom formatting for the server logs
    handler = logging.StreamHandler()
    handler.setFormatter(ServerFormatter())
    for logger_name in [
        "lsst.rubintv.analysis.service.worker",
        "lsst.rubintv.analysis.service.client",
        "lsst.rubintv.analysis.service.commands.db",
        "lsst.rubintv.analysis.service.database",
    ]:
        logger = logging.getLogger(logger_name)
        logger.setLevel(log_level)
        logger.addHandler(handler)

    # Create tornado application and supply URL routes
    app = tornado.web.Application(WebSocketHandler.urls())  # type: ignore

    # Setup HTTP Server
    http_server = tornado.httpserver.HTTPServer(app)
    http_server.listen(args.port, args.address)

    logger.connection(f"Listening on address: {args.address}, {args.port}")

    # Start IO/Event loop
    tornado.ioloop.IOLoop.instance().start()


if __name__ == "__main__":
    main()
