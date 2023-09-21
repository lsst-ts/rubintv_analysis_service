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

import uuid
from dataclasses import dataclass
from enum import Enum

import tornado.httpserver
import tornado.ioloop
import tornado.web
import tornado.websocket

# Default port and address to listen on
LISTEN_PORT = 2000
LISTEN_ADDRESS = "localhost"


# ANSI color codes for printing to the terminal
ansi_colors = {
    "black": "30",
    "red": "31",
    "green": "32",
    "yellow": "33",
    "blue": "34",
    "magenta": "35",
    "cyan": "36",
    "white": "37",
}


def log(message, color, end="\033[31"):
    """Print a message to the terminal in color.

    Parameters
    ----------
    message :
        The message to print.
    color :
        The color to print the message in.
    end :
        The color future messages should be printed in.
    """
    _color = ansi_colors[color]
    print(f"\033[{_color}m{message}{end}m")


class WorkerPodStatus(Enum):
    """Status of a worker pod."""

    IDLE = "idle"
    BUSY = "busy"


class WebSocketHandler(tornado.websocket.WebSocketHandler):
    """
    Handler that handles WebSocket connections
    """

    @classmethod
    def urls(cls) -> list[tuple[str, type[tornado.web.RequestHandler], dict[str, str]]]:
        """url to handle websocket connections.

        Websocket URLs should either be followed by 'worker' for worker pods
        or client for clients.
        """
        return [
            (r"/ws/([^/]+)", cls, {}),  # Route/Handler/kwargs
        ]

    def open(self, type: str) -> None:
        """
        Client opens a websocket

        Parameters
        ----------
        type :
            The type of client that is connecting.
        """
        self.client_id = str(uuid.uuid4())
        if type == "worker":
            workers[self.client_id] = WorkerPod(self.client_id, self)
            log(f"New worker {self.client_id} connected. Total workers: {len(workers)}", "blue")
        if type == "client":
            clients[self.client_id] = self
            log(f"New client {self.client_id} connected. Total clients: {len(clients)}", "yellow")

    def on_message(self, message: str) -> None:
        """
        Message received from a client or worker.

        Parameters
        ----------
        message :
            The message received from the client or worker.
        """
        if self.client_id in clients:
            log(f"Message received from {self.client_id}", "yellow")
            client = clients[self.client_id]

            # Find an idle worker
            idle_worker = None
            for worker in workers.values():
                if worker.status == WorkerPodStatus.IDLE:
                    idle_worker = worker
                    break

            if idle_worker is None:
                # No idle worker found, add to queue
                queue.append(QueueItem(message, client))
                return
            idle_worker.process(message, client)
            return

        if self.client_id in workers:
            worker = workers[self.client_id]
            worker.on_finished(message)
            log(
                f"Message received from worker {self.client_id}. New status {worker.status}",
                "blue",
            )

            # Check the queue for any outstanding jobs.
            if len(queue) > 0:
                queue_item = queue.pop(0)
                worker.process(queue_item.message, queue_item.client)
                return

    def on_close(self) -> None:
        """
        Client closes the connection
        """
        if self.client_id in clients:
            del clients[self.client_id]
            log(f"Client disconnected. Active clients: {len(clients)}", "yellow")
            for worker in workers.values():
                if worker.connected_client == self:
                    worker.on_finished("Client disconnected")
                    break
        if self.client_id in workers:
            del workers[self.client_id]
            log(f"Worker disconnected. Active workers: {len(workers)}", "blue")

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

    def __init__(self, id: str, ws: WebSocketHandler):
        self.id = id
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
        log(f"Worker {self.id} processing message from client {connected_client.client_id}", "blue")
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
            log(f"Worker {self.id} finished processing, but no client was connected.", "red")
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


workers: dict[str, WorkerPod] = dict()  # Keep track of connected worker pods
clients: dict[str, WebSocketHandler] = dict()  # Keep track of connected clients
queue: list[QueueItem] = list()  # Queue of messages to be processed


def main():
    # Create tornado application and supply URL routes
    app = tornado.web.Application(WebSocketHandler.urls())  # type: ignore

    # Setup HTTP Server
    http_server = tornado.httpserver.HTTPServer(app)
    http_server.listen(LISTEN_PORT, LISTEN_ADDRESS)

    log(f"Listening on address: {LISTEN_ADDRESS}, {LISTEN_PORT}", "green")

    # Start IO/Event loop
    tornado.ioloop.IOLoop.instance().start()


if __name__ == "__main__":
    main()
