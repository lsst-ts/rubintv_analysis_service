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
from enum import Enum

__all__ = ["ServerFormatter"]

# Define custom loggers for the web app
WORKER_LEVEL = 21
CLIENT_LEVEL = 22
CONNECTION_LEVEL = 23
logging.addLevelName(WORKER_LEVEL, "WORKER")
logging.addLevelName(CLIENT_LEVEL, "CLIENT")
logging.addLevelName(CONNECTION_LEVEL, "CONNECTION")


def worker(self, message, *args, **kws):
    """Special log level for workers"""
    if self.isEnabledFor(WORKER_LEVEL):
        self._log(WORKER_LEVEL, message, args, **kws)


def client(self, message, *args, **kws):
    """Special log level for clients"""
    if self.isEnabledFor(CLIENT_LEVEL):
        self._log(CLIENT_LEVEL, message, args, **kws)


def connection(self, message, *args, **kws):
    """Special log level for connections"""
    if self.isEnabledFor(CONNECTION_LEVEL):
        self._log(CONNECTION_LEVEL, message, args, **kws)


logging.Logger.worker = worker
logging.Logger.client = client
logging.Logger.connection = connection


# ANSI color codes for printing to the terminal
class Colors(Enum):
    RESET = 0
    BLACK = 30
    RED = 31
    GREEN = 32
    YELLOW = 33
    BLUE = 34
    MAGENTA = 35
    CYAN = 36
    WHITE = 37
    DEFAULT = 39
    BRIGHT_BLACK = 90
    BRIGHT_RED = 91
    BRIGHT_GREEN = 92
    BRIGHT_YELLOW = 93
    BRIGHT_BLUE = 94
    BRIGHT_MAGENTA = 95
    BRIGHT_CYAN = 96
    BRIGHT_WHITE = 97

    @property
    def ansi_code(self):
        return f"\x1b[{self.value};20m"


def color_to_ansi(color: Colors) -> str:
    return f"\x1b[{color.value};20m"


class ServerFormatter(logging.Formatter):
    format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    FORMATS = {
        logging.DEBUG: Colors.BRIGHT_BLACK.ansi_code + format + Colors.RESET.ansi_code,
        logging.INFO: Colors.WHITE.ansi_code + format + Colors.RESET.ansi_code,
        WORKER_LEVEL: Colors.BLUE.ansi_code + format + Colors.BRIGHT_RED.ansi_code,
        CLIENT_LEVEL: Colors.YELLOW.ansi_code + format + Colors.BRIGHT_RED.ansi_code,
        CONNECTION_LEVEL: Colors.GREEN.ansi_code + format + Colors.BRIGHT_RED.ansi_code,
        logging.WARNING: Colors.YELLOW.ansi_code + format + Colors.RESET.ansi_code,
        logging.ERROR: Colors.RED.ansi_code + format + Colors.RESET.ansi_code,
        logging.CRITICAL: Colors.BRIGHT_RED.ansi_code + format + Colors.RESET.ansi_code,
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno, self.format)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)
