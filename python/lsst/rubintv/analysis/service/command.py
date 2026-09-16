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

import datetime
import decimal
import json
import logging
import math
import time
import traceback
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import numpy as np

if TYPE_CHECKING:
    from .data import DataCenter


logger = logging.getLogger("lsst.rubintv.analysis.service.command")


def format_timestamp(value: datetime.datetime) -> str:
    """Format a timestamp the way the clients parse it.

    The React client (``rubintv-ddv``, ``parseTimestampMs``) recognises a
    ``YYYY-MM-DD `` prefix and swaps the space for ``T`` before
    ``Date.parse``; ``rubin_chart``'s ``dateFromString`` in the Flutter
    client splits on the space and reads the two halves. Neither wants a
    zone suffix, so this is deliberately not `datetime.isoformat` with its
    defaults.

    ConsDB timestamps are ``timestamp without time zone`` columns holding
    TAI, which the driver returns as naive datetimes; those are formatted as
    they are. An aware datetime is converted to UTC first and its zone
    dropped, so the string is still unambiguous.

    Parameters
    ----------
    value :
        The timestamp to format.

    Returns
    -------
    formatted :
        ``YYYY-MM-DD HH:MM:SS`` followed by ``.ffffff`` when the timestamp has
        a fractional second.
    """
    if value.tzinfo is not None:
        value = value.astimezone(datetime.timezone.utc).replace(tzinfo=None)
    return value.isoformat(sep=" ")


def to_json_safe(value: Any) -> Any:
    """Convert a command result into values that JSON can carry.

    `json.dumps` only knows the JSON types, but the values that reach a
    result come from the database driver, numpy and the stack, so this walks
    the result and rewrites everything else:

    - `datetime.datetime` becomes the string described in `format_timestamp`;
      `datetime.date` and `datetime.time` become their ISO strings and a
      `datetime.timedelta` its length in seconds.
    - A non-finite float (``nan``, ``inf``) becomes ``null``. Python would
      otherwise emit the bare tokens ``NaN`` and ``Infinity``, which are not
      JSON and make the Dart client's decoder reject the whole message.
    - `decimal.Decimal` becomes a float, `uuid.UUID` a string.
    - numpy scalars and arrays become the equivalent Python values, then are
      converted again by the rules above.
    - Tuples and sets become lists.

    Anything else is returned unchanged, so an unsupported type still fails
    in `json.dumps` with its usual ``TypeError`` rather than being silently
    stringified.

    Parameters
    ----------
    value :
        The result, or a value within it.

    Returns
    -------
    converted :
        The value with every unsupported type replaced.
    """
    if isinstance(value, dict):
        return {key: to_json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [to_json_safe(item) for item in value]
    if isinstance(value, np.ndarray):
        return to_json_safe(value.tolist())
    if isinstance(value, np.generic):
        return to_json_safe(value.item())
    # bool is an int subclass, and is left alone by the float check below.
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, datetime.datetime):
        return format_timestamp(value)
    if isinstance(value, (datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, datetime.timedelta):
        return value.total_seconds()
    if isinstance(value, decimal.Decimal):
        return to_json_safe(float(value))
    if isinstance(value, uuid.UUID):
        return str(value)
    return value


def construct_error_message(error_name: str, description: str, traceback: str, request_id: Any = None) -> str:
    """Use a standard format for all error messages.

    Parameters
    ----------
    error_name :
        Name of the error.
    description :
        Description of the error.
    traceback :
        The formatted traceback of the error.
    request_id :
        The ``requestId`` of the command that failed, echoed back so the
        client can match the error to its request. Omitted from the message
        when the command carried none (or could not be parsed at all).

    Returns
    -------
    result :
        JSON formatted string.
    """
    message: dict[str, Any] = {
        "type": "error",
        "content": {
            "error": error_name,
            "description": description,
            "traceback": traceback,
        },
    }
    if request_id is not None:
        message["requestId"] = request_id
    return json.dumps(message)


def error_msg(error: Exception, traceback: str, request_id: Any = None) -> str:
    """Handle errors received while parsing or executing a command.

    Parameters
    ----------
    error :
        The error that was raised while parsing, executing,
        or responding to a command.
    traceback :
        The formatted traceback of the error.
    request_id :
        The ``requestId`` of the failed command, if it had one.

    Returns
    -------
    response :
        The JSON formatted error message sent to the user.

    """
    if isinstance(error, json.decoder.JSONDecodeError):
        return construct_error_message("JSON decoder error", error.args[0], traceback, request_id)

    if isinstance(error, CommandParsingError):
        return construct_error_message("parsing error", error.args[0], traceback, request_id)

    if isinstance(error, CommandExecutionError):
        return construct_error_message("execution error", error.args[0], traceback, request_id)

    if isinstance(error, CommandResponseError):
        return construct_error_message("command response error", error.args[0], traceback, request_id)

    # We should always receive one of the above errors, so the code should
    # never get to here. But we generate this response just in case something
    # very unexpected happens, or (more likely) the code is altered in such a
    # way that this line is it.
    msg = "An unknown error occurred, you should never reach this message."
    return construct_error_message(error.__class__.__name__, msg, traceback, request_id)


class CommandParsingError(Exception):
    """An `~Exception` caused by an error in parsing a command and
    constructing a response.
    """

    pass


class CommandExecutionError(Exception):
    """An error occurred while executing a command."""

    pass


class CommandResponseError(Exception):
    """An error occurred while converting a command result to JSON"""

    pass


@dataclass(kw_only=True)
class BaseCommand(ABC):
    """Base class for commands.

    Attributes
    ----------
    result :
        The response generated by the command as a `dict` that can
        be converted into JSON.
    response_type :
        The type of response that this command sends to the user.
        This should be unique for each command.
    """

    command_registry = {}
    result: dict | None = None
    response_type: str
    request_id: str | None = None

    @abstractmethod
    def build_contents(self, data_center: DataCenter) -> dict:
        """Build the contents of the command.

        Parameters
        ----------
        data_center :
            Connections to databases, the Butler, and the EFD.

        Returns
        -------
        contents :
            The contents of the response to the user.
        """
        pass

    def get_log_metadata(self) -> dict:
        """Return metadata to be logged for this command.

        Override this method to provide command-specific logging information.
        Should return lightweight metadata, not actual data content.

        Returns
        -------
        metadata : dict
            Dictionary of metadata to log
        """
        return {"response_type": self.response_type}

    def execute(self, data_center: DataCenter):
        """Execute the command with logging.

        This method does not return anything, buts sets the `result`,
        the JSON formatted string that is sent to the user.

        Parameters
        ----------
        data_center :
            Connections to databases, the Butler, and the EFD.

        """
        from .logging import get_persistent_logger

        persistent_logger = get_persistent_logger(data_center)
        metadata = self.get_log_metadata()

        try:
            start_time = time.time()
            self.result = {"type": self.response_type, "content": self.build_contents(data_center)}
            execution_time = time.time() - start_time

            command_name = self.__class__.__name__
            persistent_logger.info(f"{command_name} completed: {metadata} (took {execution_time:.3f}s)")

        except Exception as e:
            persistent_logger.error(f"Command failed: {metadata} - {str(e)}")
            raise

    def to_json(self, request_id: str | None = None):
        """Convert the `result` into JSON.

        The result is passed through `to_json_safe` first, so timestamps,
        non-finite floats and numpy values are sent in the form the client
        expects. ``allow_nan=False`` then guarantees the message is strict
        JSON: if a ``NaN`` ever slipped past the conversion it is reported
        here as an error rather than handed to the client, whose decoder
        would reject it.
        """
        if self.result is None:
            raise CommandExecutionError(f"Null result for command {self.__class__.__name__}")
        final_request_id = request_id if request_id is not None else self.request_id
        if final_request_id is not None:
            self.result["requestId"] = final_request_id
        return json.dumps(to_json_safe(self.result), allow_nan=False)

    @classmethod
    def register(cls, name: str):
        """Register a command."""
        BaseCommand.command_registry[name] = cls


def execute_command(command_str: str, data_center: DataCenter) -> str:
    """Parse a JSON formatted string into a command and execute it.

    Command format:
    ```
    {
        name: command name,
        content: command content (usually a dict)
    }
    ```

    Parameters
    ----------
    command_str :
        The JSON formatted command received from the user.
    data_center :
        Connections to databases, the Butler, and the EFD.
    """
    logger.debug(f"Received command {command_str}")
    try:
        command_dict = json.loads(command_str)
        if "type" in command_dict and command_dict["type"] == "ping":
            return json.dumps({"type": "pong"})
        if not isinstance(command_dict, dict):
            raise CommandParsingError(f"Could not generate a valid command from {command_str}")
    except Exception as err:
        logger.exception("Error converting command to JSON.")
        traceback_string = traceback.format_exc()
        return error_msg(err, traceback_string)

    # Echo the requestId on every reply, errors included, so the client can
    # match the reply to the request that produced it.
    request_id = command_dict.get("requestId")

    try:
        logger.info(f"Parsing command {command_dict}")
        if "name" not in command_dict.keys():
            raise CommandParsingError("No command 'name' given")

        if command_dict["name"] not in BaseCommand.command_registry.keys():
            raise CommandParsingError(f"Unrecognized command '{command_dict['name']}'")

        parameters = command_dict.get("parameters", {})

        if request_id is not None:
            parameters["request_id"] = request_id

        command = BaseCommand.command_registry[command_dict["name"]](**parameters)

    except Exception as err:
        logger.exception(f"Error parsing command {command_dict}")
        traceback_string = traceback.format_exc()
        return error_msg(
            CommandParsingError(f"'{err}' error while parsing command"), traceback_string, request_id
        )

    try:
        logger.info(f"Executing command {command_str}")
        command.execute(data_center)
    except Exception as err:
        logger.exception(f"Error executing command {command_dict}")
        traceback_string = traceback.format_exc()
        return error_msg(
            CommandExecutionError(f"{err} error executing command."), traceback_string, request_id
        )

    try:
        result = command.to_json()
    except Exception as err:
        logger.exception("Error converting command response to JSON.")
        traceback_string = traceback.format_exc()
        return error_msg(
            CommandResponseError(f"{err} error converting command response to JSON."),
            traceback_string,
            request_id,
        )

    return result
