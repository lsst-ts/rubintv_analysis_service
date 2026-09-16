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

"""The wire contract the Flutter DDV client relies on.

The commands here are the exact JSON that ``rubintv_visualization`` builds
(``lib/io.dart`` and ``lib/query/primitives.dart``), and the assertions are
what its message handlers read (``lib/chart/base.dart``,
``lib/workspace/state.dart`` and ``lib/dialog/bloc.dart``). Two clients now
speak this protocol, so a change made for one must keep these passing for
the other.
"""

import datetime
import json
import os
import re
from unittest.mock import MagicMock, patch

import lsst.rubintv.analysis.service as lras
import utils

# LoadColumnsCommand and CountRowsCommand use
# "${windowId.id},${seriesId.shortString}".
WINDOW_SERIES_REQUEST_ID = "3f2a1b,0"
# FileDialogCommand: a fixed requestId shared by all file-dialog commands.
FILE_DIALOG_REQUEST_ID = "file dialog"

COLUMNS = ["exposure.ra", "exposure.dec"]
# Test rows where both ra and dec are non-null, in seq_num order.
VALID_ROWS = 7


def field(schema: str, name: str) -> dict:
    """``SchemaField.toJson`` as sent inside an EqualityQuery."""
    return {"schema": schema, "name": name}


class TestFlutterContract(utils.RasTestCase):
    def send(self, command: dict) -> dict:
        return json.loads(lras.command.execute_command(json.dumps(command), self.data_center))

    def load_columns(self, request_id=WINDOW_SERIES_REQUEST_ID, **overrides) -> dict:
        """``LoadColumnsCommand``: every parameter is sent, unused as null."""
        parameters = {
            "database": "testdb",
            "columns": COLUMNS,
            "query": None,
            "global_query": None,
            "data_ids": None,
            "day_obs": None,
            "is_new_plot": False,
        }
        parameters.update(overrides)
        return self.send({"name": "load columns", "parameters": parameters, "requestId": request_id})

    @patch.dict(
        "sys.modules",
        {
            "lsst": MagicMock(),
            "lsst.obs": MagicMock(),
            "lsst.obs.lsst": MagicMock(),
            "lsst.afw.cameraGeom": MagicMock(),
        },
    )
    def test_load_instrument_has_no_request_id(self):
        # LoadInstrumentAction sends no requestId, and the workspace matches
        # the reply on its type alone.
        reply = self.send({"name": "load instrument", "parameters": {"instrument": "testdb"}})
        self.assertEqual(reply["type"], "instrument info")
        self.assertNotIn("requestId", reply)
        self.assertIn("tables", reply["content"]["schema"])

    def test_load_columns_reply(self):
        reply = self.load_columns(is_new_plot=True)
        # base.dart: type "table columns" and requestId "windowId,seriesId".
        self.assertEqual(reply["type"], "table columns")
        self.assertEqual(reply["requestId"], WINDOW_SERIES_REQUEST_ID)
        content = reply["content"]
        self.assertEqual(content["schema"], "testdb")
        self.assertEqual(content["columns"], COLUMNS)
        # content.data is column -> list, with the data ids injected under the
        # bare names day_obs and seq_num, and every list the same length.
        data = content["data"]
        self.assertEqual(set(data), {*COLUMNS, "day_obs", "seq_num"})
        self.assertEqual({len(values) for values in data.values()}, {VALID_ROWS})

    def test_count_rows_reply(self):
        # CountRowsCommand is "load columns" with aggregator "count" and
        # response_type "count"; base.dart reads content.data.values.first
        # as an int.
        reply = self.load_columns(aggregator="count", response_type="count")
        self.assertEqual(reply["type"], "count")
        self.assertEqual(reply["requestId"], WINDOW_SERIES_REQUEST_ID)
        self.assertEqual(reply["content"]["data"], {column: VALID_ROWS for column in COLUMNS})

    def test_query_operators_use_the_enum_names(self):
        # EqualityQuery.toJson sends EqualityOperator.name: the string
        # operators arrive as "startswith", "endswith" and "contains".
        reply = self.load_columns(
            query={
                "type": "EqualityQuery",
                "id": "q1",
                "field": field("exposure", "physical_filter"),
                "rightOperator": "startswith",
                "rightValue": "DECam",
            }
        )
        self.assertEqual(reply["type"], "table columns")
        self.assertEqual(reply["content"]["data"]["seq_num"], [5, 8, 9])

        # Only lt and le are offered on the left ("40 < ra" flips to ra > 40).
        reply = self.load_columns(
            query={
                "type": "EqualityQuery",
                "id": "q2",
                "field": field("exposure", "ra"),
                "leftOperator": "lt",
                "leftValue": 40,
            }
        )
        self.assertEqual(reply["content"]["data"]["seq_num"], [4, 5, 8, 9])

        # Both sides at once: 40 < ra <= 90.
        reply = self.load_columns(
            query={
                "type": "EqualityQuery",
                "id": "q3",
                "field": field("exposure", "ra"),
                "leftOperator": "lt",
                "leftValue": 40,
                "rightOperator": "le",
                "rightValue": 90,
            }
        )
        self.assertEqual(reply["content"]["data"]["seq_num"], [4, 5, 8])

    def test_global_query_is_anded_with_the_query(self):
        # ParentQuery.toJson: operator is QueryOperator.name, i.e. "AND".
        global_query = {
            "type": "ParentQuery",
            "id": "g",
            "operator": "AND",
            "children": [
                {
                    "type": "EqualityQuery",
                    "id": "g1",
                    "field": field("exposure", "ra"),
                    "rightOperator": "ne",
                    "rightValue": 50,
                },
                {
                    "type": "EqualityQuery",
                    "id": "g2",
                    "field": field("exposure", "dec"),
                    "rightOperator": "lt",
                    "rightValue": 45,
                },
            ],
        }
        query = {
            "type": "EqualityQuery",
            "id": "q",
            "field": field("exposure", "physical_filter"),
            "rightOperator": "contains",
            "rightValue": "band",
        }
        reply = self.load_columns(query=query, global_query=global_query)
        self.assertEqual(reply["content"]["data"]["seq_num"], [0, 1, 3, 5, 8])

    def strict_decode(self, reply: str) -> dict:
        """Decode as Dart's ``jsonDecode`` does.

        ``NaN`` and ``Infinity`` are not JSON and are rejected.
        """

        def reject(token):
            raise AssertionError(f"{token} is not JSON and the client cannot decode it")

        return json.loads(reply, parse_constant=reject)

    def test_timestamps_use_the_format_date_from_string_parses(self):
        # workspace/data.dart maps datatype "timestamp" to
        # ColumnDataType.dateTime and parses each value with rubin_chart's
        # dateFromString, which splits on a space and reads "year-month-day"
        # then "hours:minutes:seconds[.fraction]". No "T", no zone suffix.
        rows = {
            "exposure.obs_start": [
                datetime.datetime(2023, 5, 19, 20, 20, 20),
                datetime.datetime(2023, 5, 19, 20, 20, 20, 123456),
            ],
            "day_obs": [20230519, 20230519],
            "seq_num": [0, 1],
        }
        with patch.object(self.database, "fetch_data", return_value=rows):
            reply = self.load_columns(columns=["exposure.obs_start"])
        values = reply["content"]["data"]["exposure.obs_start"]
        pattern = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d{1,6})?$")
        for value in values:
            self.assertRegex(value, pattern)
        self.assertEqual(values, ["2023-05-19 20:20:20", "2023-05-19 20:20:20.123456"])

    def test_number_columns_never_carry_nan(self):
        # base.dart reads every number with toDouble() and jsonDecode rejects
        # the NaN token outright, so a row with a nan must not be sent at all.
        rows = {
            "exposure.ra": [1.0, float("nan"), 3.0],
            "exposure.dec": [-1.0, -2.0, float("inf")],
            "day_obs": [20230519] * 3,
            "seq_num": [0, 1, 2],
        }
        with patch.object(self.database, "fetch_data", return_value=rows):
            raw = lras.command.execute_command(
                json.dumps(
                    {
                        "name": "load columns",
                        "parameters": {"database": "testdb", "columns": COLUMNS},
                        "requestId": WINDOW_SERIES_REQUEST_ID,
                    }
                ),
                self.data_center,
            )
        reply = self.strict_decode(raw)
        data = reply["content"]["data"]
        self.assertEqual(data["seq_num"], [0])
        self.assertNotIn(None, data["exposure.ra"])
        self.assertNotIn(None, data["exposure.dec"])

    def test_error_reply(self):
        # workspace/state.dart reads content.error, content.description and
        # content.traceback from a type "error" reply.
        reply = self.load_columns(columns=["exposure.not_a_column"])
        self.assertEqual(reply["type"], "error")
        self.assertEqual(reply["requestId"], WINDOW_SERIES_REQUEST_ID)
        content = reply["content"]
        self.assertEqual(content["error"], "execution error")
        self.assertIsInstance(content["description"], str)
        self.assertIsInstance(content["traceback"], str)

    def test_file_dialog_replies(self):
        # dialog/bloc.dart casts content to a map before anything else, then
        # matches requestId "file dialog" and the reply type.
        os.makedirs(os.path.join(self.user_dir, "saved"))
        with open(os.path.join(self.user_dir, "workspace.json"), "w") as f:
            f.write("{}")

        reply = self.send(
            {
                "name": "list directory",
                "parameters": {"path": []},
                "requestId": FILE_DIALOG_REQUEST_ID,
            }
        )
        self.assertEqual(reply["type"], "directory files")
        self.assertEqual(reply["requestId"], FILE_DIALOG_REQUEST_ID)
        self.assertEqual(
            reply["content"],
            {"path": [], "files": ["workspace.json"], "directories": ["saved"]},
        )

        # A failed file command still carries a content map; the error is
        # reported inside it.
        reply = self.send(
            {
                "name": "list directory",
                "parameters": {"path": ["missing"]},
                "requestId": FILE_DIALOG_REQUEST_ID,
            }
        )
        self.assertEqual(reply["type"], "directory files")
        self.assertEqual(reply["requestId"], FILE_DIALOG_REQUEST_ID)
        self.assertIsInstance(reply["content"], dict)
        self.assertIn("error", reply["content"])
