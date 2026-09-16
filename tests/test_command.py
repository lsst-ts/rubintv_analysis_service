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

import datetime
import decimal
import json
import math
import unittest
import uuid
from typing import cast
from unittest.mock import MagicMock, patch

import astropy.table
import lsst.rubintv.analysis.service as lras
import numpy as np
import pytest
import utils
from lsst.rubintv.analysis.service.command import format_timestamp, to_json_safe


class TestCommand(utils.RasTestCase):
    def execute_command(self, command: dict, response_type: str) -> dict:
        command_json = json.dumps(command)
        response = lras.command.execute_command(command_json, self.data_center)
        result = json.loads(response)
        self.assertEqual(result["type"], response_type)
        return result["content"]


class TestLoadInstrumentCommand(TestCommand):
    @patch.dict(
        "sys.modules",
        {
            "lsst": MagicMock(),
            "lsst.obs": MagicMock(),
            "lsst.obs.lsst": MagicMock(),
            "lsst.afw.cameraGeom": MagicMock(),
        },
    )
    def test_load_instrument_command(self):
        command = {
            "name": "load instrument",
            "parameters": {
                "instrument": "testdb",
            },
        }
        content = self.execute_command(command, response_type="instrument info")

        # Check that empty columns are not included in the returned schema
        data = utils.get_visit_data_dict()
        if "visit1_quicklook.empty_column" in data:
            del data["visit1_quicklook.empty_column"]

        visit1_quicklook = [
            table for table in content["schema"]["tables"] if table["name"] == "visit1_quicklook"
        ][0]
        column_names = [f"visit1_quicklook.{column['name']}" for column in visit1_quicklook["columns"]]

        self.assertListEqual(column_names, list(data.keys()))


class TestGetCamera(utils.RasTestCase):
    """The instrument -> camera class lookup.

    Instruments are named by the schemas in config.yaml, so resolving the class
    from the name means adding an instrument does not also mean editing code.
    """

    def make_obs_lsst(self):
        """A stand-in for lsst.obs.lsst holding the real class names."""
        module = MagicMock()
        names = ["LsstCam", "LsstComCam", "LsstComCamSim", "Latiss"]
        cameras = {}
        for name in names:
            camera_class = MagicMock()
            camera_class.getCamera.return_value = f"<{name}>"
            cameras[name] = camera_class
            setattr(module, name, camera_class)
        module.__dir__ = lambda self=None: names
        return module, cameras

    def test_resolves_configured_instruments(self):
        module, _ = self.make_obs_lsst()
        obs = MagicMock()
        obs.lsst = module
        with patch.dict("sys.modules", {"lsst.obs": obs, "lsst.obs.lsst": module}):
            for instrument, expected in [
                ("lsstcam", "LsstCam"),
                ("lsstcomcam", "LsstComCam"),
                ("lsstcomcamsim", "LsstComCamSim"),
                ("latiss", "Latiss"),
            ]:
                self.assertEqual(lras.commands.db.get_camera(instrument), f"<{expected}>")

    def test_instrument_without_camera(self):
        self.assertIsNone(lras.commands.db.get_camera("testdb"))

    def test_unknown_instrument_raises(self):
        module, _ = self.make_obs_lsst()
        obs = MagicMock()
        obs.lsst = module
        with patch.dict("sys.modules", {"lsst.obs": obs, "lsst.obs.lsst": module}):
            with self.assertRaises(ValueError):
                lras.commands.db.get_camera("not_an_instrument")

    def test_warm_cameras_survives_failures(self):
        """A camera that cannot be loaded must not stop the others warming."""
        module, cameras = self.make_obs_lsst()
        obs = MagicMock()
        obs.lsst = module
        with patch.dict("sys.modules", {"lsst.obs": obs, "lsst.obs.lsst": module}):
            lras.commands.db.warm_cameras(["lsstcam", "not_an_instrument", "testdb"])
        cameras["LsstCam"].getCamera.assert_called_once()


class TestLoadColumnsWithAggregatorCommand(TestCommand):
    def setUpTest(self):
        """
        Set up common test parameters.
        """
        self.columns = [
            "exposure.ra",
            "exposure.dec",
        ]
        self.database = "testdb"
        self.base_command = {
            "name": "load columns",
            "parameters": {
                "database": self.database,
                "columns": self.columns,
            },
        }

    def test_count_rows(self):
        """
        Test counting rows with an aggregator.
        """
        self.setUpTest()
        command = {
            **self.base_command,
            "parameters": {
                **self.base_command["parameters"],
                "aggregator": "count",
            },
        }
        content = self.execute_command(command, "table columns")
        data = content["data"]
        self.assertEqual(data, {self.columns[0]: 7, self.columns[1]: 7})

    def test_sum_rows(self):
        """
        Test summing rows with an aggregator.
        """
        self.setUpTest()
        command = {
            **self.base_command,
            "parameters": {
                **self.base_command["parameters"],
                "aggregator": "sum",
            },
        }
        content = self.execute_command(command, "table columns")
        data = content["data"]
        self.assertEqual(data, {"exposure.ra": 370.0, "exposure.dec": 20.0})

    def test_aggregator_with_conditions(self):
        """
        Test applying an aggregator with additional query conditions.
        """
        self.setUpTest()
        query = {
            "type": "EqualityQuery",
            "field": {
                "schema": "visit1_quicklook",
                "name": "exp_time",
            },
            "rightOperator": "eq",
            "rightValue": 30,
        }
        command = {
            **self.base_command,
            "parameters": {
                **self.base_command["parameters"],
                "aggregator": "avg",  # Average RA and DEC
                "query": query,
            },
        }
        content = self.execute_command(command, "table columns")
        data = content["data"]
        self.assertEqual(data, {"exposure.ra": 30.0, "exposure.dec": -20.0})

    def test_count_with_specific_query_structure(self):
        """
        Test counting rows with aggregator using the specific query structure
        format.
        """
        command = {
            "name": "load columns",
            "parameters": {
                "aggregator": "count",
                "database": "testdb",  # Using testdb instead of cdb_lsstcam for test compatibility
                "columns": [
                    "exposure.ra",
                    "exposure.dec",
                ],  # Using ra/dec instead of s_ra/s_dec for test data compatibility
                "query": {
                    "type": "EqualityQuery",
                    "id": "2",
                    "field": {"name": "ra", "schema": "exposure", "database": "testdb"},
                    "rightOperator": "le",
                    "rightValue": "20",
                },
            },
        }
        content = self.execute_command(command, "table columns")
        data = content["data"]
        # Based on test data, ra values <= 20 are: [10] (2 non-null value)
        self.assertEqual(data, {"exposure.ra": 2, "exposure.dec": 2})

    def test_count_rows_grouped(self):
        """
        Test counting rows per group with an aggregator and group_by.
        """
        self.setUpTest()
        command = {
            **self.base_command,
            "parameters": {
                **self.base_command["parameters"],
                "aggregator": "count",
                "group_by": ["exposure.day_obs"],
            },
        }
        content = self.execute_command(command, "table columns")
        # Rows with a null ra or dec are excluded before counting; the groups
        # are ordered by the group column.
        self.assertEqual(
            content["data"],
            {
                "exposure.day_obs": ["2023-02-14", "2023-05-19"],
                self.columns[0]: [3, 4],
                self.columns[1]: [3, 4],
            },
        )

    def test_aggregate_grouped(self):
        """
        Test a per-column aggregator with group_by.
        """
        self.setUpTest()
        command = {
            **self.base_command,
            "parameters": {
                **self.base_command["parameters"],
                "aggregator": "max",
                "group_by": ["exposure.day_obs"],
            },
        }
        content = self.execute_command(command, "table columns")
        self.assertEqual(
            content["data"],
            {
                "exposure.day_obs": ["2023-02-14", "2023-05-19"],
                self.columns[0]: [100, 50],
                self.columns[1]: [50, 0],
            },
        )

    def test_group_by_requires_aggregator(self):
        """
        group_by without an aggregator is rejected rather than ignored.
        """
        self.setUpTest()
        command = {
            **self.base_command,
            "parameters": {
                **self.base_command["parameters"],
                "group_by": ["exposure.day_obs"],
            },
        }
        content = self.execute_command(command, "error")
        self.assertEqual(content["error"], "execution error")
        self.assertIn("group_by", content["description"])


class TestCalculateBoundsCommand(TestCommand):
    def test_calculate_bounds_command(self):
        command = {
            "name": "get bounds",
            "parameters": {
                "database": "testdb",
                "column": "exposure.dec",
            },
        }
        print(lras.command.BaseCommand.command_registry)
        content = self.execute_command(command, "column bounds")
        self.assertEqual(content["column"], "exposure.dec")
        self.assertListEqual(content["bounds"], [-40, 50])


class TestLoadColumnsCommand(TestCommand):
    def test_load_full_columns(self):
        command = {
            "name": "load columns",
            "parameters": {
                "database": "testdb",
                "columns": [
                    "exposure.ra",
                    "exposure.dec",
                ],
            },
        }

        content = self.execute_command(command, "table columns")
        data = content["data"]
        print(data)

        truth = cast(
            astropy.table.Table,
            utils.get_test_data("exposure")[
                "exposure.ra",
                "exposure.dec",
                "exposure.day_obs",
                "exposure.seq_num",
            ],
        )
        valid = (truth["exposure.ra"] != None) & (truth["exposure.dec"] != None)  # noqa: E711
        truth = cast(astropy.table.Table, truth[valid])
        self.assertDataTableEqual(data, truth)

    def test_load_columns_with_query(self):
        command = {
            "name": "load columns",
            "parameters": {
                "database": "testdb",
                "columns": [
                    "visit1_quicklook.visit_id",
                    "exposure.ra",
                    "exposure.dec",
                ],
                "query": {
                    "type": "EqualityQuery",
                    "field": {
                        "schema": "visit1_quicklook",
                        "name": "exp_time",
                    },
                    "rightOperator": "eq",
                    "rightValue": 30,
                },
            },
        }

        content = self.execute_command(command, "table columns")
        data = content["data"]

        visit_truth = utils.get_test_data("exposure")
        exp_truth = utils.get_test_data("visit1_quicklook")
        truth = astropy.table.join(
            visit_truth,
            exp_truth,
            keys_left=("exposure.exposure_id",),
            keys_right=("visit1_quicklook.visit_id",),
        )
        truth = truth[
            "visit1_quicklook.visit_id",
            "exposure.ra",
            "exposure.dec",
            "exposure.day_obs",
            "exposure.seq_num",
        ]

        # Select rows with expTime = 30
        truth = truth[[True, True, False, False, False, True, False, False, False, False]]
        self.assertDataTableEqual(data, truth)

    @patch("lsst.rubintv.analysis.service.commands.db.logger")
    def test_is_new_plot_logged(self, mock_logger):
        """Test that is_new_plot=True is reflected in the log metadata."""
        # Create a LoadColumnsCommand directly to test logging
        from lsst.rubintv.analysis.service.commands.db import LoadColumnsCommand

        command = LoadColumnsCommand(
            database="testdb",
            columns=["exposure.ra", "exposure.dec"],
            is_new_plot=True,
        )

        # Execute the command to trigger logging
        command.build_contents(self.data_center)

        # Get the log metadata
        log_metadata = command.get_log_metadata()

        # Assert that is_new_plot is True in the metadata
        self.assertTrue(log_metadata["is_new_plot"])

        # Also test the inverse case
        command_false = LoadColumnsCommand(
            database="testdb",
            columns=["exposure.ra", "exposure.dec"],
            is_new_plot=False,
        )

        log_metadata_false = command_false.get_log_metadata()
        self.assertFalse(log_metadata_false["is_new_plot"])


class TestCommandErrors(TestCommand):
    def check_error_response(self, content: dict, error: str, description: str | None = None):
        self.assertEqual(content["error"], error)
        if description is not None:
            self.assertEqual(content["description"], description)

    def test_errors(self):
        # Command cannot be decoded as JSON dict
        content = self.execute_command("{'test': [1,2,3,0004,}", "error")  # type: ignore
        self.check_error_response(content, "parsing error")

        # Command does not contain a "name"
        command = {"content": {}}
        content = self.execute_command(command, "error")
        self.check_error_response(
            content,
            "parsing error",
            "'No command 'name' given' error while parsing command",
        )

        # Command has an invalid name
        command = {"name": "invalid name"}
        content = self.execute_command(command, "error")
        self.check_error_response(
            content,
            "parsing error",
            "'Unrecognized command 'invalid name'' error while parsing command",
        )

        # Command has no parameters
        command = {"name": "get bounds"}
        content = self.execute_command(command, "error")
        self.check_error_response(
            content,
            "parsing error",
        )

        # Command has invalid parameters
        command = {
            "name": "get bounds",
            "parameters": {
                "a": 1,
            },
        }
        content = self.execute_command(command, "error")
        self.check_error_response(
            content,
            "parsing error",
        )

        # Command execution failed (table name does not exist)
        command = {
            "name": "get bounds",
            "parameters": {"database": "testdb", "column": "InvalidTable.invalid_column"},
        }
        content = self.execute_command(command, "error")
        self.check_error_response(
            content,
            "execution error",
        )


# Only runs if butler instantiated
@pytest.mark.skip(reason="Needs butler access")
class TestSendFitsImageCommand(TestCommand):
    def test_send_fits_image_command(self):
        command = {
            "name": "get fits image",
            "parameters": {
                "repo": "embargo",
                "collection": "u/kadrlica/binCalexp4",
                "image_name": "calexpBinned8",
                "data_id": {"instrument": "LSSTComCam", "detector": 3, "visit": 2024110900185},
            },
        }

        print(lras.command.BaseCommand.command_registry)
        content = self.execute_command(command, "get fits image")
        length = len(content["fits"])
        self.assertEqual(length, 4608000)

    def test_error_echoes_request_id(self):
        """An error reply carries the requestId of the command that failed."""

        def send(command) -> dict:
            command_json = command if isinstance(command, str) else json.dumps(command)
            return json.loads(lras.command.execute_command(command_json, self.data_center))

        # Parsing error
        response = send({"name": "get bounds", "parameters": {"a": 1}, "requestId": "abc,0"})
        self.assertEqual(response["type"], "error")
        self.assertEqual(response["content"]["error"], "parsing error")
        self.assertEqual(response["requestId"], "abc,0")

        # Execution error, with a non-string requestId
        response = send(
            {
                "name": "load columns",
                "parameters": {"database": "testdb", "columns": ["exposure.not_a_column"]},
                "requestId": 7,
            }
        )
        self.assertEqual(response["type"], "error")
        self.assertEqual(response["content"]["error"], "execution error")
        self.assertEqual(response["requestId"], 7)

        # No requestId on the command: none on the reply either
        response = send({"name": "invalid name"})
        self.assertEqual(response["type"], "error")
        self.assertNotIn("requestId", response)

        # Undecodable command: there is no requestId to echo
        response = send("{'test': [1,2,3,0004,}")
        self.assertEqual(response["type"], "error")
        self.assertNotIn("requestId", response)


class TestJsonSerialization(unittest.TestCase):
    """The conversion of driver and numpy values into JSON."""

    def test_json_types_are_unchanged(self):
        result = {"int": 1, "float": 1.5, "str": "s", "bool": True, "none": None, "list": [1, "a"]}
        self.assertEqual(to_json_safe(result), result)
        # bool is a subclass of int and must not be mistaken for a number to
        # check for finiteness, or anything else.
        self.assertIs(to_json_safe(False), False)

    def test_timestamps(self):
        self.assertEqual(format_timestamp(datetime.datetime(2023, 5, 19, 20, 20, 20)), "2023-05-19 20:20:20")
        self.assertEqual(
            format_timestamp(datetime.datetime(2023, 5, 19, 20, 20, 20, 123456)),
            "2023-05-19 20:20:20.123456",
        )
        # An aware timestamp is sent as UTC with the zone dropped.
        aware = datetime.datetime(
            2023, 5, 19, 22, 20, 20, tzinfo=datetime.timezone(datetime.timedelta(hours=2))
        )
        self.assertEqual(format_timestamp(aware), "2023-05-19 20:20:20")
        self.assertEqual(to_json_safe(datetime.datetime(2023, 5, 19, 20, 20, 20)), "2023-05-19 20:20:20")

    def test_other_temporal_types(self):
        self.assertEqual(to_json_safe(datetime.date(2023, 5, 19)), "2023-05-19")
        self.assertEqual(to_json_safe(datetime.time(20, 20, 20)), "20:20:20")
        self.assertEqual(to_json_safe(datetime.timedelta(minutes=1, seconds=30)), 90.0)

    def test_non_finite_floats_become_null(self):
        self.assertIsNone(to_json_safe(float("nan")))
        self.assertIsNone(to_json_safe(float("inf")))
        self.assertIsNone(to_json_safe(float("-inf")))
        self.assertEqual(to_json_safe([1.0, float("nan"), 2.0]), [1.0, None, 2.0])

    def test_decimal_and_uuid(self):
        self.assertEqual(to_json_safe(decimal.Decimal("1.25")), 1.25)
        self.assertIsNone(to_json_safe(decimal.Decimal("NaN")))
        identifier = uuid.UUID("12345678-1234-5678-1234-567812345678")
        self.assertEqual(to_json_safe(identifier), str(identifier))

    def test_numpy(self):
        self.assertEqual(to_json_safe(np.int64(3)), 3)
        self.assertIsInstance(to_json_safe(np.int64(3)), int)
        self.assertEqual(to_json_safe(np.float32(1.5)), 1.5)
        self.assertIsNone(to_json_safe(np.float64("nan")))
        self.assertIs(to_json_safe(np.bool_(True)), True)
        self.assertEqual(to_json_safe(np.array([[1, 2], [3, 4]])), [[1, 2], [3, 4]])
        self.assertEqual(to_json_safe(np.array([1.0, np.nan])), [1.0, None])
        self.assertEqual(
            to_json_safe(np.datetime64("2023-05-19T20:20:20.123456")),
            "2023-05-19 20:20:20.123456",
        )

    def test_containers(self):
        self.assertEqual(to_json_safe((1, 2)), [1, 2])
        self.assertEqual(to_json_safe({3}), [3])
        nested = {"a": [{"b": (datetime.date(2023, 5, 19), float("nan"))}]}
        self.assertEqual(to_json_safe(nested), {"a": [{"b": ["2023-05-19", None]}]})

    def test_unknown_types_still_fail(self):
        # Rather than stringifying whatever it does not recognise, the
        # conversion leaves it for json.dumps to reject.
        class Opaque:
            pass

        opaque = Opaque()
        self.assertIs(to_json_safe(opaque), opaque)
        with self.assertRaises(TypeError):
            json.dumps(to_json_safe({"x": opaque}))


class TestResultSerialization(TestCommand):
    """The values a query can produce, sent through a real command."""

    def load(self, rows: dict) -> str:
        """Send "load columns", with the database returning ``rows``."""
        with patch.object(self.database, "fetch_data", return_value=rows):
            return lras.command.execute_command(
                json.dumps(
                    {
                        "name": "load columns",
                        "parameters": {"database": "testdb", "columns": ["exposure.obs_start"]},
                        "requestId": "r1",
                    }
                ),
                self.data_center,
            )

    def test_timestamps_and_non_finite_values_are_serialized(self):
        rows = {
            "exposure.obs_start": [
                datetime.datetime(2023, 5, 19, 20, 20, 20),
                datetime.datetime(2023, 5, 19, 21, 21, 21, 500000),
            ],
            "exposure.obs_start_mjd": [np.float64(60083.847453703704), decimal.Decimal("60083.9")],
            "day_obs": [np.int64(20230519), 20230519],
            "seq_num": [0, 1],
        }
        reply = self.load(rows)
        content = json.loads(reply)
        self.assertEqual(content["type"], "table columns")
        self.assertEqual(content["requestId"], "r1")
        self.assertEqual(
            content["content"]["data"],
            {
                "exposure.obs_start": ["2023-05-19 20:20:20", "2023-05-19 21:21:21.500000"],
                "exposure.obs_start_mjd": [60083.847453703704, 60083.9],
                "day_obs": [20230519, 20230519],
                "seq_num": [0, 1],
            },
        )

    def test_rows_with_nan_are_dropped(self):
        rows = {
            "exposure.obs_start": [datetime.datetime(2023, 5, 19, n) for n in range(4)],
            "exposure.ra": [1.0, float("nan"), 3.0, float("inf")],
            "day_obs": [20230519] * 4,
            "seq_num": [0, 1, 2, 3],
        }
        content = json.loads(self.load(rows))["content"]
        self.assertEqual(content["data"]["seq_num"], [0, 2])
        self.assertEqual(content["data"]["exposure.ra"], [1.0, 3.0])
        self.assertEqual(
            content["data"]["exposure.obs_start"], ["2023-05-19 00:00:00", "2023-05-19 02:00:00"]
        )

    def test_aggregate_nan_is_sent_as_null(self):
        with patch.object(self.database, "fetch_data", return_value={"avg_1": [float("nan")]}):
            reply = lras.command.execute_command(
                json.dumps(
                    {
                        "name": "load columns",
                        "parameters": {"database": "testdb", "columns": ["exposure.ra"], "aggregator": "avg"},
                    }
                ),
                self.data_center,
            )
        self.assertNotIn("NaN", reply)
        self.assertEqual(json.loads(reply)["content"]["data"], {"exposure.ra": None})

    def test_unserializable_result_is_a_response_error(self):
        reply = json.loads(self.load({"exposure.obs_start": [object()]}))
        self.assertEqual(reply["type"], "error")
        self.assertEqual(reply["content"]["error"], "command response error")
        self.assertEqual(reply["requestId"], "r1")

    def test_nan_cannot_reach_the_client(self):
        # allow_nan=False is the backstop should a value ever escape
        # to_json_safe: the reply is an error, never invalid JSON.
        command = lras.command.BaseCommand.command_registry["load columns"](
            database="testdb", columns=["exposure.ra"]
        )
        command.result = {"type": "table columns", "content": {"data": {"exposure.ra": [1.0]}}}
        with patch("lsst.rubintv.analysis.service.command.to_json_safe", side_effect=lambda value: value):
            command.result["content"]["data"]["exposure.ra"] = [math.nan]
            with self.assertRaises(ValueError):
                command.to_json()
