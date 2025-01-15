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

import json
from typing import cast

import astropy.table
import lsst.rubintv.analysis.service as lras
import pytest
import utils


class TestCommand(utils.RasTestCase):
    def execute_command(self, command: dict, response_type: str) -> dict:
        command_json = json.dumps(command)
        response = lras.command.execute_command(command_json, self.data_center)
        result = json.loads(response)
        self.assertEqual(result["type"], response_type)
        return result["content"]


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
