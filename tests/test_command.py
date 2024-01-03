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
import os
import tempfile
from typing import cast

import astropy.table
import lsst.rubintv.analysis.service as lras
import lsst.rubintv.analysis.service.database
import sqlalchemy
import utils
import yaml


class TestCommand(utils.RasTestCase):
    def setUp(self):
        path = os.path.dirname(__file__)
        yaml_filename = os.path.join(path, "schema.yaml")

        with open(yaml_filename) as file:
            schema = yaml.safe_load(file)
        db_file = tempfile.NamedTemporaryFile(delete=False)
        utils.create_database(schema, db_file.name)
        self.db_file = db_file
        self.db_filename = db_file.name

        # Load the database connection information
        databases = {
            "testdb": lsst.rubintv.analysis.service.database.DatabaseConnection(
                schema=schema, engine=sqlalchemy.create_engine("sqlite:///" + db_file.name)
            )
        }

        self.data_center = lras.data.DataCenter(databases=databases)

    def tearDown(self) -> None:
        self.db_file.close()
        os.remove(self.db_file.name)

    def execute_command(self, command: dict, response_type: str) -> dict:
        command_json = json.dumps(command)
        response = lras.command.execute_command(command_json, self.data_center)
        result = json.loads(response)
        self.assertEqual(result["type"], response_type)
        return result["content"]


class TestCalculateBoundsCommand(TestCommand):
    def test_calculate_bounds_command(self):
        command = {
            "name": "get bounds",
            "parameters": {
                "database": "testdb",
                "column": "Visit.dec",
            },
        }
        print(lras.command.BaseCommand.command_registry)
        content = self.execute_command(command, "column bounds")
        self.assertEqual(content["column"], "Visit.dec")
        self.assertListEqual(content["bounds"], [-40, 50])


class TestLoadColumnsCommand(TestCommand):
    def test_load_full_columns(self):
        command = {
            "name": "load columns",
            "parameters": {
                "database": "testdb",
                "columns": [
                    "Visit.ra",
                    "Visit.dec",
                ],
            },
        }

        content = self.execute_command(command, "table columns")
        columns = content["columns"]
        data = content["data"]

        truth = cast(astropy.table.Table, utils.get_test_data("Visit")["ra", "dec", "day_obs", "seq_num", "instrument"])
        valid = (truth["ra"] != None) & (truth["dec"] != None)
        truth = cast(astropy.table.Table, truth[valid])
        truth_data = utils.ap_table_to_list(truth)
        truth_data = truth_data

        self.assertTupleEqual(tuple(columns), tuple(truth.columns))
        self.assertDataTableEqual(data, truth_data)

    def test_load_columns_with_query(self):
        command = {
            "name": "load columns",
            "parameters": {
                "database": "testdb",
                "columns": [
                    "ExposureInfo.exposure_id",
                    "Visit.ra",
                    "Visit.dec",
                ],
                "query": {
                    "name": "EqualityQuery",
                    "content": {
                        "column": "ExposureInfo.expTime",
                        "operator": "eq",
                        "value": 30,
                    },
                },
            },
        }

        content = self.execute_command(command, "table columns")
        columns = content["columns"]
        data = content["data"]

        visit_truth = utils.get_test_data("Visit")
        exp_truth = utils.get_test_data("ExposureInfo")
        truth = astropy.table.join(visit_truth, exp_truth, keys=("seq_num", "day_obs", "instrument"))
        truth = truth["exposure_id", "ra", "dec", "day_obs", "seq_num", "instrument"]

        # Select rows with expTime = 30
        truth = truth[[True, True, False, False, False, True, False, False, False, False]]
        truth_data = utils.ap_table_to_list(truth)

        self.assertTupleEqual(tuple(columns), tuple(truth.columns))
        self.assertDataTableEqual(data, truth_data)


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
