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

import os
import tempfile
from unittest import TestCase

import lsst.rubintv.analysis.service as lras
import sqlalchemy
import utils
import yaml


class TestDatabase(TestCase):
    def setUp(self):
        path = os.path.dirname(__file__)
        yaml_filename = os.path.join(path, "schema.yaml")

        with open(yaml_filename) as file:
            schema = yaml.safe_load(file)
        db_file = tempfile.NamedTemporaryFile(delete=False)
        utils.create_database(schema, db_file.name)
        self.db_file = db_file
        self.db_filename = db_file.name
        self.schema = schema

        # Set up the sqlalchemy connection
        self.engine = sqlalchemy.create_engine("sqlite:///" + db_file.name)

    def tearDown(self) -> None:
        self.db_file.close()
        os.remove(self.db_file.name)

    def test_get_table_names(self):
        table_names = lras.database.get_table_names(self.schema)
        self.assertTupleEqual(table_names, ("ExposureInfo",))

    def test_get_table_schema(self):
        schema = lras.database.get_table_schema(self.schema, "ExposureInfo")
        self.assertEqual(schema["name"], "ExposureInfo")

        columns = [
            "exposure_id",
            "seq_num",
            "ra",
            "dec",
            "expTime",
            "physical_filter",
            "obsNight",
            "obsStart",
            "obsStartMJD",
        ]
        for n, column in enumerate(schema["columns"]):
            self.assertEqual(column["name"], columns[n])

    def test_query_full_table(self):
        truth_table = utils.get_test_data()
        truth = utils.ap_table_to_list(truth_table)

        data = lras.database.query_table("ExposureInfo", engine=self.engine)
        print(data)

        self.assertListEqual(list(data[0]._fields), list(truth_table.columns))

        for n in range(len(truth)):
            true_row = tuple(truth[n])
            row = tuple(data[n])
            self.assertTupleEqual(row, true_row)

    def test_query_columns(self):
        truth = utils.get_test_data()
        truth = utils.ap_table_to_list(truth["ra", "dec"])

        data = lras.database.query_table("ExposureInfo", columns=["ra", "dec"], engine=self.engine)

        self.assertListEqual(list(data[0]._fields), ["ra", "dec"])

        for n in range(len(truth)):
            true_row = tuple(truth[n])
            row = tuple(data[n])
            self.assertTupleEqual(row, true_row)

    def test_calculate_bounds(self):
        result = lras.database.calculate_bounds("ExposureInfo", "dec", self.engine)
        self.assertTupleEqual(result, (-40, 50))
