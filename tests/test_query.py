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

import lsst.rubintv.analysis.service as lras
import sqlalchemy
import utils
import yaml


class TestQuery(utils.RasTestCase):
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
        self.metadata = sqlalchemy.MetaData()
        self.table = sqlalchemy.Table("ExposureInfo", self.metadata, autoload_with=self.engine)

    def tearDown(self) -> None:
        self.db_file.close()
        os.remove(self.db_file.name)

    def test_equality(self):
        table = self.table
        column = table.columns.dec

        value = 0
        truth_dict = {
            "eq": column == value,
            "ne": column != value,
            "lt": column < value,
            "le": column <= value,
            "gt": column > value,
            "ge": column >= value,
        }

        for operator, truth in truth_dict.items():
            self.assertTrue(lras.query.EqualityQuery("dec", operator, value)(table).compare(truth))

    def test_query(self):
        table = self.table

        # dec > 0
        query = lras.query.EqualityQuery("dec", "gt", 0)
        result = query(table)
        self.assertTrue(result.compare(table.columns.dec > 0))

        # dec < 0 and ra > 60
        query = lras.query.ParentQuery(
            operator="AND",
            children=[
                lras.query.EqualityQuery("dec", "lt", 0),
                lras.query.EqualityQuery("ra", "gt", 60),
            ],
        )
        result = query(table)
        truth = sqlalchemy.and_(
            table.columns.dec < 0,
            table.columns.ra > 60,
        )
        self.assertTrue(result.compare(truth))

        # Check queries that are unequal to verify that they don't work
        result = query(table)
        truth = sqlalchemy.and_(
            table.columns.dec < 0,
            table.columns.ra > 70,
        )
        self.assertFalse(result.compare(truth))

    def test_database_query(self):
        data = utils.get_test_data()

        # dec > 0 (and is not None)
        query1 = {
            "name": "EqualityQuery",
            "content": {
                "column": "dec",
                "operator": "gt",
                "value": 0,
            },
        }
        # ra > 60 (and is not None)
        query2 = {
            "name": "EqualityQuery",
            "content": {
                "column": "ra",
                "operator": "gt",
                "value": 60,
            },
        }

        # Test 1: dec > 0 (and is not None)
        query = query1
        result = lras.database.query_table("ExposureInfo", engine=self.engine, query=query)
        truth = data[[False, False, False, False, False, True, False, True, True, True]]
        truth = utils.ap_table_to_list(truth)
        self.assertDataTableEqual(result, truth)

        # Test 2: dec > 0 and ra > 60 (and neither is None)
        query = {
            "name": "ParentQuery",
            "content": {
                "operator": "AND",
                "children": [query1, query2],
            },
        }
        result = lras.database.query_table("ExposureInfo", engine=self.engine, query=query)
        truth = data[[False, False, False, False, False, False, False, False, True, True]]
        truth = utils.ap_table_to_list(truth)
        self.assertDataTableEqual(result, truth)

        # Test 3: dec <= 0 or ra > 60 (and neither is None)
        query = {
            "name": "ParentQuery",
            "content": {
                "operator": "OR",
                "children": [
                    {
                        "name": "ParentQuery",
                        "content": {
                            "operator": "NOT",
                            "children": [query1],
                        },
                    },
                    query2,
                ],
            },
        }

        result = lras.database.query_table("ExposureInfo", engine=self.engine, query=query)
        truth = data[[True, True, False, True, True, False, True, False, True, True]]
        truth = utils.ap_table_to_list(truth)
        self.assertDataTableEqual(result, truth)

        # Test 4: dec > 0 XOR ra > 60
        query = {
            "name": "ParentQuery",
            "content": {
                "operator": "XOR",
                "children": [query1, query2],
            },
        }
        result = lras.database.query_table("ExposureInfo", engine=self.engine, query=query)
        truth = data[[False, False, False, False, False, True, False, False, False, False]]
        truth = utils.ap_table_to_list(truth)
        self.assertDataTableEqual(result, truth)

    def test_database_string_query(self):
        data = utils.get_test_data()

        # Test equality
        query = {
            "name": "EqualityQuery",
            "content": {
                "column": "physical_filter",
                "operator": "eq",
                "value": "DECam r-band",
            },
        }
        result = lras.database.query_table("ExposureInfo", engine=self.engine, query=query)
        truth = data[[False, False, False, False, False, False, True, False, False, False]]
        truth = utils.ap_table_to_list(truth)
        self.assertDataTableEqual(result, truth)

        # Test "startswith"
        query = {
            "name": "EqualityQuery",
            "content": {
                "column": "physical_filter",
                "operator": "startswith",
                "value": "DECam",
            },
        }
        result = lras.database.query_table("ExposureInfo", engine=self.engine, query=query)
        truth = data[[False, False, False, False, False, True, True, True, True, True]]
        truth = utils.ap_table_to_list(truth)
        self.assertDataTableEqual(result, truth)

        # Test "endswith"
        query = {
            "name": "EqualityQuery",
            "content": {
                "column": "physical_filter",
                "operator": "endswith",
                "value": "r-band",
            },
        }
        result = lras.database.query_table("ExposureInfo", engine=self.engine, query=query)
        truth = data[[False, True, False, False, False, False, True, False, False, False]]
        truth = utils.ap_table_to_list(truth)
        self.assertDataTableEqual(result, truth)

        # Test "like"
        query = {
            "name": "EqualityQuery",
            "content": {
                "column": "physical_filter",
                "operator": "contains",
                "value": "T r",
            },
        }
        result = lras.database.query_table("ExposureInfo", engine=self.engine, query=query)
        truth = data[[False, True, False, False, False, False, False, False, False, False]]
        truth = utils.ap_table_to_list(truth)
        self.assertDataTableEqual(result, truth)

    def test_database_datatime_query(self):
        data = utils.get_test_data()

        # Test <
        query1 = {
            "name": "EqualityQuery",
            "content": {
                "column": "obsStart",
                "operator": "lt",
                "value": "2023-05-19 23:23:23",
            },
        }
        result = lras.database.query_table("ExposureInfo", engine=self.engine, query=query1)
        truth = data[[True, True, True, False, False, True, True, True, True, True]]
        truth = utils.ap_table_to_list(truth)
        self.assertDataTableEqual(result, truth)

        # Test >
        query2 = {
            "name": "EqualityQuery",
            "content": {
                "column": "obsStart",
                "operator": "gt",
                "value": "2023-05-01 23:23:23",
            },
        }
        result = lras.database.query_table("ExposureInfo", engine=self.engine, query=query2)
        truth = data[[True, True, True, True, True, False, False, False, False, False]]
        truth = utils.ap_table_to_list(truth)
        self.assertDataTableEqual(result, truth)

        # Test in range
        query3 = {
            "name": "ParentQuery",
            "content": {
                "operator": "AND",
                "children": [query1, query2],
            },
        }
        result = lras.database.query_table("ExposureInfo", engine=self.engine, query=query3)
        truth = data[[True, True, True, False, False, False, False, False, False, False]]
        truth = utils.ap_table_to_list(truth)
        self.assertDataTableEqual(result, truth)
