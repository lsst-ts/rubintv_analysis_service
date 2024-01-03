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

import astropy.table
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
        self.database = lras.database.DatabaseConnection(schema=schema, engine=self.engine)

    def tearDown(self) -> None:
        self.db_file.close()
        os.remove(self.db_file.name)

    def test_equality(self):
        query_table = self.database.tables["Visit"]
        query_column = query_table.columns.dec

        value = 0
        truth_dict = {
            "eq": query_column == value,
            "ne": query_column != value,
            "lt": query_column < value,
            "le": query_column <= value,
            "gt": query_column > value,
            "ge": query_column >= value,
        }

        for operator, truth in truth_dict.items():
            result = lras.query.EqualityQuery("Visit.dec", operator, value)(self.database)
            self.assertTrue(result.result.compare(truth))
            self.assertSetEqual(result.tables, {"Visit",})

    def test_query(self):
        dec_column = self.database.tables["Visit"].columns.dec
        ra_column = self.database.tables["Visit"].columns.ra
        # dec > 0
        query = lras.query.EqualityQuery("Visit.dec", "gt", 0)
        result = query(self.database)
        self.assertTrue(result.result.compare(dec_column > 0))

        # dec < 0 and ra > 60
        query = lras.query.ParentQuery(
            operator="AND",
            children=[
                lras.query.EqualityQuery("Visit.dec", "lt", 0),
                lras.query.EqualityQuery("Visit.ra", "gt", 60),
            ],
        )
        result = query(self.database)
        truth = sqlalchemy.and_(
            dec_column < 0,
            ra_column > 60,
        )
        self.assertTrue(result.result.compare(truth))

        # Check queries that are unequal to verify that they don't work
        result = query(self.database)
        truth = sqlalchemy.and_(
            dec_column < 0,
            ra_column > 70,
        )
        self.assertFalse(result.result.compare(truth))

    def test_database_query(self):
        data = utils.get_test_data("Visit")

        # dec > 0 (and is not None)
        query1 = {
            "name": "EqualityQuery",
            "content": {
                "column": "Visit.dec",
                "operator": "gt",
                "value": 0,
            },
        }
        # ra > 60 (and is not None)
        query2 = {
            "name": "EqualityQuery",
            "content": {
                "column": "Visit.ra",
                "operator": "gt",
                "value": 60,
            },
        }

        # Test 1: dec > 0 (and is not None)
        query = query1
        result = self.database.query(["Visit.ra", "Visit.dec"], query=query)
        truth = data[[False, False, False, False, False, True, False, False, True, True]]
        truth = truth["ra", "dec", "day_obs", "seq_num", "instrument"]  # type: ignore
        truth = utils.ap_table_to_list(truth)  # type:ignore
        self.assertDataTableEqual(result, truth)

        # Test 2: dec > 0 and ra > 60 (and neither is None)
        query = {
            "name": "ParentQuery",
            "content": {
                "operator": "AND",
                "children": [query1, query2],
            },
        }
        result = self.database.query(["Visit.ra", "Visit.dec"], query=query)
        truth = data[[False, False, False, False, False, False, False, False, True, True]]
        truth = truth["ra", "dec", "day_obs", "seq_num", "instrument"]  # type: ignore
        truth = utils.ap_table_to_list(truth)  # type:ignore
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

        result = self.database.query(["Visit.ra", "Visit.dec"], query=query)
        truth = data[[True, True, False, True, True, False, False, False, True, True]]
        truth = truth["ra", "dec", "day_obs", "seq_num", "instrument"]  # type: ignore
        truth = utils.ap_table_to_list(truth)  # type:ignore
        self.assertDataTableEqual(result, truth)

        # Test 4: dec > 0 XOR ra > 60
        query = {
            "name": "ParentQuery",
            "content": {
                "operator": "XOR",
                "children": [query1, query2],
            },
        }
        result = self.database.query(["Visit.ra", "Visit.dec"], query=query)
        truth = data[[False, False, False, False, False, True, False, False, False, False]]
        truth = truth["ra", "dec", "day_obs", "seq_num", "instrument"]  # type: ignore
        truth = utils.ap_table_to_list(truth)  # type:ignore
        self.assertDataTableEqual(result, truth)

    def test_database_string_query(self):
        data = utils.get_test_data("ExposureInfo")

        # Test equality
        query = {
            "name": "EqualityQuery",
            "content": {
                "column": "ExposureInfo.physical_filter",
                "operator": "eq",
                "value": "DECam r-band",
            },
        }
        result = self.database.query(["ExposureInfo.physical_filter"], query=query)
        truth = data[[False, False, False, False, False, False, True, False, False, False]]
        truth = truth["physical_filter", "day_obs", "seq_num", "instrument"]  # type: ignore
        truth = utils.ap_table_to_list(truth)  # type:ignore
        self.assertDataTableEqual(result, truth)

        # Test "startswith"
        query = {
            "name": "EqualityQuery",
            "content": {
                "column": "ExposureInfo.physical_filter",
                "operator": "startswith",
                "value": "DECam",
            },
        }
        result = self.database.query(["ExposureInfo.physical_filter"], query=query)
        truth = data[[False, False, False, False, False, True, True, True, True, True]]
        truth = truth["physical_filter", "day_obs", "seq_num", "instrument"]  # type: ignore
        truth = utils.ap_table_to_list(truth)  # type:ignore
        self.assertDataTableEqual(result, truth)

        # Test "endswith"
        query = {
            "name": "EqualityQuery",
            "content": {
                "column": "ExposureInfo.physical_filter",
                "operator": "endswith",
                "value": "r-band",
            },
        }
        result = self.database.query(["ExposureInfo.physical_filter"], query=query)
        truth = data[[False, True, False, False, False, False, True, False, False, False]]
        truth = truth["physical_filter", "day_obs", "seq_num", "instrument"]  # type: ignore
        truth = utils.ap_table_to_list(truth)  # type:ignore
        self.assertDataTableEqual(result, truth)

        # Test "like"
        query = {
            "name": "EqualityQuery",
            "content": {
                "column": "ExposureInfo.physical_filter",
                "operator": "contains",
                "value": "T r",
            },
        }
        result = self.database.query(["ExposureInfo.physical_filter"], query=query)
        truth = data[[False, True, False, False, False, False, False, False, False, False]]
        truth = truth["physical_filter", "day_obs", "seq_num", "instrument"]  # type: ignore
        truth = utils.ap_table_to_list(truth)  # type:ignore
        self.assertDataTableEqual(result, truth)

    def test_database_datatime_query(self):
        data = utils.get_test_data("ExposureInfo")

        # Test <
        query1 = {
            "name": "EqualityQuery",
            "content": {
                "column": "ExposureInfo.obsStart",
                "operator": "lt",
                "value": "2023-05-19 23:23:23",
            },
        }
        result = self.database.query(["ExposureInfo.obsStart"], query=query1)
        truth = data[[True, True, True, False, False, True, True, True, True, True]]
        truth = truth["obsStart", "day_obs", "seq_num", "instrument"]  # type: ignore
        truth = utils.ap_table_to_list(truth)  # type:ignore
        self.assertDataTableEqual(result, truth)

        # Test >
        query2 = {
            "name": "EqualityQuery",
            "content": {
                "column": "ExposureInfo.obsStart",
                "operator": "gt",
                "value": "2023-05-01 23:23:23",
            },
        }
        result = self.database.query(["ExposureInfo.obsStart"], query=query2)
        truth = data[[True, True, True, True, True, False, False, False, False, False]]
        truth = truth["obsStart", "day_obs", "seq_num", "instrument"]  # type: ignore
        truth = utils.ap_table_to_list(truth)  # type:ignore
        self.assertDataTableEqual(result, truth)

        # Test in range
        query3 = {
            "name": "ParentQuery",
            "content": {
                "operator": "AND",
                "children": [query1, query2],
            },
        }
        result = self.database.query(["ExposureInfo.obsStart"], query=query3)
        truth = data[[True, True, True, False, False, False, False, False, False, False]]
        truth = truth["obsStart", "day_obs", "seq_num", "instrument"]  # type: ignore
        truth = utils.ap_table_to_list(truth)  # type:ignore
        self.assertDataTableEqual(result, truth)

    def test_multiple_table_query(self):
        visit_truth = utils.get_test_data("Visit")
        exp_truth = utils.get_test_data("ExposureInfo")
        truth = astropy.table.join(visit_truth, exp_truth, keys=("seq_num", "day_obs", "instrument"))

        # dec > 0 (and is not None)
        query1 = {
            "name": "EqualityQuery",
            "content": {
                "column": "Visit.dec",
                "operator": "gt",
                "value": 0,
            },
        }
        # exposure time == 30 (and is not None)
        query2 = {
            "name": "EqualityQuery",
            "content": {
                "column": "ExposureInfo.expTime",
                "operator": "eq",
                "value": 30,
            },
        }
        # Intersection of the two queries
        query3 = {
            "name": "ParentQuery",
            "content": {
                "operator": "AND",
                "children": [query1, query2],
            },
        }

        valid = (truth["dec"] != None) & (truth["ra"] != None) & (truth["exposure_id"] != None)
        truth = truth[valid]
        valid = (truth["dec"] > 0) & (truth["expTime"] == 30)
        truth = truth[valid]
        truth = utils.ap_table_to_list(truth["ra", "dec", "exposure_id", "day_obs", "seq_num", "instrument"])

        result = self.database.query(
            columns=["Visit.ra", "Visit.dec", "ExposureInfo.exposure_id"],
            query=query3
        )

        self.assertDataTableEqual(result, truth)
