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

import astropy.table
import lsst.rubintv.analysis.service as lras
import sqlalchemy
import utils


class TestQuery(utils.RasTestCase):
    def test_equality(self):
        query_table = self.database.tables["exposure"]
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
            result = lras.query.EqualityQuery("exposure.dec", operator, value)(self.database)
            self.assertTrue(result.result.compare(truth))
            self.assertSetEqual(
                result.tables,
                {
                    "exposure",
                },
            )

    def test_query(self):
        dec_column = self.database.tables["exposure"].columns.dec
        ra_column = self.database.tables["exposure"].columns.ra
        # dec > 0
        query = lras.query.EqualityQuery("exposure.dec", "gt", 0)
        result = query(self.database)
        self.assertTrue(result.result.compare(dec_column > 0))

        # dec < 0 and ra > 60
        query = lras.query.ParentQuery(
            operator="AND",
            children=[
                lras.query.EqualityQuery("exposure.dec", "lt", 0),
                lras.query.EqualityQuery("exposure.ra", "gt", 60),
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
        data = utils.get_test_data("exposure")

        # dec > 0 (and is not None)
        query1 = {
            "type": "EqualityQuery",
            "field": {
                "schema": "exposure",
                "name": "dec",
            },
            "leftOperator": "lt",
            "leftValue": 0,
        }
        # ra > 60 (and is not None)
        query2 = {
            "type": "EqualityQuery",
            "field": {
                "schema": "exposure",
                "name": "ra",
            },
            "leftOperator": "lt",
            "leftValue": 60,
        }

        # Test 1: dec > 0 (and is not None)
        query = query1
        result = self.database.query(["exposure.ra", "exposure.dec"], query=lras.query.Query.from_dict(query))
        truth = data[[False, False, False, False, False, True, False, False, True, True]]
        truth = truth[
            "exposure.ra",
            "exposure.dec",
            "exposure.day_obs",
            "exposure.seq_num",
        ]  # type: ignore
        self.assertDataTableEqual(result, truth)  # type:ignore

        # Test 2: dec > 0 and ra > 60 (and neither is None)
        query = {
            "type": "ParentQuery",
            "operator": "AND",
            "children": [query1, query2],
        }
        result = self.database.query(["exposure.ra", "exposure.dec"], query=lras.query.Query.from_dict(query))
        truth = data[[False, False, False, False, False, False, False, False, True, True]]
        truth = truth[
            "exposure.ra",
            "exposure.dec",
            "exposure.day_obs",
            "exposure.seq_num",
        ]  # type: ignore
        self.assertDataTableEqual(result, truth)  # type:ignore

        # Test 3: dec <= 0 or ra > 60 (and neither is None)
        query = {
            "type": "ParentQuery",
            "operator": "OR",
            "children": [
                {
                    "type": "ParentQuery",
                    "operator": "NOT",
                    "children": [query1],
                },
                query2,
            ],
        }

        result = self.database.query(["exposure.ra", "exposure.dec"], query=lras.query.Query.from_dict(query))
        truth = data[[True, True, False, True, True, False, False, False, True, True]]
        truth = truth[
            "exposure.ra",
            "exposure.dec",
            "exposure.day_obs",
            "exposure.seq_num",
        ]  # type: ignore
        self.assertDataTableEqual(result, truth)  # type:ignore

        # Test 4: dec > 0 XOR ra > 60
        query = {
            "type": "ParentQuery",
            "operator": "XOR",
            "children": [query1, query2],
        }
        result = self.database.query(["exposure.ra", "exposure.dec"], query=lras.query.Query.from_dict(query))
        truth = data[[False, False, False, False, False, True, False, False, False, False]]
        truth = truth[
            "exposure.ra",
            "exposure.dec",
            "exposure.day_obs",
            "exposure.seq_num",
        ]  # type: ignore
        self.assertDataTableEqual(result, truth)  # type:ignore

    def test_database_string_query(self):
        data = utils.get_test_data("exposure")

        # Test equality
        query = {
            "type": "EqualityQuery",
            "field": {
                "schema": "exposure",
                "name": "physical_filter",
            },
            "rightOperator": "eq",
            "rightValue": "DECam r-band",
        }
        result = self.database.query(["exposure.physical_filter"], query=lras.query.Query.from_dict(query))
        truth = data[[False, False, False, False, False, False, True, False, False, False]]
        truth = truth[
            "exposure.physical_filter",
            "exposure.day_obs",
            "exposure.seq_num",
        ]  # type: ignore
        self.assertDataTableEqual(result, truth)  # type:ignore

        # Test "startswith"
        query = {
            "type": "EqualityQuery",
            "field": {
                "schema": "exposure",
                "name": "physical_filter",
            },
            "rightOperator": "startswith",
            "rightValue": "DECam",
        }
        result = self.database.query(["exposure.physical_filter"], query=lras.query.Query.from_dict(query))
        truth = data[[False, False, False, False, False, True, True, True, True, True]]
        truth = truth[
            "exposure.physical_filter",
            "exposure.day_obs",
            "exposure.seq_num",
        ]  # type: ignore
        self.assertDataTableEqual(result, truth)  # type:ignore

        # Test "endswith"
        query = {
            "type": "EqualityQuery",
            "field": {
                "schema": "exposure",
                "name": "physical_filter",
            },
            "rightOperator": "endswith",
            "rightValue": "r-band",
        }
        result = self.database.query(["exposure.physical_filter"], query=lras.query.Query.from_dict(query))
        truth = data[[False, True, False, False, False, False, True, False, False, False]]
        truth = truth[
            "exposure.physical_filter",
            "exposure.day_obs",
            "exposure.seq_num",
        ]  # type: ignore
        self.assertDataTableEqual(result, truth)  # type:ignore

        # Test "like"
        query = {
            "type": "EqualityQuery",
            "field": {
                "schema": "exposure",
                "name": "physical_filter",
            },
            "rightOperator": "contains",
            "rightValue": "T r",
        }
        result = self.database.query(["exposure.physical_filter"], query=lras.query.Query.from_dict(query))
        truth = data[[False, True, False, False, False, False, False, False, False, False]]
        truth = truth[
            "exposure.physical_filter",
            "exposure.day_obs",
            "exposure.seq_num",
        ]  # type: ignore
        self.assertDataTableEqual(result, truth)  # type:ignore

    def test_database_datatime_query(self):
        data = utils.get_test_data("exposure")

        # Test <
        query1 = {
            "type": "EqualityQuery",
            "field": {
                "schema": "exposure",
                "name": "obs_start",
            },
            "rightOperator": "lt",
            "rightValue": "2023-05-19 23:23:23",
        }
        result = self.database.query(["exposure.obs_start"], query=lras.query.Query.from_dict(query1))
        truth = data[[True, True, True, False, False, True, True, True, True, True]]
        truth = truth[
            "exposure.obs_start",
            "exposure.day_obs",
            "exposure.seq_num",
        ]  # type: ignore
        self.assertDataTableEqual(result, truth)  # type:ignore

        # Test >
        query2 = {
            "type": "EqualityQuery",
            "field": {
                "schema": "exposure",
                "name": "obs_start",
            },
            "leftOperator": "lt",
            "leftValue": "2023-05-01 23:23:23",
        }
        result = self.database.query(["exposure.obs_start"], query=lras.query.Query.from_dict(query2))
        truth = data[[True, True, True, True, True, False, False, False, False, False]]
        truth = truth[
            "exposure.obs_start",
            "exposure.day_obs",
            "exposure.seq_num",
        ]  # type: ignore
        self.assertDataTableEqual(result, truth)  # type:ignore

        # Test in range
        query3 = {
            "type": "ParentQuery",
            "operator": "AND",
            "children": [query1, query2],
        }
        result = self.database.query(["exposure.obs_start"], query=lras.query.Query.from_dict(query3))
        truth = data[[True, True, True, False, False, False, False, False, False, False]]
        truth = truth[
            "exposure.obs_start",
            "exposure.day_obs",
            "exposure.seq_num",
        ]  # type: ignore
        self.assertDataTableEqual(result, truth)  # type:ignore

    def test_multiple_table_query(self):
        visit_truth = utils.get_test_data("exposure")
        exp_truth = utils.get_test_data("visit1_quicklook")
        truth = astropy.table.join(
            visit_truth,
            exp_truth,
            keys_left=("exposure.exposure_id",),
            keys_right=("visit1_quicklook.visit_id",),
        )

        # dec > 0 (and is not None)
        query1 = {
            "type": "EqualityQuery",
            "field": {
                "schema": "exposure",
                "name": "dec",
            },
            "leftOperator": "lt",
            "leftValue": 0,
        }
        # exposure time == 30 (and is not None)
        query2 = {
            "type": "EqualityQuery",
            "field": {
                "schema": "visit1_quicklook",
                "name": "exp_time",
            },
            "rightOperator": "eq",
            "rightValue": 30,
        }
        # Intersection of the two queries
        query3 = {
            "type": "ParentQuery",
            "operator": "AND",
            "children": [query1, query2],
        }

        valid = (
            (truth["exposure.dec"] != None)  # noqa: E711
            & (truth["exposure.ra"] != None)  # noqa: E711
            & (truth["visit1_quicklook.visit_id"] != None)  # noqa: E711
        )
        truth = truth[valid]
        valid = (truth["exposure.dec"] > 0) & (truth["visit1_quicklook.exp_time"] == 30)
        truth = truth[valid]
        truth = truth[
            "exposure.dec",
            "exposure.ra",
            "visit1_quicklook.visit_id",
            "exposure.day_obs",
            "exposure.seq_num",
        ]

        result = self.database.query(
            columns=["exposure.ra", "exposure.dec", "visit1_quicklook.visit_id"],
            query=lras.query.Query.from_dict(query3),
        )

        self.assertDataTableEqual(result, truth)
