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
import utils


class TestDatabase(utils.RasTestCase):
    def test_get_table_names(self):
        table_names = self.database.get_table_names()
        self.assertTupleEqual(
            table_names,
            (
                "exposure",
                "visit1",
                "visit1_quicklook",
            ),
        )

    def test_get_table_schema(self):
        schema = lras.database.get_table_schema(self.database.schema, "exposure")
        self.assertEqual(schema["name"], "exposure")

        columns = [
            "exposure_id",
            "seq_num",
            "day_obs",
            "ra",
            "dec",
            "physical_filter",
            "obs_start",
            "obs_start_mjd",
        ]
        for n, column in enumerate(schema["columns"]):
            self.assertEqual(column["name"], columns[n])

    def test_single_table_query_columns(self):
        truth = utils.get_test_data("exposure")
        valid = (truth["exposure.ra"] != None) & (truth["exposure.dec"] != None)  # noqa: E711
        truth = truth[valid]
        truth = truth["exposure.ra", "exposure.dec", "exposure.day_obs", "exposure.seq_num"]
        data = self.database.query(columns=["exposure.ra", "exposure.dec"])
        self.assertDataTableEqual(data, truth)  # type: ignore

    def test_multiple_table_query_columns(self):
        visit_truth = utils.get_test_data("exposure")
        exp_truth = utils.get_test_data("visit1_quicklook")
        truth = astropy.table.join(
            visit_truth,
            exp_truth,
            keys_left=("exposure.exposure_id"),
            keys_right=("visit1_quicklook.visit_id"),
        )
        valid = (truth["exposure.ra"] != None) & (truth["exposure.dec"] != None)  # noqa: E711
        truth = truth[valid]
        truth = truth[
            "exposure.ra",
            "exposure.dec",
            "visit1_quicklook.visit_id",
            "exposure.day_obs",
            "exposure.seq_num",
        ]

        data = self.database.query(columns=["exposure.ra", "exposure.dec", "visit1_quicklook.visit_id"])

        self.assertDataTableEqual(data, truth)

    def test_calculate_bounds(self):
        result = self.database.calculate_bounds("exposure.dec")
        self.assertTupleEqual(result, (-40, 50))

    def test_columns_with_values(self):
        """The batched check must agree with the per-column one it replaces."""
        column_names = ["visit_id", "exp_time", "empty_column"]
        populated = self.database.columns_with_values("visit1_quicklook", column_names)

        self.assertEqual(populated, {"visit_id", "exp_time"})
        self.assertEqual(
            populated,
            {name for name in column_names if self.database.has_non_null_values(f"visit1_quicklook.{name}")},
        )

    def test_columns_with_values_unknown_table(self):
        self.assertEqual(self.database.columns_with_values("not_a_table", ["a"]), set())

    def test_verified_schema_drops_empty_columns(self):
        schema = self.database.get_verified_schema()

        columns = {
            f"{table['name']}.{column['name']}" for table in schema["tables"] for column in table["columns"]
        }
        self.assertIn("visit1_quicklook.visit_id", columns)
        self.assertNotIn("visit1_quicklook.empty_column", columns)

        # The schema the instance was built with must not be modified, since
        # it is shared with whoever constructed it.
        source_columns = {
            column["name"]
            for table in self.database.schema["tables"]
            if table["name"] == "visit1_quicklook"
            for column in table["columns"]
        }
        self.assertIn("empty_column", source_columns)

    def test_verified_schema_is_cached(self):
        first = self.database.get_verified_schema()
        self.assertIs(self.database.get_verified_schema(), first)

        # An explicit refresh recalculates, and still filters correctly.
        refreshed = self.database.refresh_verified_schema()
        self.assertIsNot(refreshed, first)
        self.assertEqual(
            {t["name"] for t in refreshed["tables"]},
            {t["name"] for t in first["tables"]},
        )

    def test_verified_schema_ttl_expires(self):
        database = lras.database.ConsDbSchema(
            engine=self.database.engine,
            schema=self.database.schema,
            join_templates=[],
            verified_schema_ttl=0,
        )
        first = database.get_verified_schema()
        # With no TTL every call recalculates rather than returning the cache.
        self.assertIsNot(database.get_verified_schema(), first)
