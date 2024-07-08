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
