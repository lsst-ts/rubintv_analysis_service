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
                "Visit",
                "ExposureInfo",
            ),
        )

    def test_get_table_schema(self):
        schema = lras.database.get_table_schema(self.database.schema, "ExposureInfo")
        self.assertEqual(schema["name"], "ExposureInfo")

        columns = [
            "seq_num",
            "day_obs",
            "instrument",
            "exposure_id",
            "expTime",
            "physical_filter",
            "obsStart",
            "obsStartMJD",
        ]
        for n, column in enumerate(schema["columns"]):
            self.assertEqual(column["name"], columns[n])

    def test_single_table_query_columns(self):
        truth = utils.get_test_data("Visit")
        valid = (truth["ra"] != None) & (truth["dec"] != None)  # noqa: E711
        truth = truth[valid]
        truth = utils.ap_table_to_list(truth["ra", "dec", "day_obs", "seq_num", "instrument"])  # type: ignore

        data = self.database.query(columns=["Visit.ra", "Visit.dec"])

        self.assertListEqual(list(data[0]._fields), ["ra", "dec", "day_obs", "seq_num", "instrument"])

        for n in range(len(truth)):
            true_row = tuple(truth[n])
            row = tuple(data[n])
            self.assertTupleEqual(row, true_row)

    def test_multiple_table_query_columns(self):
        visit_truth = utils.get_test_data("Visit")
        exp_truth = utils.get_test_data("ExposureInfo")
        truth = astropy.table.join(visit_truth, exp_truth, keys=("seq_num", "day_obs", "instrument"))
        valid = (truth["ra"] != None) & (truth["dec"] != None)  # noqa: E711
        truth = truth[valid]
        truth = utils.ap_table_to_list(truth["ra", "dec", "day_obs", "seq_num", "instrument", "exposure_id"])

        data = self.database.query(columns=["Visit.ra", "Visit.dec", "ExposureInfo.exposure_id"])

        self.assertListEqual(
            list(data[0]._fields), ["ra", "dec", "exposure_id", "day_obs", "seq_num", "instrument"]
        )

        for n in range(len(truth)):
            true_row = tuple(truth[n])
            row = tuple(data[n])
            self.assertTupleEqual(row, true_row)

    def test_calculate_bounds(self):
        result = self.database.calculate_bounds("Visit", "dec")
        self.assertTupleEqual(result, (-40, 50))
