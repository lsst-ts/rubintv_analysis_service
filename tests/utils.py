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

import sqlite3
from unittest import TestCase

from astropy.table import Table as ApTable
from astropy.time import Time

# Convert visit DB datatypes to sqlite3 datatypes
datatype_transform = {
    "int": "integer",
    "long": "integer",
    "double": "real",
    "float": "real",
    "char": "text",
    "date": "text",
    "datetime": "text",
}


def create_table(cursor: sqlite3.Cursor, tbl_name: str, schema: dict):
    """Create a table in an sqlite database.

    Parameters
    ----------
    cursor :
        The cursor associated with the database connection.
    tbl_name :
        The name of the table to create.
    schema :
        The schema of the table.
    """
    command = f"CREATE TABLE {tbl_name}(\n"
    for field in schema:
        command += f'  {field["name"]} {datatype_transform[field["datatype"]]},\n'
    command = command[:-2] + "\n);"
    cursor.execute(command)


def get_test_data_dict() -> dict:
    """Get a dictionary containing the test data"""
    obs_start = [
        "2023-05-19 20:20:20",
        "2023-05-19 21:21:21",
        "2023-05-19 22:22:22",
        "2023-05-19 23:23:23",
        "2023-05-20 00:00:00",
        "2023-02-14 22:22:22",
        "2023-02-14 23:23:23",
        "2023-02-14 00:00:00",
        "2023-02-14 01:01:01",
        "2023-02-14 02:02:02",
    ]

    obs_start_mjd = [Time(time).mjd for time in obs_start]

    return {
        "exposure_id": [0, 2, 4, 6, 8, 10, 12, 14, 16, 18],
        "seq_num": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
        "ra": [10, 20, None, 40, 50, 60, 70, None, 90, 100],
        "dec": [-40, -30, None, -10, 0, 10, None, 30, 40, 50],
        "expTime": [30, 30, 10, 15, 15, 30, 30, 30, 15, 20],
        "physical_filter": [
            "LSST g-band",
            "LSST r-band",
            "LSST i-band",
            "LSST z-band",
            "LSST y-band",
            "DECam g-band",
            "DECam r-band",
            "DECam i-band",
            "DECam z-band",
            "DECam y-band",
        ],
        "obsNight": [
            "2023-05-19",
            "2023-05-19",
            "2023-05-19",
            "2023-05-19",
            "2023-05-19",
            "2023-02-14",
            "2023-02-14",
            "2023-02-14",
            "2023-02-14",
            "2023-02-14",
        ],
        "obsStart": obs_start,
        "obsStartMJD": obs_start_mjd,
    }


def get_test_data() -> ApTable:
    """Generate data for the test database"""
    data_dict = get_test_data_dict()

    table = ApTable(list(data_dict.values()), names=list(data_dict.keys()))
    return table


def ap_table_to_list(data: ApTable) -> list:
    """Convert an astropy Table into a list of tuples."""
    rows = []
    for row in data:
        rows.append(tuple(row))
    return rows


def create_database(schema: dict, db_filename: str):
    """Create the test database"""
    tbl_name = "ExposureInfo"
    connection = sqlite3.connect(db_filename)
    cursor = connection.cursor()

    create_table(cursor, tbl_name, schema["tables"][0]["columns"])

    data = get_test_data_dict()

    for n in range(len(data["exposure_id"])):
        row = tuple(data[key][n] for key in data.keys())
        value_str = "?, " * (len(row) - 1) + "?"
        command = f"INSERT INTO {tbl_name} VALUES({value_str});"
        cursor.execute(command, row)
    connection.commit()
    cursor.close()


class TableMismatchError(AssertionError):
    pass


class RasTestCase(TestCase):
    """Base class for tests in this package

    For now this only includes methods to check the
    database results, but in the future other checks
    might be put in place.
    """

    @staticmethod
    def get_data_table_indices(table: list[tuple]) -> list[int]:
        """Get the index for each rom in the data table.

        Parameters
        ----------
        table :
            The table containing the data.

        Returns
        -------
        result :
            The index for each row in the table.
        """
        # Return the seq_num as an index
        return [row[1] for row in table]

    def assertDataTableEqual(self, result, truth):
        """Check if two data tables are equal.

        Parameters
        ----------
        result :
            The result generated by the test that is checked.
        truth :
            The expected value of the test.
        """
        if len(result) != len(truth):
            msg = "Data tables have a different number of rows: "
            msg += f"indices: [{self.get_data_table_indices(result)}], [{self.get_data_table_indices(truth)}]"
            raise TableMismatchError(msg)
        try:
            for n in range(len(truth)):
                true_row = tuple(truth[n])
                row = tuple(result[n])
                self.assertTupleEqual(row, true_row)
        except AssertionError:
            msg = "Mismatched tables: "
            msg += f"indices: [{self.get_data_table_indices(result)}], [{self.get_data_table_indices(truth)}]"
            raise TableMismatchError(msg)
