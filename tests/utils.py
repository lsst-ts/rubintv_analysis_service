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
import sqlite3
import tempfile
from unittest import TestCase

import numpy as np
import sqlalchemy
import yaml
from astropy.table import Table as ApTable
from astropy.time import Time
from lsst.rubintv.analysis.service.data import DataCenter
from lsst.rubintv.analysis.service.database import ConsDbSchema

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

# Convert DataID columns
dataid_transform = {
    "exposure.day_obs": "day_obs",
    "exposure.seq_num": "seq_num",
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


def get_exposure_data_dict(table_name: str, id_name: str) -> dict:
    """Get a dictionary containing the visit test data"""

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
        f"{table_name}.{id_name}": [2, 4, 6, 8, 10, 12, 14, 16, 18, 20],
        f"{table_name}.seq_num": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
        f"{table_name}.day_obs": [
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
        f"{table_name}.ra": [10, 20, None, 40, 50, 60, 70, None, 90, 100],
        f"{table_name}.dec": [-40, -30, None, -10, 0, 10, None, 30, 40, 50],
        f"{table_name}.physical_filter": [
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
        f"{table_name}.obs_start": obs_start,
        f"{table_name}.obs_start_mjd": obs_start_mjd,
    }


def get_visit_data_dict() -> dict:
    """Get a dictionary containing the exposure test data"""
    return {
        "visit1_quicklook.visit_id": [2, 4, 6, 8, 10, 12, 14, 16, 18, 20],
        "visit1_quicklook.exp_time": [30, 30, 10, 15, 15, 30, 30, 30, 15, 20],
        "visit1_quicklook.empty_column": [None] * 10,
    }


def get_test_data(table: str) -> ApTable:
    """Generate data for the test database"""
    if table == "exposure":
        data_dict = get_exposure_data_dict("exposure", "exposure_id")
    else:
        data_dict = get_visit_data_dict()

    return ApTable(list(data_dict.values()), names=list(data_dict.keys()))


def ap_table_to_list(data: ApTable) -> list:
    """Convert an astropy Table into a list of tuples."""
    rows = []
    for row in data:
        rows.append(tuple(row))  # type: ignore
    return rows


def create_database(schema: dict, db_filename: str):
    """Create the test database"""

    for table in schema["tables"]:
        connection = sqlite3.connect(db_filename)
        cursor = connection.cursor()

        create_table(cursor, table["name"], table["columns"])

        if table["name"] == "exposure":
            data = get_exposure_data_dict("exposure", "exposure_id")
            index_key = "exposure.exposure_id"
        elif table["name"] == "visit1":
            data = get_exposure_data_dict("visit1", "visit_id")
            index_key = "visit1.visit_id"
        elif table["name"] == "visit1_quicklook":
            data = get_visit_data_dict()
            index_key = "visit1_quicklook.visit_id"
        else:
            raise ValueError(f"Unknown table name: {table['name']}")

        for n in range(len(data[index_key])):
            row = tuple(data[key][n] for key in data.keys())
            value_str = "?, " * (len(row) - 1) + "?"
            command = f"INSERT INTO {table['name']} VALUES({value_str});"
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

    def setUp(self):
        # Load the testdb schema
        path = os.path.dirname(__file__)
        yaml_filename = os.path.join(path, "schema.yaml")

        with open(yaml_filename) as file:
            schema = yaml.safe_load(file)

        # Remove the name of the schema, since sqlite does not have
        # schema names and this will break the code otherwise.
        schema["name"] = None

        # Create the sqlite test database
        db_file = tempfile.NamedTemporaryFile(delete=False)
        create_database(schema, db_file.name)
        self.db_file = db_file
        self.db_filename = db_file.name
        self.schema = schema

        # Set up the sqlalchemy connection
        engine = sqlalchemy.create_engine("sqlite:///" + db_file.name)

        # Load the table joins
        joins_path = os.path.join(path, "joins.yaml")
        with open(joins_path) as file:
            joins = yaml.safe_load(file)["joins"]

        # Create the datacenter
        self.database = ConsDbSchema(schema=schema, engine=engine, join_templates=joins)
        self.data_center = DataCenter(schemas={"testdb": self.database}, user_path="")

    def tearDown(self) -> None:
        self.db_file.close()
        os.remove(self.db_file.name)

    def assertDataTableEqual(self, result: dict | ApTable, truth: ApTable):  # NOQA: N802
        """Check if two data tables are equal.

        Parameters
        ----------
        result :
            The result generated by the test that is checked.
        truth :
            The expected value of the test.
        """
        columns = truth.colnames
        for column in columns:
            result_column = column
            if column not in result:
                if column in dataid_transform:
                    result_column = dataid_transform[column]
                else:
                    msg = f"Column {column} not found in result"
                    raise TableMismatchError(msg)
            np.testing.assert_array_equal(np.array(result[result_column]), np.array(truth[column]))
