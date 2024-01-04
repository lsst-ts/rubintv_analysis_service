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

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from .command import BaseCommand


if TYPE_CHECKING:
    from .data import DataCenter

@dataclass(kw_only=True)
class LoadColumnsCommand(BaseCommand):
    """Load columns from a database table with an optional query.

    Attributes
    ----------
    database :
        The name of the database that the table is in.
    columns :
        Columns that are to be loaded.
        This should be a string with the format `table.columnName`.
        If there is only a single entry and it does not contain a `.`,
        then the table name is used and all of the columns matching the
        `query` are loaded.
    query :
        Query used to select rows in the table.
        If `query` is ``None`` then all the rows are loaded.
    """

    database: str
    columns: list[str]
    query: dict | None = None
    response_type: str = "table columns"

    def build_contents(self, data_center: DataCenter) -> dict:
        # Query the database to return the requested columns
        database = data_center.databases[self.database]
        data = database.query(self.columns, self.query)

        if not data:
            # There is no column data to return
            content: dict = {
                "columns": self.columns,
                "data": [],
            }
        else:
            content = {
                "columns": [column for column in data[0]._fields],
                "data": [list(row) for row in data],
            }

        return content


@dataclass(kw_only=True)
class CalculateBoundsCommand(BaseCommand):
    """Calculate the bounds of a table column.

    Attributes
    ----------
    database :
        The name of the database that the table is in.
    column :
        The column to calculate the bounds of in the format "table.column".
    """

    database: str
    column: str
    response_type: str = "column bounds"

    def build_contents(self, data_center: DataCenter) -> dict:
        # Query the database to return the requested columns
        database = data_center.databases[self.database]
        data = database.calculate_bounds(
            column=self.column,
        )
        return {
            "column": self.column,
            "bounds": data,
        }


@dataclass(kw_only=True)
class LoadSchemaCommand(BaseCommand):
    """Load the schema for a database.

    Attributes
    ----------
    database :
        The name of the database that the table is in.
    """

    database: str
    response_type: str = "database schema"

    def build_contents(self, data_center: DataCenter) -> dict:
        database = data_center.databases[self.database]
        return database.schema


# Register the commands
LoadColumnsCommand.register("load columns")
CalculateBoundsCommand.register("get bounds")
LoadSchemaCommand.register("load schema")
