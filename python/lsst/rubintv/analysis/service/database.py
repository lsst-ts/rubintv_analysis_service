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

from dataclasses import dataclass
from typing import Sequence

import sqlalchemy
from lsst.daf.butler import Butler

from .command import BaseCommand, DatabaseConnection
from .query import Query


class UnrecognizedTableError(Exception):
    """An error that occurs when a table name does not appear in the schema"""

    pass


def get_table_names(schema: dict) -> tuple[str, ...]:
    """Given a schema, return a list of dataset names

    Parameters
    ----------
    schema :
        The schema for a database.

    Returns
    -------
    result :
        The names of all the tables in the database.
    """
    return tuple(tbl["name"] for tbl in schema["tables"])


def get_table_schema(schema: dict, table: str) -> dict:
    """Get the schema for a table from the database schema

    Parameters
    ----------
    schema:
        The schema for a database.
    table:
        The name of the table in the database.

    Returns
    -------
    result:
        The schema for the table.
    """
    tables = schema["tables"]
    for _table in tables:
        if _table["name"] == table:
            return _table
    raise UnrecognizedTableError("Could not find the table '{table}' in database")


def column_names_to_models(table: sqlalchemy.Table, columns: list[str]) -> list[sqlalchemy.Column]:
    """Return the sqlalchemy model of a Table column for each column name.

    This method is used to generate a sqlalchemy query based on a `~Query`.

    Parameters
    ----------
    table :
        The name of the table in the database.
    columns :
        The names of the columns to generate models for.

    Returns
    -------
        A list of sqlalchemy columns.
    """
    models = []
    for column in columns:
        models.append(getattr(table.columns, column))
    return models


def query_table(
    table: str,
    engine: sqlalchemy.engine.Engine,
    columns: list[str] | None = None,
    query: dict | None = None,
) -> Sequence[sqlalchemy.engine.row.Row]:
    """Query a table and return the results

    Parameters
    ----------
    engine :
        The engine used to connect to the database.
    table :
        The table that is being queried.
    columns :
        The columns from the table to return.
        If `columns` is ``None`` then all the columns
        in the table are returned.
    query :
        A query used on the table.
        If `query` is ``None`` then all the rows
        in the query are returned.

    Returns
    -------
    result :
        A list of the rows that were returned by the query.
    """
    metadata = sqlalchemy.MetaData()
    _table = sqlalchemy.Table(table, metadata, autoload_with=engine)

    if columns is None:
        _query = _table.select()
    else:
        _query = sqlalchemy.select(*column_names_to_models(_table, columns))

    if query is not None:
        _query = _query.where(Query.from_dict(query)(_table))

    connection = engine.connect()
    result = connection.execute(_query)
    return result.fetchall()


def calculate_bounds(table: str, column: str, engine: sqlalchemy.engine.Engine) -> tuple[float, float]:
    """Calculate the min, max for a column

    Parameters
    ----------
    table :
        The table that is being queried.
    column :
        The column to calculate the bounds of.
    engine :
        The engine used to connect to the database.

    Returns
    -------
    result :
        The ``(min, max)`` of the chosen column.
    """
    metadata = sqlalchemy.MetaData()
    _table = sqlalchemy.Table(table, metadata, autoload_with=engine)
    _column = _table.columns[column]

    query = sqlalchemy.select((sqlalchemy.func.min(_column)))
    connection = engine.connect()
    result = connection.execute(query)
    col_min = result.fetchone()
    if col_min is not None:
        col_min = col_min[0]
    else:
        raise ValueError(f"Could not calculate the min of column {column}")

    query = sqlalchemy.select((sqlalchemy.func.max(_column)))
    connection = engine.connect()
    result = connection.execute(query)
    col_max = result.fetchone()
    if col_max is not None:
        col_max = col_max[0]
    else:
        raise ValueError(f"Could not calculate the min of column {column}")

    return col_min, col_max


@dataclass(kw_only=True)
class LoadColumnsCommand(BaseCommand):
    """Load columns from a database table with an optional query.

    Attributes
    ----------
    database :
        The name of the database that the table is in.
    table :
        The table that the columns are loaded from.
    columns :
        Columns that are to be loaded. If `columns` is ``None``
        then all the columns in the `table` are loaded.
    query :
        Query used to select rows in the table.
        If `query` is ``None`` then all the rows are loaded.
    """

    database: str
    table: str
    columns: list[str] | None = None
    query: dict | None = None
    response_type: str = "table columns"

    def build_contents(self, databases: dict[str, DatabaseConnection], butler: Butler | None) -> dict:
        # Query the database to return the requested columns
        database = databases[self.database]
        index_column = get_table_schema(database.schema, self.table)["index_column"]
        columns = self.columns
        if columns is not None and index_column not in columns:
            columns = [index_column] + columns
        data = query_table(
            table=self.table,
            columns=columns,
            query=self.query,
            engine=database.engine,
        )

        if not data:
            # There is no column data to return
            content: dict = {
                "columns": columns,
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
    table :
        The table that the columns are loaded from.
    column :
        The column to calculate the bounds of.
    """

    database: str
    table: str
    column: str
    response_type: str = "column bounds"

    def build_contents(self, databases: dict[str, DatabaseConnection], butler: Butler | None) -> dict:
        database = databases[self.database]
        data = calculate_bounds(
            table=self.table,
            column=self.column,
            engine=database.engine,
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

    def build_contents(self, databases: dict[str, DatabaseConnection], butler: Butler | None) -> dict:
        database = databases[self.database]
        return database.schema


# Register the commands
LoadColumnsCommand.register("load columns")
CalculateBoundsCommand.register("get bounds")
LoadSchemaCommand.register("load schema");