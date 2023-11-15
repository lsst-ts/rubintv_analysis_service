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

from abc import ABC, abstractmethod
from typing import cast, Sequence
from lsst.rubintv.analysis.service.data import DataId, DatabaseSelectionId

import sqlalchemy
from .query import Query


class UnrecognizedTableError(Exception):
    """An error that occurs when a table name does not appear in the schema"""

    pass


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


class Join(ABC):
    join_type: str

    @abstractmethod
    def __call__(self, database: DatabaseConnection):
        pass


class InnerJoin(Join):
    n_columns: int
    matches: dict[str, tuple[str, ...]]

    def __init__(self, matches: dict[str, tuple[str, ...]]):
        self.join_type = "inner"
        if len(matches) != 2:
            raise ValueError(f"Inner joins must have exactly two tables: got {len(matches)}")

        n_columns = 0
        for _, fields in matches.items():
            if n_columns == 0:
                n_columns = len(fields)
            else:
                if n_columns != len(fields):
                    raise ValueError(
                        "Inner joins must have the same number of fields for each table: "
                        f"got {n_columns} and {len(fields)}"
                    )
        self.n_columns = n_columns
        self.matches = matches

    def __call__(self, database: DatabaseConnection):
        tables = tuple(self.matches.keys())
        table1 = tables[0]
        table2 = tables[1]
        table_model1 = sqlalchemy.Table(table1, database.metadata, autoload_with=database.engine)
        table_model2 = sqlalchemy.Table(table2, database.metadata, autoload_with=database.engine)
        joins = []
        for index in range(self.n_columns):
            joins.append(
                table_model1.columns[self.matches[table1][index]]
                == table_model2.columns[self.matches[table2][index]]
            )
        print("joins:", joins)
        return sqlalchemy.and_(*joins)


class DatabaseConnection:
    """A connection to a database.

    Attributes
    ----------
    engine :
        The engine used to connect to the database.
    schema :
        The schema for the database.
    metadata :
        The metadata for the database.
    joins :
        A dictionary of joins between tables.
    """

    engine: sqlalchemy.engine.Engine
    schema: dict
    metadata: sqlalchemy.MetaData
    joins: dict[str, tuple[Join, ...]]
    tables: dict[str, sqlalchemy.Table]

    def __init__(
        self,
        engine: sqlalchemy.engine.Engine,
        schema: dict,
    ):
        self.engine = engine
        self.schema = schema
        self.metadata = sqlalchemy.MetaData()
        joins = {}

        for join in self.schema["joins"]:
            if join["type"] == "inner":
                if "inner" not in joins:
                    joins["inner"] = []
                joins["inner"].append(InnerJoin(join["matches"]))
            else:
                raise NotImplementedError(f"Join type {join['type']} is not implemented")
        self.joins = {key: tuple(value) for key, value in joins.items()}

        self.tables = {}
        for table in schema["tables"]:
            self.tables[table["name"]] = sqlalchemy.Table(
                table["name"], self.metadata, autoload_with=self.engine
            )

    def get_table_names(self) -> tuple[str, ...]:
        """Given a schema, return a list of dataset names

        Returns
        -------
        result :
            The names of all the tables in the database.
        """
        return tuple(tbl["name"] for tbl in self.schema["tables"])

    def get_data_id(self, table: str) -> DataId:
        """Return the data id for a table.

        Parameters
        ----------
        table :
            The name of the table in the database.

        Returns
        -------
        result :
            The data id for the table.
        """
        return DataId(database=self.schema["name"], table=table)

    def get_selection_id(self, table: str) -> DatabaseSelectionId:
        """Return the selection indices for a table.

        Parameters
        ----------
        table :
            The name of the table in the database.

        Returns
        -------
        result :
            The selection indices for the table.
        """
        _table = self.schema["tables"][table]
        index_columns = _table["index_columns"]
        return DatabaseSelectionId(dataId=self.get_data_id(table), columns=index_columns)

    def get_column(self, column: str) -> sqlalchemy.Column:
        """Return the column model for a column.

        Parameters
        ----------
        column :
            The name of the column in the database.

        Returns
        -------
        result :
            The column model for the column.
        """
        table, column = column.split(".")
        return self.tables[table].columns[column]

    def get_index_model(self, table: str) -> sqlalchemy.Tuple:
        """Return the sqlalchemy model of the index for a table.

        Parameters
        ----------
        table :
            The name of the table in the database.

        Returns
        -------
            A sqlalchemy tuple of the index columns.
        """
        _table = self.schema["tables"][table]
        index_columns = [self.get_column(column) for column in _table["index_columns"]]
        return sqlalchemy.tuple_(*index_columns)

    def select_rows(
        self, table: str, columns: list[str], selected: list[tuple]
    ) -> Sequence[sqlalchemy.engine.row.Row]:
        """Select rows from a table.

        Parameters
        ----------
        table :
            The name of the table in the database.
        columns :
            The names of the columns to load.
        selected :
            The unique indices of the rows to select.

        Returns
        -------
        result :
            The selected rows.
        """
        table_model = self.tables[table]
        if len(columns) == 0:
            query = table_model.select()
        else:
            column_models = [table_model.columns[column] for column in columns]
            query = sqlalchemy.select(*column_models)
        query = query.filter(self.get_index_model(table))  # type: ignore

        connection = self.engine.connect()
        result = connection.execute(query)  # type: ignore
        return result.fetchall()

    def get_join(self, table1: str, table2: str, join_type: str) -> sqlalchemy.ColumnElement:
        """Return the join between two tables.

        Parameters
        ----------
        table1 :
            The first table in the join.
        table2 :
            The second table in the join.
        join_type :
            The type of join to use.

        Returns
        -------
        result :
            The join between the two tables.
        """
        if join_type == "inner":
            joins = cast(tuple[InnerJoin, ...], self.joins["inner"])
            for join in joins:
                tables = join.matches.keys()
                if table1 in tables and table2 in tables:
                    return join(self)

        raise ValueError(f"Could not find a join between {table1} and {table2}")

    def query(
        self,
        columns: list[str],
        query: dict | None = None,
    ) -> Sequence[sqlalchemy.engine.row.Row]:
        """Query a table and return the results

        Parameters
        ----------
        columns :
            The ``table.column`` names of the columns to load.
        query :
            A query used on the table.
            If `query` is ``None`` then all the rows
            in the query are returned.

        Returns
        -------
        result :
            A list of the rows that were returned by the query.
        """
        # Map each table to all of the columns that are being selected
        # from that table.
        table_columns = {}
        for full_column in columns:
            table, column = full_column.split(".")
            if table not in table_columns:
                table_columns[table] = []
            table_columns[table].append(column)

        # Add the index columns for each table if they aren't in the
        # list of columns to select.
        select_columns = []
        for table, columns in table_columns.items():
            # Add the index columns to the result, since the front end
            # will need the unique identifier for each row.
            table_schema = get_table_schema(self.schema, table)

            for index in table_schema["index_columns"]:
                if index not in table_columns:
                    table_columns[table].append(index)
            select_columns += [self.get_column(f"{table}.{column}") for column in columns]

        # Select columns to query.
        non_null_conditions = sqlalchemy.and_(*[col.isnot(None) for col in select_columns])
        query_model = sqlalchemy.select(*select_columns).where(non_null_conditions)

        # Apply the query (if there is one).
        if query is not None:
            _query = Query.from_dict(query)(self)
            query_model = query_model.where(_query.result)

        # Include a join if there is more than one table in the selection
        join_conditions = []
        join_table = None
        tables = set(table_columns.keys())
        for table in tables:
            if join_table is None:
                # This is the first table.
                join_table = table
            else:
                join_conditions.append((table, self.get_join(join_table, table, "inner")))
                join_table = table
        for join in join_conditions:
            print(type(query_model))
            query_model = query_model.join(join)

        connection = self.engine.connect()
        result = connection.execute(query_model)
        return result.fetchall()

    def calculate_bounds(self, table: str, column: str) -> tuple[float, float]:
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
        _table = sqlalchemy.Table(table, self.metadata, autoload_with=self.engine)
        _column = _table.columns[column]

        with self.engine.connect() as connection:
            query = sqlalchemy.select((sqlalchemy.func.min(_column)))
            result = connection.execute(query)
            col_min = result.fetchone()
            if col_min is not None:
                col_min = col_min[0]
            else:
                raise ValueError(f"Could not calculate the min of column {column}")

            query = sqlalchemy.select((sqlalchemy.func.max(_column)))
            result = connection.execute(query)
            col_max = result.fetchone()
            if col_max is not None:
                col_max = col_max[0]
            else:
                raise ValueError(f"Could not calculate the max of column {column}")
        return col_min, col_max
