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

import sqlalchemy
from .query import Query
from .data import DataId, DatabaseSelectionId


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
    """A join between two tables in a database.

    Attributes
    ----------
    join_type :
        The type of join. For now only "inner" joins are supported.
    """
    join_type: str

    @abstractmethod
    def __call__(self, database: DatabaseConnection):
        pass


class InnerJoin(Join):
    """An inner join between two tables in a database.

    Attributes
    ----------
    n_columns :
        The number of columns in the join.
    matches :
        Dictionary with table names as keys and tuples of column names as
        values in the order in which they are matched in the join.
    """
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
        """Create the sqlalchemy join between the two tables.

        Parameters
        ----------
        database :
            The database connection.
        """
        tables = tuple(self.matches.keys())
        table1 = tables[0]
        table2 = tables[1]
        table_model1 = database.tables[table1]
        table_model2 = database.tables[table2]
        joins = []
        for index in range(self.n_columns):
            joins.append(
                table_model1.columns[self.matches[table1][index]]
                == table_model2.columns[self.matches[table2][index]]
            )
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
    ) -> dict[str, list]:
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
        table_columns = set()
        table_names = set()
        column_names = set()
        # get the sql alchemy model for each column
        for column in columns:
            table_name, column_name = column.split(".")
            table_names.add(table_name)
            column_names.add(column_name)
            column_obj = self.get_column(column)
            # Label each column as 'table_name.column_name'
            table_columns.add(column_obj.label(f"{table_name}.{column_name}"))

        # Add the index for all of the tables that are being selected on
        for table_name in table_names:
            _table = get_table_schema(self.schema, table_name)
            for column in _table["index_columns"]:
                if column not in column_names:
                    column_obj = self.get_column(f"{table_name}.{column}")
                    table_columns.add(column_obj.label(f"{table_name}.{column}"))
                    column_names.add(column)

        # generate the query
        query_model = sqlalchemy.and_(*[col.isnot(None) for col in table_columns])
        if query is not None:
            query_result = Query.from_dict(query)(self)
            query_model = sqlalchemy.and_(query_model, query_result.result)
            table_names.union(query_result.tables)

        # Build the join
        tables = list(table_names)
        last_table = tables[0]
        select_from = self.tables[last_table]
        if len(table_names) > 1:
            for table_name in tables[1:]:
                join = self.get_join(last_table, table_name, "inner")
                select_from = sqlalchemy.join(select_from, self.tables[table_name], join)

        # Build the query
        query_model = sqlalchemy.select(*table_columns).select_from(select_from).where(query_model)

        # Fetch the data
        connection = self.engine.connect()
        result = connection.execute(query_model)
        data = result.fetchall()

        # Convert the unnamed row data into columns
        return {str(col): [row[i] for row in data] for i, col in enumerate(result.keys())}


    def calculate_bounds(self, column: str) -> tuple[float, float]:
        """Calculate the min, max for a column

        Parameters
        ----------
        column :
            The column to calculate the bounds of in the format "table.column".

        Returns
        -------
        result :
            The ``(min, max)`` of the chosen column.
        """
        table, column = column.split(".")
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
