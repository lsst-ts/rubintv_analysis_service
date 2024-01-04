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
import operator as op
from abc import ABC, abstractmethod
from typing import Any, TYPE_CHECKING

import sqlalchemy

if TYPE_CHECKING:
    from .database import DatabaseConnection


class QueryError(Exception):
    """An error that occurred during a query"""

    pass


@dataclass
class QueryResult:
    """The result of a query.

    Attributes
    ----------
    result :
        The result of the query as an sqlalchemy expression.
    tables :
        All of the tables that were used in the query.
    """
    result: sqlalchemy.ColumnElement
    tables: set[str]


class Query(ABC):
    """Base class for constructing queries."""

    @abstractmethod
    def __call__(self, database: DatabaseConnection) -> QueryResult:
        """Run the query on a table.

        Parameters
        ----------
        database :
            The connection to the database that is being queried.

        Returns
        -------
        QueryResult :
            The result of the query, including the tables that are
            needed for the query.
        """
        pass

    @staticmethod
    def from_dict(query_dict: dict[str, Any]) -> Query:
        """Construct a query from a dictionary of parameters.

        Parameters
        ----------
        query_dict :
            Kwargs used to initialize the query.
            There should only be two keys in this dict,
            the ``name`` of the query and the ``content`` used
            to initialize the query.
        """
        try:
            if query_dict["name"] == "EqualityQuery":
                return EqualityQuery.from_dict(query_dict["content"])
            elif query_dict["name"] == "ParentQuery":
                return ParentQuery.from_dict(query_dict["content"])
        except Exception:
            raise QueryError(f"Failed to parse query: {query_dict}")

        raise QueryError("Unrecognized query type")


class EqualityQuery(Query):
    """A query that compares a column to a static value.

    Parameters
    ----------
    column :
        The column used in the query.
    operator :
        The operator to use for the query.
    value :
        The value that the column is compared to.
    """

    def __init__(
        self,
        column: str,
        operator: str,
        value: Any,
    ):
        self.operator = operator
        self.column = column
        self.value = value

    def __call__(self, database: DatabaseConnection) -> QueryResult:
        table_name, _ = self.column.split(".")
        column = database.get_column(self.column)
        result = None
        if self.operator in ("eq", "ne", "lt", "le", "gt", "ge"):
            operator = getattr(op, self.operator)
            result = operator(column, self.value)
        elif self.operator in ("startswith", "endswith", "contains"):
            result = getattr(column, self.operator)(self.value)
        else :
            raise QueryError(f"Unrecognized Equality operator {self.operator}")

        return QueryResult(result, set((table_name,)))

    @staticmethod
    def from_dict(query_dict: dict[str, Any]) -> EqualityQuery:
        return EqualityQuery(**query_dict)


class ParentQuery(Query):
    """A query that uses a binary operation to combine other queries.

    Parameters
    ----------
    children :
        The child queries that are combined using the binary operator.
    operator :
        The operator that us used to combine the queries.
    """

    def __init__(self, children: list[Query], operator: str):
        self._children = children
        self._operator = operator

    def __call__(self, database: DatabaseConnection) -> QueryResult:
        child_results = []
        tables = set()
        for child in self._children:
            result = child(database)
            child_results.append(result.result)
            tables.update(result.tables)

        try:
            match self._operator:
                case "AND":
                    result = sqlalchemy.and_(*child_results)
                case "OR":
                    result = sqlalchemy.or_(*child_results)
                case "NOT":
                    result = sqlalchemy.not_(*child_results)
                case "XOR":
                    result = sqlalchemy.and_(
                        sqlalchemy.or_(*child_results),
                        sqlalchemy.not_(sqlalchemy.and_(*child_results)),
                    )
        except Exception:
            raise QueryError("Error applying a boolean query statement.")

        return QueryResult(result, tables)  # type: ignore

    @staticmethod
    def from_dict(query_dict: dict[str, Any]) -> ParentQuery:
        return ParentQuery(
            children=[Query.from_dict(child) for child in query_dict["children"]],
            operator=query_dict["operator"],
        )
