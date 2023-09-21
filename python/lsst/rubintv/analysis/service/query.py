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

import operator as op
from abc import ABC, abstractmethod
from typing import Any

import sqlalchemy


class QueryError(Exception):
    """An error that occurred during a query"""

    pass


class Query(ABC):
    """Base class for constructing queries."""

    @abstractmethod
    def __call__(self, table: sqlalchemy.Table) -> sqlalchemy.sql.elements.BooleanClauseList:
        """Run the query on a table.

        Parameters
        ----------
        table :
            The table to run the query on.
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
            raise QueryError("Failed to parse query.")

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

    def __call__(self, table: sqlalchemy.Table) -> sqlalchemy.sql.elements.BooleanClauseList:
        column = table.columns[self.column]

        if self.operator in ("eq", "ne", "lt", "le", "gt", "ge"):
            operator = getattr(op, self.operator)
            return operator(column, self.value)

        if self.operator not in ("startswith", "endswith", "contains"):
            raise QueryError(f"Unrecognized Equality operator {self.operator}")

        return getattr(column, self.operator)(self.value)

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
        self.children = children
        self.operator = operator

    def __call__(self, table: sqlalchemy.Table) -> sqlalchemy.sql.elements.BooleanClauseList:
        child_results = [child(table) for child in self.children]
        try:
            if self.operator == "AND":
                return sqlalchemy.and_(*child_results)
            if self.operator == "OR":
                return sqlalchemy.or_(*child_results)
            if self.operator == "NOT":
                return sqlalchemy.not_(*child_results)
            if self.operator == "XOR":
                return sqlalchemy.and_(
                    sqlalchemy.or_(*child_results),
                    sqlalchemy.not_(sqlalchemy.and_(*child_results)),
                )
        except Exception:
            raise QueryError("Error applying a boolean query statement.")

    @staticmethod
    def from_dict(query_dict: dict[str, Any]) -> ParentQuery:
        return ParentQuery(
            children=[Query.from_dict(child) for child in query_dict["children"]],
            operator=query_dict["operator"],
        )
