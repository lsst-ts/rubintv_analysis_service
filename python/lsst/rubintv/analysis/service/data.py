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
from dataclasses import dataclass
from typing import Callable, TYPE_CHECKING

if TYPE_CHECKING:
    from lsst.daf.butler import Butler
    from .database import DatabaseConnection
    from .efd import EfdClient


class DataId:
    """A unique identifier for a dataset."""
    def __init__(self, **parameters):
        self.parameters = parameters
        for name, parameter in parameters.items():
            setattr(self, name, parameter)

    def __hash__(self):
        return hash(tuple(sorted(self.parameters.items())))

    def __eq__(self, other: DataId):
        for parameter in self.parameters:
            if parameter not in other.parameters or self.parameters[parameter] != other.parameters[parameter]:
                return False
        return True


@dataclass(kw_only=True)
class SelectionId:
    """A unique identifier for a data entry."""
    pass


@dataclass(kw_only=True)
class DatabaseSelectionId(SelectionId):
    """A unique identifier for a database row.

    Attributes
    ----------
    dataID :
        The data ID of the row.
    columns :
        Columns in the selected row.
    """
    dataId: DataId
    columns: tuple[str]

    def __hash__(self):
        return hash((self.dataId, sorted(self.columns)))

    def __eq__(self, other: DatabaseSelectionId):
        if self.dataId != other.dataId or len(self.columns) != len(other.columns):
            return False
        for column, other_column in zip(sorted(self.columns), sorted(other.columns)):
            if column != other_column:
                return False
        return True


@dataclass(kw_only=True)
class ButlerSelectionId(SelectionId):
    """A unique identifier for a Butler dataset."""
    pass


@dataclass(kw_only=True)
class EfdSelectionId(SelectionId):
    """A unique identifier for an EFD dataset entry."""
    pass


@dataclass
class DataMatch(ABC):
    """A match between two datasets.

    Attributes
    ----------
    dataId1 :
        The data ID of the first dataset to match.
    dataId2 :
        The data ID of the second dataset to match.
    """
    dataId1: DataId
    dataId2: DataId

    @abstractmethod
    def match_forward(self, indices: list[SelectionId]):
        """Match the first dataset to the second."""
        pass

    @abstractmethod
    def match_backward(self, indices: list[SelectionId]):
        """Match the second dataset to the first."""
        pass


class DataCenter:
    """A class that manages access to data.

    This includes functions to match entries between datasets,
    for example the exposure ID from the visit database can
    be matched to exposures in the Butler.

    Attributes
    ----------
    matches :
        A dictionary of matches between datasets.
    databases :
        A dictionary of database connections.
    butler :
        A Butler instance.
    efd_client :
        An EFD client instance.
    """
    matches: dict[tuple[DataId, DataId], Callable]
    databases: dict[str, DatabaseConnection]
    butler: Butler | None = None
    efd_client: EfdClient | None = None

    def __init__(
        self,
        databases: dict[str, DatabaseConnection],
        butler: Butler | None = None,
        efd_client: EfdClient | None = None,
        matches: dict[tuple[DataId, DataId], Callable] | None = None,
    ):
        self.databases = databases
        self.butler = butler
        self.efdClient = efd_client
        self.matches = matches or {}
