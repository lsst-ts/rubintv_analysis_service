from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable, TYPE_CHECKING

if TYPE_CHECKING:
    from lsst.daf.butler import Butler
    from .database import DatabaseConnection
    from .efd import EfdClient


class DataId:
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
    pass


@dataclass(kw_only=True)
class DatabaseSelectionId(SelectionId):
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
    pass


@dataclass(kw_only=True)
class EfdSelectionId(SelectionId):
    pass


@dataclass
class DataMatch(ABC):
    dataId1: DataId
    dataId2: DataId

    @abstractmethod
    def match_forward(self, indices: list[SelectionId]):
        pass

    @abstractmethod
    def match_backward(self, indices: list[SelectionId]):
        pass


class DataCenter:
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
