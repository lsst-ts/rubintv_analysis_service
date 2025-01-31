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

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

from ..command import BaseCommand
from ..database import exposure_tables, visit1_tables
from ..query import EqualityQuery, ParentQuery, Query

if TYPE_CHECKING:
    from ..data import DataCenter


logger = logging.getLogger("lsst.rubintv.analysis.service.commands.db")


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
        Query used to select rows in the table for a specific Widget.
        If `query` is ``None`` then all the rows are loaded.
    global_query :
        Query used to select rows for all Widgets in the remote workspace.
        If `global_query` is ``None`` then no global query is used.
    day_obs :
        The day_obs to filter the data on.
        If day_obs is None then no filter on day_obs is used unless otherwise
        specified in `query` or `global_query`.
    data_ids :
        The data IDs to filter the data on.
        If data_ids is specified then only rows with the specified
        day_obs and seq_num are selected.
    """

    database: str
    columns: list[str]
    query: dict | None = None
    global_query: dict | None = None
    day_obs: str | None = None
    data_ids: list[tuple[int, int]] | None = None
    aggregator: str | None = None
    response_type: str = "table columns"

    def build_contents(self, data_center: DataCenter) -> dict:
        # Query the database to return the requested columns
        database = data_center.schemas[self.database]

        query: Query | None = None
        if self.query is not None:
            query = Query.from_dict(self.query)
        if self.global_query is not None:
            global_query = Query.from_dict(self.global_query)
            if query is None:
                query = global_query
            else:
                query = ParentQuery(
                    children=[query, global_query],
                    operator="AND",
                )
        if self.day_obs is not None:
            table_name = self.columns[0].split(".")[0]
            if table_name in exposure_tables:
                column = "exposure.day_obs"
            elif table_name in visit1_tables:
                column = "visit1.day_obs"
            else:
                raise ValueError(f"Unsupported table name: {table_name}")
            day_obs_query = EqualityQuery(
                column=column,
                value=int(self.day_obs.replace("-", "")),
                operator="eq",
            )
            if query is None:
                query = day_obs_query
            else:
                query = ParentQuery(
                    children=[query, day_obs_query],
                    operator="AND",
                )

        data = database.query(self.columns, query, self.data_ids, self.aggregator)

        if not data:
            # There is no data to return
            data = []
        content = {
            "schema": self.database,
            "columns": self.columns,
            "data": data,
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
        database = data_center.schemas[self.database]
        data = database.calculate_bounds(
            column=self.column,
        )
        return {
            "column": self.column,
            "bounds": data,
        }


@dataclass(kw_only=True)
class LoadInstrumentCommand(BaseCommand):
    """Load the instruments for a database.

    Attributes
    ----------
    instrument :
        The name of the instrument (camera) to load.
    """

    instrument: str
    response_type: str = "instrument info"

    def build_contents(self, data_center: DataCenter) -> dict:
        from lsst.afw.cameraGeom import FOCAL_PLANE
        from lsst.obs.lsst import Latiss, LsstCam, LsstComCam, LsstComCamSim

        instrument = self.instrument.lower()

        match instrument:
            case "lsstcam":
                camera = LsstCam.getCamera()
            case "lsstcomcam":
                camera = LsstComCam.getCamera()
            case "latiss":
                camera = Latiss.getCamera()
            case "lsstcomcamsim":
                camera = LsstComCamSim.getCamera()
            case "testdb":
                camera = None
            case _:
                raise ValueError(f"Unsupported instrument: {instrument}")

        detectors = []
        if camera is not None:
            for detector in camera:
                corners = [(c.getX(), c.getY()) for c in detector.getCorners(FOCAL_PLANE)]
                detectors.append(
                    {
                        "id": detector.getId(),
                        "name": detector.getName(),
                        "corners": corners,
                    }
                )

        result = {
            "instrument": self.instrument,
            "detectors": detectors,
        }

        # Load the data base to access the schema
        schema_name = f"cdb_{instrument}" if instrument != "testdb" else "testdb"
        try:
            database = data_center.schemas[schema_name]
            result["schema"] = database.get_verified_schema()

        except KeyError:
            logger.warning(f"No database connection available for {schema_name}")
            logger.warning(f"Available databases: {data_center.schemas.keys()}")

        return result


# Register the commands
LoadColumnsCommand.register("load columns")
CalculateBoundsCommand.register("get bounds")
LoadInstrumentCommand.register("load instrument")
