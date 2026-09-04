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
import threading
import time

import sqlalchemy

from .data import DatabaseSelectionId, DataId
from .query import Query

logger = logging.getLogger("lsst.rubintv.analysis.service.database")

# How long a verified schema is reused before being recalculated. Which
# columns are entirely null changes as the schema evolves rather than per
# request, so this only has to be short enough that a column gaining its first
# data becomes visible without a restart.
VERIFIED_SCHEMA_TTL = 3600.0


# Exposure tables currently in the schema
exposure_tables = [
    "exposure",
    "exposure_quicklook",
    "ccdexposure",
    "ccdexposure_camera",
    "ccdexposure_quicklook",
]

# Tables in the schema for single visit exposures
visit1_tables = [
    "visit1",
    "visit1_quicklook",
    "ccdvisit1",
    "ccdvisit1_quicklook",
]

# Flex tables in the schema.
# These are currently not implement and would take some thought implmenting
# correctly, so we ignore them for now.
flex_tables = [
    "exposure_flexdata",
    "exposure_flexdata_schema",
    "ccdexposure_flexdata",
    "ccdexposure_flexdata_schema",
    "ccdexposure_quicklook",
]


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


class JoinError(Exception):
    """An error that occurs when a join cannot be made between two tables"""

    pass


class JoinBuilder:
    """Builds joins between tables in sqlalchemy.

    Using a dictionary of joins, usually from the joins.yaml file,
    this class builds a graph of joins between tables so that given a
    list of tables it can create a join that connects all the tables.

    Attributes
    ----------
    tables :
        A dictionary of tables in the schema.
    joins :
        A list of inner joins between tables. Each item in the list should
        have a ``matches`` key with another dictionary as values.
        The values will have the names of the tables being joined as keys
        and a list of columns to join on as values.
    """

    def __init__(self, tables: dict[str, sqlalchemy.Table], joins: list[dict]):
        self.tables = tables
        self.joins = joins
        self.join_graph = self._build_join_graph()

    def _build_join_graph(self) -> dict[str, dict[str, list[str]]]:
        """Create the graph of joins from the list of joins."""
        graph = {table: {} for table in self.tables}
        for join in self.joins:
            tables = list(join["matches"].keys())
            t1, t2 = tables[0], tables[1]
            if t1 not in self.tables.keys() or t2 not in self.tables.keys():
                # The tables have likely been added to the schema
                # but not yet implemented in the database,
                # so the joins exist but there are no tables to join yet.
                logger.warning(f"Skipping join between tables {t1} and {t2}")
                continue
            join_columns = list(zip(join["matches"][t1], join["matches"][t2]))
            graph[t1][t2] = join_columns
            graph[t2][t1] = [(col2, col1) for col1, col2 in join_columns]
        return graph

    def _find_join_path(self, start: str, end: str) -> list[str]:
        """Find a path between two tables in the join graph.

        In some cases, such as between vist1 and ccdvisit1_quicklook,
        this might require intermediary joins.

        Parameters
        ----------
        start :
            The name of the table to start the join from.
        end :
            The name of the table to join to.

        Returns
        -------
        result :
            A list of tables that can be joined to get from the
            first table to the last table.
        """
        queue = [(start, [start])]
        visited = set()

        while queue:
            (node, path) = queue.pop(0)
            if node not in visited:
                if node == end:
                    return path
                visited.add(node)
                for neighbor in self.join_graph[node]:
                    if neighbor not in visited:
                        queue.append((neighbor, path + [neighbor]))
        raise JoinError(f"No path found between {start} and {end}")

    def build_join(self, table_names: set[str]) -> sqlalchemy.Table | sqlalchemy.Join:
        """Build a join between all of the tables in a SQL statement.

        Parameters
        ----------
        table_names :
            A set of table names to join.

        Returns
        -------
        result :
            The join between all of the tables.
        """
        tables = list(table_names)
        select_from = self.tables[tables[0]]
        # Use the first table as the starting point
        joined_tables = set([tables[0]])
        logger.info(f"Starting join with table: {tables[0]}")
        logger.info(f"all tables: {tables}")

        for i in range(1, len(tables)):
            # Move to the next table
            current_table = tables[i]
            if current_table in joined_tables:
                logger.info(f"Skipping {current_table} as it's already joined")
                continue

            # find the join path from the first table to the current table
            join_path = self._find_join_path(tables[0], current_table)
            logger.info(f"Join path from {tables[0]} to {current_table}: {join_path}")

            for j in range(1, len(join_path)):
                # Join all of the tables in the join_path
                t1, t2 = join_path[j - 1], join_path[j]
                if t2 in joined_tables:
                    logger.info(f"Skipping {t2} as it's already joined")
                    continue

                logger.info(f"Joining {t1} to {t2}")
                join_conditions = []
                for col1, col2 in self.join_graph[t1][t2]:
                    logger.info(f"Attempting to join {t1}.{col1} = {t2}.{col2}")
                    try:
                        condition = self.tables[t1].columns[col1] == self.tables[t2].columns[col2]
                        join_conditions.append(condition)
                    except KeyError as e:
                        logger.error(f"Column not found: {e}")
                        logger.error(f"Available columns in {t1}: {list(self.tables[t1].columns.keys())}")
                        logger.error(f"Available columns in {t2}: {list(self.tables[t2].columns.keys())}")
                        raise

                if not join_conditions:
                    raise ValueError(f"No valid join conditions found between {t1} and {t2}")

                # Implement the join in sqlalchemy
                select_from = sqlalchemy.join(select_from, self.tables[t2], *join_conditions)
                joined_tables.add(t2)

        return select_from


def _remove_schema_table(schema, table_name):
    """Remove a table from the schema

    We do this because the ConsDbSchema contains the tables and columns
    that are sent to the DDV. So removing a table here ensures that
    the DDV does not try to query a table that does not exist in the database.

    Parameters
    ----------
    schema :
        The schema to remove the table from.
    table_name :
        The name of the table to remove.

    Returns
    -------
    result :
        The schema with the table removed.
    """
    schema = schema.copy()
    for table in schema["tables"]:
        if table["name"] == table_name:
            logger.warning(f"Removing table {table_name} from schema")
            schema["tables"].remove(table)
            break
    return schema


class ConsDbSchema:
    """A schema (instrument) in the consolidated database.

    Attributes
    ----------
    engine :
        The engine used to connect to the database.
    schema :
        The schema yaml converted into a dict for the instrument.
    metadata :
        The metadata for the database.
    joins :
        A JoinBuilder object that builds joins between tables.
    """

    engine: sqlalchemy.engine.Engine
    schema: dict
    metadata: sqlalchemy.MetaData
    tables: dict[str, sqlalchemy.Table]
    joins: JoinBuilder

    def __init__(
        self,
        engine: sqlalchemy.engine.Engine,
        schema: dict,
        join_templates: list,
        verified_schema_ttl: float = VERIFIED_SCHEMA_TTL,
    ):
        self.engine = engine
        self.schema = schema
        self.metadata = sqlalchemy.MetaData()

        # Cache for get_verified_schema, which is expensive enough that
        # recomputing it per request dominates "load instrument".
        self._verified_schema: dict | None = None
        self._verified_schema_time: float = 0.0
        self._verified_schema_ttl = verified_schema_ttl
        self._stop_refresh = threading.Event()

        self.tables = {}
        schema_tables = self.schema["tables"].copy()
        for table in schema_tables:
            if (
                table["name"] not in exposure_tables
                and table["name"] not in visit1_tables
                and table["name"] not in flex_tables
            ):
                # A new table was added to the schema and cannot be parsed
                msg = f"Table {table['name']} has not been implemented in the RubinTV analysis service"
                logger.warning(msg)
                _remove_schema_table(self.schema, table["name"])
            else:
                try:
                    self.tables[table["name"]] = sqlalchemy.Table(
                        table["name"],
                        self.metadata,
                        autoload_with=self.engine,
                        schema=schema["name"],
                    )
                except sqlalchemy.exc.NoSuchTableError:
                    # The table is in sdm_schemas but has not yet been added
                    # to the database.
                    logger.warning(f"Table {table['name']} from schema not found in database")
                    _remove_schema_table(self.schema, table["name"])

        self.joins = JoinBuilder(self.tables, join_templates)

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
        return DatabaseSelectionId(data_id=self.get_data_id(table), columns=index_columns)

    def get_table(self, table: str) -> sqlalchemy.Table:
        """Return the table model for a table.

        Parameters
        ----------
        table :
            The name of the table in the database.

        Returns
        -------
        result :
            The table model for the table.
        """
        return self.tables[table]

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

    def fetch_data(self, query_model: sqlalchemy.Select) -> dict[str, list]:
        """Load data from the database.

        Parameters
        ----------
        query_model :
            The query to run on the database.
        """
        logger.info(f"Query: {query_model}")
        connection = self.engine.connect()
        result = connection.execute(query_model)
        data = result.fetchall()
        connection.close()

        # Convert the unnamed row data into columns
        return {str(col): [row[i] for row in data] for i, col in enumerate(result.keys())}

    def get_column_models(
        self, columns: list[str], using_aggregator: bool
    ) -> tuple[set[sqlalchemy.Column], set[str], list[sqlalchemy.Column]]:
        """Return the sqlalchemy models for a list of columns.

        Parameters
        ----------
        columns :
            The names of the columns in the database.
        using_aggregator : bool
            True if aggregator is being used for this query.


        Returns
        -------
        result :
            The column models for the columns.
        """
        table_columns = set()
        table_names: set[str] = set()
        # get the sql alchemy model for each column
        for column in columns:
            table_name, column_name = column.split(".")
            table_names.add(table_name)
            column_obj = self.get_column(column)
            # Label each column as 'table_name.column_name'
            table_columns.add(column_obj.label(f"{table_name}.{column_name}"))

        # Add the data Ids (seq_num and day_obs) to the query.
        def add_data_ids(table_name: str) -> list[sqlalchemy.Column]:
            day_obs_column = self.get_column(f"{table_name}.day_obs")
            seq_num_column = self.get_column(f"{table_name}.seq_num")
            # Strip off the table name to make the data IDs uniform
            table_columns.add(day_obs_column.label("day_obs"))
            table_columns.add(seq_num_column.label("seq_num"))
            return [day_obs_column, seq_num_column]

        if not using_aggregator:
            if list(table_names)[0] in visit1_tables:
                data_id_columns = add_data_ids("visit1")
                table_names.add("visit1")
            elif list(table_names)[0] in exposure_tables:
                data_id_columns = add_data_ids("exposure")
                table_names.add("exposure")
            else:
                raise ValueError(f"Unsupported table name: {list(table_names)[0]}")
        else:
            data_id_columns = [None, None]

        return table_columns, table_names, data_id_columns

    def query(
        self,
        columns: list[str],
        query: Query | None = None,
        data_ids: list[tuple[int, int]] | None = None,
        aggregator: str | None = None,
    ) -> dict[str, list] | dict[str, int]:
        """
        Query a table and return the results.

        Parameters
        ----------
        columns : list[str]
            The ``table.column`` names of the columns to load.
        query : Query | None
            A query used on the table. If `query` is None, all rows are
            returned.
        data_ids : list[tuple[int, int]] | None
            The data IDs to query, in the format ``(day_obs, seq_num)``.
        aggregator : str | None
            The SQL aggregation function to apply (e.g., 'sum', 'avg').

        Returns
        -------
        result : dict[str, list] | dict[str, int]
            A dictionary of columns as keys and lists of values or aggregated
            results as values.
        """
        # Get the models for the columns
        table_columns, table_names, data_id_columns = self.get_column_models(columns, aggregator is not None)
        if data_id_columns:
            day_obs_column, seq_num_column = data_id_columns

        logger.info(f"Table names: {table_names}")

        # Generate the base query
        query_model = sqlalchemy.and_(*[col.isnot(None) for col in table_columns])

        if query is not None:
            query_result = query(self)
            query_model = sqlalchemy.and_(query_model, query_result.result)
            table_names.update(query_result.tables)

        # Build the `FROM` clause after ensuring all required table names are
        # included
        select_from = self.joins.build_join(table_names)

        if data_ids is not None:
            data_id_select = sqlalchemy.tuple_(day_obs_column, seq_num_column).in_(data_ids)
            query_model = sqlalchemy.and_(query_model, data_id_select)

        if aggregator is not None:
            # Validate and apply the aggregator
            try:
                aggregate_func = getattr(sqlalchemy.func, aggregator.lower())
            except AttributeError:
                raise ValueError(f"Invalid aggregator '{aggregator}' provided.")

            query_model = (
                sqlalchemy.select(*[aggregate_func(column) for column in table_columns])
                .select_from(select_from)
                .where(query_model)
            )
        else:
            # Build the standard query
            query_model = sqlalchemy.select(*table_columns).select_from(select_from).where(query_model)

        # Fetch the data
        result = self.fetch_data(query_model)

        # Adjust result structure for aggregator
        if aggregator is not None:
            # Flatten result for single aggregated value
            values = iter(result.values())
            return {column.key: next(values)[0] for column in table_columns}

        return result

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

    def columns_with_values(self, table_name: str, column_names: list[str]) -> set[str]:
        """Find which of a table's columns contain at least one non-null value.

        This answers the same question as `has_non_null_values` but for a whole
        table at once: ``count`` ignores nulls, so a count of zero means the
        column is entirely null. One query per table rather than one per column
        roughly halves the time to verify a schema.

        Parameters
        ----------
        table_name :
            The name of the table to check.
        column_names :
            The columns of that table to check.

        Returns
        -------
        set[str]
            The names of the columns that hold at least one non-null value. On
            error the columns are all reported as populated, so that a failure
            here hides nothing from the user.
        """
        if table_name not in self.tables:
            logger.warning(f"Table '{table_name}' not found in database schema.")
            return set()

        table = self.tables[table_name]
        try:
            counts = [sqlalchemy.func.count(table.columns[name]).label(name) for name in column_names]
            with self.engine.connect() as connection:
                row = connection.execute(sqlalchemy.select(*counts)).fetchone()
            if row is None:
                return set()
            return {name for name, count in zip(column_names, row) if count}
        except Exception as e:
            logger.error(f"Error checking non-null values for table '{table_name}': {e}")
            return set(column_names)

    def has_non_null_values(self, column: str) -> bool:
        """Check if a column contains any non-null values.

        Parameters
        ----------
        column : str
            The column to check, in the format "table.column".

        Returns
        -------
        bool
            True if the column contains at least one non-null value, False
            otherwise.
        """
        try:
            table_name, column_name = column.split(".")
            if table_name not in self.tables:
                logger.warning(f"Table '{table_name}' not found in database schema.")
                return False

            _table = self.tables[table_name]
            _column = _table.columns[column_name]

            # Query to check if at least one non-null value exists
            query = sqlalchemy.select(_column).where(_column.isnot(None)).limit(1)

            with self.engine.connect() as connection:
                result = connection.execute(query).fetchone()

            return result is not None  # True if we found at least one non-null value

        except Exception as e:
            logger.error(f"Error checking non-null values for column '{column}': {e}")
            return False

    def get_verified_schema(self) -> dict:
        """The schema with entirely empty columns removed.

        The result is cached: calculating it queries every table, which is slow
        enough to dominate the "load instrument" command, and which columns
        hold data changes as the schema evolves rather than between requests.
        The cache expires after ``verified_schema_ttl`` seconds so a column
        that gains its first data appears without restarting the worker.
        """
        age = time.monotonic() - self._verified_schema_time
        if self._verified_schema is not None and age < self._verified_schema_ttl:
            return self._verified_schema

        start = time.monotonic()
        self._verified_schema = self._calculate_verified_schema()
        self._verified_schema_time = time.monotonic()
        logger.info(
            f"Verified the {self.schema['name']} schema in " f"{self._verified_schema_time - start:.1f}s"
        )
        return self._verified_schema

    def refresh_verified_schema(self) -> dict:
        """Recalculate the verified schema, ignoring any cached result.

        The new schema is built before it replaces the old one, so a request
        arriving during the recalculation is served the previous result rather
        than waiting for this one or seeing a half-built schema.
        """
        schema = self._calculate_verified_schema()
        self._verified_schema = schema
        self._verified_schema_time = time.monotonic()
        return schema

    def start_background_refresh(self, interval: float | None = None) -> threading.Thread:
        """Recalculate the verified schema periodically on a daemon thread.

        Without this the cache expires lazily: the first request after the TTS
        lapses pays the whole recalculation, so roughly once an interval one
        user waits several seconds. Refreshing ahead of that keeps every
        request served from cache.

        The thread is a daemon so it never holds up interpreter shutdown, and
        it swallows its errors: a refresh that fails leaves the previous schema
        in place, which is the same outcome as not having refreshed at all.

        Parameters
        ----------
        interval :
            Seconds between refreshes. Defaults to the instance's TTL.

        Returns
        -------
        thread :
            The started thread, so a caller can keep a handle on it.
        """
        if interval is None:
            interval = self._verified_schema_ttl

        def refresh_periodically() -> None:
            # wait() rather than sleep() so a future shutdown path can stop the
            # thread promptly instead of waiting out the whole interval.
            while not self._stop_refresh.wait(interval):
                try:
                    self.refresh_verified_schema()
                except Exception as e:
                    logger.error(f"Failed to refresh the {self.schema['name']} schema: {e}")

        thread = threading.Thread(
            target=refresh_periodically,
            name=f"refresh-{self.schema['name']}",
            daemon=True,
        )
        thread.start()
        return thread

    def stop_background_refresh(self) -> None:
        """Ask the background refresh thread to finish."""
        self._stop_refresh.set()

    def _calculate_verified_schema(self) -> dict:
        """Query the database to find which columns hold data."""
        filtered_schema = self.schema.copy()
        filtered_schema["tables"] = []

        for table in self.schema.get("tables", []):
            column_names = [column["name"] for column in table.get("columns", [])]
            populated = self.columns_with_values(table["name"], column_names)
            filtered_columns = [column for column in table.get("columns", []) if column["name"] in populated]

            if filtered_columns:
                # Preserve all table metadata dynamically
                filtered_table = {key: value for key, value in table.items() if key != "columns"}
                filtered_table["columns"] = filtered_columns
                filtered_schema["tables"].append(filtered_table)
            else:
                logger.warning(f"All columns in {self.schema['name']}:{table['name']} are empty.")

        return filtered_schema
