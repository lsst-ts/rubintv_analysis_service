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

import copy
import itertools
import math
import os
import threading
import time
from unittest.mock import patch

import astropy.table
import lsst.rubintv.analysis.service as lras
import numpy as np
import sqlalchemy
import utils
import yaml


class TestDatabase(utils.RasTestCase):
    def test_get_table_names(self):
        table_names = self.database.get_table_names()
        # The transformed EFD tables come after the instrument's own, and the
        # unpivoted ones are never served.
        self.assertTupleEqual(
            table_names,
            (
                "exposure",
                "visit1",
                "visit1_quicklook",
                "exposure_efd",
                "visit1_efd",
            ),
        )

    def test_get_table_schema(self):
        schema = lras.database.get_table_schema(self.database.schema, "exposure")
        self.assertEqual(schema["name"], "exposure")

        columns = [
            "exposure_id",
            "seq_num",
            "day_obs",
            "ra",
            "dec",
            "physical_filter",
            "obs_start",
            "obs_start_mjd",
        ]
        for n, column in enumerate(schema["columns"]):
            self.assertEqual(column["name"], columns[n])

    def test_single_table_query_columns(self):
        truth = utils.get_test_data("exposure")
        valid = (truth["exposure.ra"] != None) & (truth["exposure.dec"] != None)  # noqa: E711
        truth = truth[valid]
        truth = truth["exposure.ra", "exposure.dec", "exposure.day_obs", "exposure.seq_num"]
        data = self.database.query(columns=["exposure.ra", "exposure.dec"])
        self.assertDataTableEqual(data, truth)  # type: ignore

    def test_multiple_table_query_columns(self):
        visit_truth = utils.get_test_data("exposure")
        exp_truth = utils.get_test_data("visit1_quicklook")
        truth = astropy.table.join(
            visit_truth,
            exp_truth,
            keys_left=("exposure.exposure_id"),
            keys_right=("visit1_quicklook.visit_id"),
        )
        valid = (truth["exposure.ra"] != None) & (truth["exposure.dec"] != None)  # noqa: E711
        truth = truth[valid]
        truth = truth[
            "exposure.ra",
            "exposure.dec",
            "visit1_quicklook.visit_id",
            "exposure.day_obs",
            "exposure.seq_num",
        ]

        data = self.database.query(columns=["exposure.ra", "exposure.dec", "visit1_quicklook.visit_id"])

        self.assertDataTableEqual(data, truth)

    def test_join_anchors_on_the_most_connected_table(self):
        """The `FROM` clause must start from a deterministic hub table.

        `build_join` anchors on its first table, and it receives a `set`, so
        without an explicit sort the anchor depends on set iteration order and
        identical requests can emit different (though equivalent) SQL. Sorting
        most-connected-first also keeps the anchor on a hub table, which keeps
        the join paths short.
        """
        builder = self.database.joins
        # `{"exposure", "visit1"}` iterates visit1-first, so an unsorted
        # builder anchors on visit1 and emits the visit1-leading join seen in
        # production logs. Sorting must anchor on the exposure hub instead.
        tables = {"exposure", "visit1"}

        # Feed the builder every permutation. A set of the same strings
        # iterates consistently within one process, so permuting the input
        # alone would not exercise the ordering; assert directly that the
        # anchor is the most connected table however the set was built.
        degree = {name: len(builder.join_graph[name]) for name in tables}
        expected = min(tables, key=lambda name: (-degree[name], name))

        joins = set()
        for order in itertools.permutations(tables):
            join = builder.build_join(set(order))
            joins.add(str(join))
            # The anchor is the leftmost table in the rendered `FROM` clause.
            self.assertTrue(
                str(join).startswith(expected),
                f"Expected anchor {expected}, got {str(join)[:60]!r}",
            )

        self.assertEqual(len(joins), 1, f"Join order leaked into the SQL: {joins}")
        sql = joins.pop()
        for table in tables:
            self.assertIn(table, sql)

    def test_count_uses_a_single_count_star(self):
        """Counting emits one `count(*)`, not one `count` per column.

        Every selected column is already forced `IS NOT NULL`, so the
        per-column counts are necessarily identical and the extra aggregates
        are wasted work. The per-column response shape is part of the client
        contract, so it must survive the change.
        """
        columns = ["exposure.ra", "exposure.dec"]
        statements = []
        original = lras.database.ConsDbSchema.fetch_data

        def spy(db_self, query_model):
            statements.append(str(query_model))
            return original(db_self, query_model)

        lras.database.ConsDbSchema.fetch_data = spy
        try:
            counts = self.database.query(columns, aggregator="count")
            sums = self.database.query(columns, aggregator="sum")
        finally:
            lras.database.ConsDbSchema.fetch_data = original

        count_sql, sum_sql = statements
        self.assertIn("count(*)", count_sql)
        self.assertNotIn("count(exposure.ra)", count_sql)
        # The client still receives one entry per requested column.
        self.assertEqual(counts, {"exposure.ra": 7, "exposure.dec": 7})

        # Other aggregators keep their per-column aggregates.
        self.assertIn("sum(exposure.ra)", sum_sql)
        self.assertEqual(sums, {"exposure.ra": 370.0, "exposure.dec": 20.0})

    def test_invalid_aggregator_is_rejected(self):
        """An unknown aggregator raises rather than reaching the database.

        `sqlalchemy.func` synthesises any name it is asked for instead of
        raising, so an unchecked aggregator becomes an unknown-function error
        from the database rather than a clear rejection.
        """
        columns = ["exposure.ra", "exposure.dec"]

        with self.assertRaises(ValueError) as context:
            self.database.query(columns, aggregator="bogus")
        self.assertIn("bogus", str(context.exception))

        # The rejection happens before the database is touched.
        statements = []
        original = lras.database.ConsDbSchema.fetch_data

        def spy(db_self, query_model):
            statements.append(str(query_model))
            return original(db_self, query_model)

        lras.database.ConsDbSchema.fetch_data = spy
        try:
            with self.assertRaises(ValueError):
                self.database.query(columns, aggregator="drop")
            self.assertEqual(statements, [])

            # Every supported aggregator is still accepted, in any case.
            for aggregator in ("count", "SUM", "avg", "min", "max"):
                self.database.query(columns, aggregator=aggregator)
            self.assertEqual(len(statements), 5)
        finally:
            lras.database.ConsDbSchema.fetch_data = original

    def test_calculate_bounds(self):
        result = self.database.calculate_bounds("exposure.dec")
        self.assertTupleEqual(result, (-40, 50))

    def test_columns_with_values(self):
        """The batched check must agree with the per-column one it replaces."""
        column_names = ["visit_id", "exp_time", "empty_column"]
        populated = self.database.columns_with_values("visit1_quicklook", column_names)

        self.assertEqual(populated, {"visit_id", "exp_time"})
        self.assertEqual(
            populated,
            {name for name in column_names if self.database.has_non_null_values(f"visit1_quicklook.{name}")},
        )

    def test_columns_with_values_unknown_table(self):
        self.assertEqual(self.database.columns_with_values("not_a_table", ["a"]), set())

    def test_verified_schema_drops_empty_columns(self):
        schema = self.database.get_verified_schema()

        columns = {
            f"{table['name']}.{column['name']}" for table in schema["tables"] for column in table["columns"]
        }
        self.assertIn("visit1_quicklook.visit_id", columns)
        self.assertNotIn("visit1_quicklook.empty_column", columns)

        # The schema the instance was built with must not be modified, since
        # it is shared with whoever constructed it.
        source_columns = {
            column["name"]
            for table in self.database.schema["tables"]
            if table["name"] == "visit1_quicklook"
            for column in table["columns"]
        }
        self.assertIn("empty_column", source_columns)

    def test_verified_schema_is_cached(self):
        first = self.database.get_verified_schema()
        self.assertIs(self.database.get_verified_schema(), first)

        # An explicit refresh recalculates, and still filters correctly.
        refreshed = self.database.refresh_verified_schema()
        self.assertIsNot(refreshed, first)
        self.assertEqual(
            {t["name"] for t in refreshed["tables"]},
            {t["name"] for t in first["tables"]},
        )

    def test_refresh_never_exposes_a_missing_schema(self):
        """A request during a refresh must see the old schema, not None.

        The refresh runs on a background thread while requests are served from
        the main one, so building the new schema before swapping it in is what
        keeps a concurrent reader from recalculating it itself.
        """
        first = self.database.get_verified_schema()
        observed = []

        def read_repeatedly():
            for _ in range(50):
                observed.append(self.database.get_verified_schema())
                time.sleep(0.001)

        original = self.database._calculate_verified_schema

        def slow_calculate():
            time.sleep(0.05)
            return original()

        self.database._calculate_verified_schema = slow_calculate
        reader = threading.Thread(target=read_repeatedly)
        reader.start()
        self.database.refresh_verified_schema()
        reader.join()

        self.assertTrue(all(schema is not None for schema in observed))
        self.assertTrue(all(len(schema["tables"]) == len(first["tables"]) for schema in observed))

    def test_background_refresh(self):
        first = self.database.get_verified_schema()
        thread = self.database.start_background_refresh(interval=0.05)
        self.addCleanup(self.database.stop_background_refresh)

        self.assertTrue(thread.daemon)
        for _ in range(100):
            if self.database.get_verified_schema() is not first:
                break
            time.sleep(0.01)
        else:
            self.fail("the background thread did not refresh the schema")

        self.database.stop_background_refresh()
        thread.join(timeout=2)
        self.assertFalse(thread.is_alive())

    def test_background_refresh_survives_failure(self):
        """A refresh that raises leaves the previous schema in place."""
        good = self.database.get_verified_schema()

        def fail():
            raise RuntimeError("simulated failure")

        self.database._calculate_verified_schema = fail
        thread = self.database.start_background_refresh(interval=0.02)
        self.addCleanup(self.database.stop_background_refresh)

        time.sleep(0.1)
        self.assertIs(self.database.get_verified_schema(), good)
        self.assertTrue(thread.is_alive())

    def test_verified_schema_ttl_expires(self):
        database = lras.database.ConsDbSchema(
            engine=self.database.engine,
            schema=self.database.schema,
            join_templates=[],
            verified_schema_ttl=0,
        )
        first = database.get_verified_schema()
        # With no TTL every call recalculates rather than returning the cache.
        self.assertIsNot(database.get_verified_schema(), first)


class TestNonFiniteRows(utils.RasTestCase):
    def test_drop_non_finite_rows(self):
        data = {
            "a": [1.0, math.nan, 3.0, math.inf, 5.0],
            "b": [1, 2, 3, 4, 5],
            "c": ["p", "q", "r", "s", "t"],
        }
        self.assertEqual(
            lras.database.drop_non_finite_rows(data),
            {"a": [1.0, 3.0, 5.0], "b": [1, 3, 5], "c": ["p", "r", "t"]},
        )

    def test_finite_data_is_returned_as_is(self):
        data = {"a": [1.0, 2.0], "b": [None, "x"]}
        self.assertIs(lras.database.drop_non_finite_rows(data), data)
        self.assertEqual(lras.database.drop_non_finite_rows({}), {})

    def test_query_drops_non_finite_rows(self):
        # sqlite stores NaN as NULL, so the rows come from a patched fetch.
        rows = {
            "exposure.ra": [1.0, math.nan, 3.0],
            "exposure.day_obs": [20230519] * 3,
            "exposure.seq_num": [0, 1, 2],
        }
        with patch.object(self.database, "fetch_data", return_value=rows):
            result = self.database.query(["exposure.ra"])
        self.assertEqual(result["exposure.seq_num"], [0, 2])
        self.assertEqual(result["exposure.ra"], [1.0, 3.0])

    def test_aggregates_are_not_filtered(self):
        with patch.object(self.database, "fetch_data", return_value={"avg_1": [math.nan]}):
            result = self.database.query(["exposure.ra"], aggregator="avg")
        self.assertTrue(math.isnan(result["exposure.ra"]))


class TestTransformedEfd(utils.RasTestCase):
    """The transformed EFD tables, which live in a schema of their own.

    In ConsDB `exposure_efd` and `visit1_efd` are in `efd_<instrument>`, not
    `cdb_<instrument>`; the fixture attaches a second sqlite database under
    `utils.EFD_SCHEMA_NAME` to reproduce that.
    """

    def test_efd_tables_are_reflected_from_their_own_schema(self):
        self.assertEqual(self.database.get_table("exposure_efd").schema, utils.EFD_SCHEMA_NAME)
        self.assertEqual(self.database.get_table("visit1_efd").schema, utils.EFD_SCHEMA_NAME)
        self.assertIsNone(self.database.get_table("exposure").schema)

    def test_efd_tables_belong_to_the_instrument(self):
        # The client addresses a table by its instrument's database name, so
        # the EFD tables report the ConsDB schema's, not their own.
        data_id = self.database.get_data_id("exposure_efd")
        self.assertEqual(data_id.database, self.schema["name"])
        self.assertEqual(data_id.table, "exposure_efd")

    def test_unpivoted_tables_are_not_served(self):
        for table in ("exposure_efd_unpivoted", "visit1_efd_unpivoted"):
            self.assertNotIn(table, self.database.tables)
            self.assertNotIn(table, self.database.get_table_names())

    def test_efd_columns_are_in_the_schema(self):
        table = lras.database.get_table_schema(self.database.schema, "exposure_efd")
        names = [column["name"] for column in table["columns"]]
        self.assertIn("mount_jitter", names)
        self.assertIn("created_at", names)

    def test_query_joins_exposure_to_its_efd_row(self):
        exposure = utils.get_test_data("exposure")
        efd = utils.get_test_data("exposure_efd")
        truth = astropy.table.join(
            exposure, efd, keys_left="exposure.exposure_id", keys_right="exposure_efd.exposure_id"
        )
        has_ra = truth["exposure.ra"] != None  # noqa: E711
        has_temperature = truth["exposure_efd.dome_temperature"] != None  # noqa: E711
        truth = truth[has_ra & has_temperature]
        truth = truth["exposure.ra", "exposure_efd.dome_temperature", "exposure.day_obs", "exposure.seq_num"]

        data = self.database.query(columns=["exposure.ra", "exposure_efd.dome_temperature"])

        self.assertDataTableEqual(data, truth)

    def test_query_of_only_efd_columns_takes_data_ids_from_the_hub_table(self):
        # The data ids come from visit1, so the visit1_efd rows for visits
        # that visit1 has are returned, with visit1's day_obs and seq_num.
        visits = utils.get_exposure_data_dict("visit1", "visit_id")
        efd = utils.get_visit_efd_data_dict()
        kept = [
            n
            for n, visit_id in enumerate(visits["visit1.visit_id"])
            if visit_id in efd["visit1_efd.visit_id"]
        ]

        data = self.database.query(columns=["visit1_efd.sonic_temperature_mean"])

        np.testing.assert_array_equal(
            data["visit1_efd.sonic_temperature_mean"], efd["visit1_efd.sonic_temperature_mean"]
        )
        np.testing.assert_array_equal(data["day_obs"], [visits["visit1.day_obs"][n] for n in kept])
        np.testing.assert_array_equal(data["seq_num"], [visits["visit1.seq_num"][n] for n in kept])

    def test_query_joins_across_all_three(self):
        # exposure_efd and visit1_efd only meet through the exposure/visit1
        # hub tables, which the join builder has to find a path through.
        data = self.database.query(
            columns=["exposure_efd.mount_jitter", "visit1_efd.sonic_temperature_mean", "exposure.ra"]
        )
        efd = utils.get_visit_efd_data_dict()
        exposures = utils.get_exposure_data_dict("exposure", "exposure_id")
        expected = [
            visit_id
            for visit_id, ra in zip(exposures["exposure.exposure_id"], exposures["exposure.ra"])
            if ra is not None and visit_id in efd["visit1_efd.visit_id"]
        ]
        self.assertEqual(len(data["exposure.ra"]), len(expected))

    def test_count_of_efd_columns(self):
        data = self.database.query(columns=["exposure_efd.dome_temperature"], aggregator="count")
        self.assertEqual(data, {"exposure_efd.dome_temperature": 8})

    def test_calculate_bounds_of_an_efd_column(self):
        self.assertEqual(self.database.calculate_bounds("exposure_efd.mount_jitter"), (0.1, 1.0))

    def test_verified_schema_drops_the_empty_efd_column(self):
        schema = self.database.get_verified_schema()
        table = lras.database.get_table_schema(schema, "exposure_efd")
        names = [column["name"] for column in table["columns"]]
        self.assertIn("mount_jitter", names)
        self.assertNotIn("empty_metric", names)

    def test_an_efd_table_missing_from_the_database_is_dropped(self):
        # A connection with no EFD database attached: the instrument's own
        # tables are still served and the EFD tables are simply absent.
        path = os.path.dirname(__file__)
        with open(os.path.join(path, "schema.yaml")) as file:
            schema = yaml.safe_load(file)
        schema["name"] = None
        engine = sqlalchemy.create_engine("sqlite:///" + self.db_filename)
        try:
            database = lras.database.ConsDbSchema(
                schema=schema,
                engine=engine,
                join_templates=self.database.joins.joins,
                extra_schemas=[copy.deepcopy(self.efd_schema)],
            )
            self.assertTupleEqual(database.get_table_names(), ("exposure", "visit1", "visit1_quicklook"))
            self.assertNotIn("exposure_efd", database.tables)
        finally:
            engine.dispose()

    def test_without_extra_schemas_nothing_changes(self):
        path = os.path.dirname(__file__)
        with open(os.path.join(path, "schema.yaml")) as file:
            schema = yaml.safe_load(file)
        schema["name"] = None
        database = lras.database.ConsDbSchema(
            schema=schema, engine=self.database.engine, join_templates=self.database.joins.joins
        )
        self.assertTupleEqual(database.get_table_names(), ("exposure", "visit1", "visit1_quicklook"))
