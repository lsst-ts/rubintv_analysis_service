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

import os
import tempfile
from unittest import TestCase
from unittest.mock import Mock

import sqlalchemy
from lsst.rubintv.analysis.service.database import DatabaseConnectionError, check_connection


class TestCheckConnection(TestCase):
    def test_reachable_database_passes(self):
        engine = sqlalchemy.create_engine("sqlite://")
        try:
            check_connection(engine)
        finally:
            engine.dispose()

    def test_unreachable_database_raises(self):
        # sqlite cannot create a database in a directory that does not exist,
        # so connecting fails with the same OperationalError an unreachable
        # Postgres server raises.
        with tempfile.TemporaryDirectory() as tmp:
            engine = sqlalchemy.create_engine("sqlite:///" + os.path.join(tmp, "missing", "test.db"))
            try:
                with self.assertRaises(DatabaseConnectionError) as context:
                    check_connection(engine)
            finally:
                engine.dispose()
        self.assertIsInstance(context.exception.__cause__, sqlalchemy.exc.OperationalError)
        self.assertIn("unable to open database file", str(context.exception))

    def test_password_is_not_in_the_message(self):
        # The message ends up in pod logs, which must not carry the password
        # from the credentials file. A stand-in engine, because the Postgres
        # driver is not a dependency of the tests.
        engine = Mock()
        engine.url = sqlalchemy.engine.make_url("postgresql://user:secret@consdb.example/exposurelog")
        engine.connect.side_effect = sqlalchemy.exc.OperationalError(
            None, None, OSError("connection to server failed: No route to host\n")
        )
        with self.assertRaises(DatabaseConnectionError) as context:
            check_connection(engine)
        message = str(context.exception)
        self.assertNotIn("secret", message)
        self.assertEqual(
            message,
            "Could not connect to postgresql://user:***@consdb.example/exposurelog: "
            "connection to server failed: No route to host",
        )
