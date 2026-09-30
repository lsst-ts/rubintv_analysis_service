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

import io
import os
import subprocess
import sys
import tempfile
import urllib.error
from unittest import TestCase

from lsst.rubintv.analysis.service import schemas

VERSION = "30.2026.3700"


def fake_pip(root: str):
    """A stand-in for pip that lays out what a real install would.

    Records each command so tests can check what would have been run.
    """
    calls: list[list[str]] = []

    def run(command, check=False):
        calls.append(command)
        target = command[command.index("--target") + 1]
        version = command[-1].split("==")[1]
        os.makedirs(os.path.join(target, "lsst", "sdm", "schemas"), exist_ok=True)
        os.makedirs(os.path.join(target, f"lsst_sdm_schemas-{version}.dist-info"), exist_ok=True)

    return run, calls


def failing_pip(command, check=False):
    raise subprocess.CalledProcessError(1, command)


class TestResolveSchemasDir(TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = os.path.join(self.tmp.name, "root")
        self.expected = os.path.join(self.root, VERSION, "lsst", "sdm", "schemas")

    def tearDown(self):
        self.tmp.cleanup()

    def test_explicit_dir_wins(self):
        run, calls = fake_pip(self.root)
        result = schemas.resolve_schemas_dir(
            VERSION, explicit="/somewhere", environ={"SDM_SCHEMAS_DIR": "/checkout"}, root=self.root, run=run
        )
        self.assertEqual(result, "/somewhere")
        self.assertEqual(calls, [])

    def test_installs_pinned_version(self):
        run, calls = fake_pip(self.root)
        result = schemas.resolve_schemas_dir(VERSION, environ={}, root=self.root, run=run)
        self.assertEqual(result, self.expected)
        self.assertEqual(len(calls), 1)
        command = calls[0]
        self.assertEqual(command[:4], [sys.executable, "-m", "pip", "install"])
        self.assertIn("--no-deps", command)
        self.assertEqual(command[-1], f"lsst-sdm-schemas=={VERSION}")
        self.assertEqual(command[command.index("--target") + 1], os.path.join(self.root, VERSION))

    def test_reuses_existing_install(self):
        run, calls = fake_pip(self.root)
        schemas.resolve_schemas_dir(VERSION, environ={}, root=self.root, run=run)
        result = schemas.resolve_schemas_dir(VERSION, environ={}, root=self.root, run=run)
        self.assertEqual(result, self.expected)
        self.assertEqual(len(calls), 1)

    def test_incomplete_install_is_redone(self):
        # An interrupted install leaves the files but no dist-info.
        os.makedirs(os.path.join(self.root, VERSION, "lsst", "sdm", "schemas"))
        run, calls = fake_pip(self.root)
        result = schemas.resolve_schemas_dir(VERSION, environ={}, root=self.root, run=run)
        self.assertEqual(result, self.expected)
        self.assertEqual(len(calls), 1)

    def test_install_failure_falls_back_to_checkout(self):
        result = schemas.resolve_schemas_dir(
            VERSION, environ={"SDM_SCHEMAS_DIR": "/checkout"}, root=self.root, run=failing_pip
        )
        self.assertEqual(result, os.path.join("/checkout", "yml"))

    def test_install_failure_without_checkout_raises(self):
        with self.assertRaises(schemas.SchemaLookupError):
            schemas.resolve_schemas_dir(VERSION, environ={}, root=self.root, run=failing_pip)

    def test_no_version_uses_checkout(self):
        run, calls = fake_pip(self.root)
        result = schemas.resolve_schemas_dir(
            None, environ={"SDM_SCHEMAS_DIR": "/checkout"}, root=self.root, run=run
        )
        self.assertEqual(result, os.path.join("/checkout", "yml"))
        self.assertEqual(calls, [])

    def test_nothing_configured_raises(self):
        with self.assertRaises(schemas.SchemaLookupError):
            schemas.resolve_schemas_dir(None, environ={}, root=self.root)


TAG = "26.9.1"
EFD_FILES = ["efd_latiss.yaml", "efd_lsstcam.yaml"]


def fake_opener():
    """A stand-in for urlopen that serves a small yaml naming the file.

    Records each URL so tests can check what would have been fetched.
    """
    calls: list[str] = []

    def opener(url):
        calls.append(url)
        filename = url.rsplit("/", 1)[1]
        return io.BytesIO(f"name: {filename}\n".encode())

    return opener, calls


def failing_opener(url):
    raise urllib.error.URLError("no network")


class TestResolveEfdSchemasDir(TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = os.path.join(self.tmp.name, "root")
        self.expected = os.path.join(self.root, TAG)

    def tearDown(self):
        self.tmp.cleanup()

    def resolve(self, opener, version=TAG, filenames=EFD_FILES, explicit=None):
        return schemas.resolve_efd_schemas_dir(
            version, filenames, explicit=explicit, root=self.root, opener=opener
        )

    def test_explicit_dir_wins(self):
        opener, calls = fake_opener()
        self.assertEqual(self.resolve(opener, explicit="/somewhere"), "/somewhere")
        self.assertEqual(calls, [])

    def test_fetches_pinned_tag_from_consdb(self):
        opener, calls = fake_opener()
        self.assertEqual(self.resolve(opener), self.expected)
        self.assertEqual(
            calls,
            [
                "https://raw.githubusercontent.com/lsst-dm/consdb/26.9.1"
                f"/python/lsst/consdb/transformed_efd/schemas/yml/{filename}"
                for filename in EFD_FILES
            ],
        )
        for filename in EFD_FILES:
            with open(os.path.join(self.expected, filename)) as file:
                self.assertEqual(file.read(), f"name: {filename}\n")
        self.assertEqual(sorted(os.listdir(self.expected)), sorted(EFD_FILES))

    def test_reuses_fetched_files(self):
        opener, calls = fake_opener()
        self.resolve(opener)
        self.assertEqual(self.resolve(opener), self.expected)
        self.assertEqual(len(calls), len(EFD_FILES))

    def test_completes_a_partial_fetch(self):
        # One file is already there, from a fetch that failed part way.
        os.makedirs(self.expected)
        with open(os.path.join(self.expected, EFD_FILES[0]), "w") as file:
            file.write("name: kept\n")
        opener, calls = fake_opener()
        self.assertEqual(self.resolve(opener), self.expected)
        self.assertEqual(len(calls), 1)
        self.assertTrue(calls[0].endswith(EFD_FILES[1]))
        with open(os.path.join(self.expected, EFD_FILES[0])) as file:
            self.assertEqual(file.read(), "name: kept\n")

    def test_fetch_failure_returns_none(self):
        self.assertIsNone(self.resolve(failing_opener))
        # Nothing half-written is left to be mistaken for a schema file later.
        self.assertFalse(os.path.exists(os.path.join(self.expected, EFD_FILES[0])))

    def test_no_version_returns_none(self):
        opener, calls = fake_opener()
        self.assertIsNone(self.resolve(opener, version=None))
        self.assertEqual(calls, [])

    def test_no_files_returns_none(self):
        opener, calls = fake_opener()
        self.assertIsNone(self.resolve(opener, filenames=[]))
        self.assertEqual(calls, [])
