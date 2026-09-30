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

"""Locate the ConsDB schema files the worker reads at startup.

The schemas come from the ``lsst-sdm-schemas`` package on PyPI, which is
released once per science-pipelines weekly and carries the ``cdb_*.yaml``
files as package data. Pinning that version in ``config.yaml`` makes the
worker independent of whichever (often much older) ``sdm_schemas`` the
container's stack happens to bundle.

The package is installed into a private directory and the files are read
from there directly. It must not be imported: the stack's own copy sits on
``PYTHONPATH`` ahead of anything pip installs, so ``import lsst.sdm.schemas``
would silently give the stale version back.

The transformed EFD schemas (``efd_*.yaml``) are a second, separately pinned
source: they are fetched from the ``lsst-dm/consdb`` repository on GitHub,
which generates the tables they describe. See `resolve_efd_schemas_dir`.
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
import tempfile
import urllib.request
from collections.abc import Callable, Iterable, Mapping
from typing import IO, ContextManager

__all__ = [
    "CONSDB_EFD_SCHEMAS_PATH",
    "CONSDB_REPO",
    "EFD_SCHEMA_URL",
    "PACKAGE",
    "SDM_SCHEMAS_ENV",
    "SchemaLookupError",
    "default_efd_install_root",
    "default_install_root",
    "fetch_efd_schemas",
    "install_schemas",
    "installed_efd_schemas_dir",
    "installed_schemas_dir",
    "resolve_efd_schemas_dir",
    "resolve_schemas_dir",
]

logger = logging.getLogger("lsst.rubintv.analysis.service.schemas")

PACKAGE = "lsst-sdm-schemas"
"""The PyPI distribution that carries the ConsDB schema files."""

SDM_SCHEMAS_ENV = "SDM_SCHEMAS_DIR"
"""The eups variable pointing at an ``sdm_schemas`` checkout, the fallback."""

# The distribution's files inside an install target, and the dist-info
# directory pip writes next to them, which is how we tell a complete install
# from a directory left behind by an interrupted one.
_SCHEMAS_SUBDIR = os.path.join("lsst", "sdm", "schemas")
_DIST_INFO = "lsst_sdm_schemas-{version}.dist-info"

Runner = Callable[..., object]


class SchemaLookupError(RuntimeError):
    """No usable directory of schema files could be found."""


def default_install_root() -> str:
    """The directory versions of the package are installed under.

    Lives under the system temporary directory rather than the user's home:
    the worker pod runs as a uid with no passwd entry, so home is unreliable,
    whereas ``/tmp`` is writable in the image.
    """
    return os.path.join(tempfile.gettempdir(), "rubintv-ddv", "sdm_schemas")


def _target(root: str, version: str) -> str:
    return os.path.join(root, version)


def installed_schemas_dir(version: str, root: str) -> str | None:
    """The schema files of ``version``, if already installed under ``root``.

    Parameters
    ----------
    version :
        The package version, e.g. ``"30.2026.3700"``.
    root :
        The directory versions are installed under.

    Returns
    -------
    schemas_dir :
        The directory holding the ``cdb_*.yaml`` files, or `None` if that
        version has not been (completely) installed.
    """
    target = _target(root, version)
    dist_info = os.path.join(target, _DIST_INFO.format(version=version))
    schemas_dir = os.path.join(target, _SCHEMAS_SUBDIR)
    if os.path.isdir(dist_info) and os.path.isdir(schemas_dir):
        return schemas_dir
    return None


def install_schemas(version: str, root: str, run: Runner = subprocess.run) -> str:
    """Install ``version`` of the package under ``root`` and return its files.

    Parameters
    ----------
    version :
        The package version to install.
    root :
        The directory to install under; the version gets its own
        subdirectory so that several can coexist.
    run :
        Runs the pip command. Exposed so tests can stand in for pip.

    Returns
    -------
    schemas_dir :
        The directory holding the ``cdb_*.yaml`` files.

    Raises
    ------
    subprocess.CalledProcessError
        If pip fails, typically because PyPI is unreachable.
    """
    target = _target(root, version)
    os.makedirs(target, exist_ok=True)
    command = [
        sys.executable,
        "-m",
        "pip",
        "install",
        "--quiet",
        # Only the yaml files are wanted; the package's runtime dependencies
        # are for its Python API, which is never imported (see module doc).
        "--no-deps",
        # Do not touch the user's pip cache, which may not be writable.
        "--no-cache-dir",
        # Replace whatever an interrupted install left in the target.
        "--upgrade",
        "--target",
        target,
        f"{PACKAGE}=={version}",
    ]
    logger.info(f"Installing {PACKAGE}=={version} into {target}")
    run(command, check=True)
    schemas_dir = os.path.join(target, _SCHEMAS_SUBDIR)
    if not os.path.isdir(schemas_dir):
        raise SchemaLookupError(f"{PACKAGE}=={version} installed but {schemas_dir} does not exist")
    return schemas_dir


def resolve_schemas_dir(
    version: str | None,
    explicit: str | None = None,
    environ: Mapping[str, str] = os.environ,
    root: str | None = None,
    run: Runner = subprocess.run,
) -> str:
    """Find the directory of ConsDB schema files the worker should read.

    In order of precedence:

    1. ``explicit``, a directory given on the command line;
    2. the pinned ``version`` of the ``lsst-sdm-schemas`` package, installed
       on demand and reused on later starts;
    3. ``$SDM_SCHEMAS_DIR/yml``, the eups checkout, when no version is pinned
       or the install failed (no network, say), so that an outage degrades
       to the stack's schemas rather than stopping the worker.

    Parameters
    ----------
    version :
        The package version pinned in ``config.yaml``, or `None`.
    explicit :
        A directory that already holds the schema files, or `None`.
    environ :
        The environment to read ``$SDM_SCHEMAS_DIR`` from.
    root :
        Where to install the package; defaults to `default_install_root`.
    run :
        Runs the pip command; see `install_schemas`.

    Returns
    -------
    schemas_dir :
        The directory holding the ``cdb_*.yaml`` files.

    Raises
    ------
    SchemaLookupError
        If none of the sources yields a directory.
    """
    if explicit:
        return explicit

    if version:
        if root is None:
            root = default_install_root()
        installed = installed_schemas_dir(version, root)
        if installed is not None:
            logger.info(f"Using the installed {PACKAGE}=={version}")
            return installed
        try:
            return install_schemas(version, root, run)
        except Exception as e:
            logger.error(f"Could not install {PACKAGE}=={version}: {e}")

    checkout = environ.get(SDM_SCHEMAS_ENV)
    if checkout:
        schemas_dir = os.path.join(checkout, "yml")
        logger.warning(f"Falling back to the schemas in ${SDM_SCHEMAS_ENV}: {schemas_dir}")
        return schemas_dir

    raise SchemaLookupError(
        "No ConsDB schemas: pass --schemas-dir, pin sdm_schemas_version in the config file, "
        f"or set ${SDM_SCHEMAS_ENV}"
    )


# The transformed EFD schemas.
#
# The `exposure_efd` and `visit1_efd` tables are made by the transformed EFD
# service in lsst-dm/consdb, which creates them from these files, so this is
# where they are read from. `lsst-sdm-schemas` carries copies too, but they are
# synced by hand and have lagged the database by more than a hundred columns.
# There is no PyPI release of consdb, so the files are fetched from GitHub at a
# tag pinned in `config.yaml`.

CONSDB_REPO = "lsst-dm/consdb"
"""The GitHub repository holding the transformed EFD schema files."""

CONSDB_EFD_SCHEMAS_PATH = "python/lsst/consdb/transformed_efd/schemas/yml"
"""Where the ``efd_*.yaml`` files live inside that repository."""

EFD_SCHEMA_URL = "https://raw.githubusercontent.com/{repo}/{version}/{path}/{filename}"

Opener = Callable[[str], ContextManager[IO[bytes]]]
"""Opens a URL, returning a context manager over the response body."""


def _open_url(url: str) -> ContextManager[IO[bytes]]:
    return urllib.request.urlopen(url, timeout=60)


def default_efd_install_root() -> str:
    """The directory versions of the transformed EFD schemas are kept under.

    Beside `default_install_root`, for the same reason it is under the
    temporary directory.
    """
    return os.path.join(tempfile.gettempdir(), "rubintv-ddv", "consdb_efd_schemas")


def installed_efd_schemas_dir(version: str, filenames: Iterable[str], root: str) -> str | None:
    """The transformed EFD schema files of ``version``, if all already fetched.

    Parameters
    ----------
    version :
        The consdb git tag, e.g. ``"26.9.1"``.
    filenames :
        The files that have to be present, e.g. ``["efd_latiss.yaml"]``.
    root :
        The directory versions are kept under.

    Returns
    -------
    schemas_dir :
        The directory holding every file, or `None` if any is missing.
    """
    target = _target(root, version)
    if all(os.path.isfile(os.path.join(target, filename)) for filename in filenames):
        return target
    return None


def fetch_efd_schemas(version: str, filenames: Iterable[str], root: str, opener: Opener = _open_url) -> str:
    """Fetch the transformed EFD schema files of ``version`` under ``root``.

    Files already present are kept, so an earlier partial fetch is completed
    rather than repeated. Each file is written under a temporary name and
    renamed into place, so an interrupted download is never mistaken for a
    complete file on a later start.

    Parameters
    ----------
    version :
        The consdb git tag to fetch from.
    filenames :
        The files to fetch.
    root :
        The directory to keep them under; the version gets its own
        subdirectory so that several can coexist.
    opener :
        Opens a URL. Exposed so tests can stand in for the network.

    Returns
    -------
    schemas_dir :
        The directory holding the files.

    Raises
    ------
    Exception
        Whatever ``opener`` raises, typically because GitHub is unreachable
        or the tag does not exist.
    """
    target = _target(root, version)
    os.makedirs(target, exist_ok=True)
    for filename in filenames:
        path = os.path.join(target, filename)
        if os.path.isfile(path):
            continue
        url = EFD_SCHEMA_URL.format(
            repo=CONSDB_REPO, version=version, path=CONSDB_EFD_SCHEMAS_PATH, filename=filename
        )
        logger.info(f"Fetching {url}")
        with opener(url) as response:
            content = response.read()
        partial = path + ".part"
        with open(partial, "wb") as file:
            file.write(content)
        os.replace(partial, path)
    return target


def resolve_efd_schemas_dir(
    version: str | None,
    filenames: Iterable[str],
    explicit: str | None = None,
    root: str | None = None,
    opener: Opener = _open_url,
) -> str | None:
    """Find the directory of transformed EFD schema files, if any.

    In order of precedence:

    1. ``explicit``, a directory given on the command line;
    2. the pinned consdb ``version``, fetched on demand and reused on later
       starts.

    Unlike `resolve_schemas_dir` there is no fallback and no error: the
    transformed EFD tables are an addition to an instrument's schema, so when
    they cannot be read the worker serves the instrument without them rather
    than not at all.

    Parameters
    ----------
    version :
        The consdb git tag pinned in ``config.yaml``, or `None`.
    filenames :
        The files needed, e.g. ``["efd_latiss.yaml", "efd_lsstcam.yaml"]``.
    explicit :
        A directory that already holds the files, or `None`.
    root :
        Where to keep fetched files; defaults to `default_efd_install_root`.
    opener :
        Opens a URL; see `fetch_efd_schemas`.

    Returns
    -------
    schemas_dir :
        The directory holding the files, or `None` if there is none.
    """
    if explicit:
        return explicit

    filenames = list(filenames)
    if not filenames:
        return None
    if not version:
        logger.warning("Transformed EFD schemas are configured but no consdb_version is pinned")
        return None

    if root is None:
        root = default_efd_install_root()
    installed = installed_efd_schemas_dir(version, filenames, root)
    if installed is not None:
        logger.info(f"Using the fetched transformed EFD schemas from {CONSDB_REPO} {version}")
        return installed
    try:
        return fetch_efd_schemas(version, filenames, root, opener)
    except Exception as e:
        logger.error(f"Could not fetch the transformed EFD schemas from {CONSDB_REPO} {version}: {e}")
        return None
