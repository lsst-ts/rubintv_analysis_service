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
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
import tempfile
from collections.abc import Callable, Mapping

__all__ = [
    "PACKAGE",
    "SDM_SCHEMAS_ENV",
    "SchemaLookupError",
    "default_install_root",
    "install_schemas",
    "installed_schemas_dir",
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
