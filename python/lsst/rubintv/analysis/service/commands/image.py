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

from lsst.rubintv.analysis.service.data import DataCenter

from ..command import BaseCommand

logger = logging.getLogger("lsst.rubintv.analysis.service.commands.image")


@dataclass(kw_only=True)
class LoadDetectorImageCommand(BaseCommand):
    """Load an image from a data center.

    This command is not yet implemented, but will use the
    `viewer.py` module, adapted from `https://github.com/fred3m/toyz`
    to load image tiles and send them to the client to display
    detector images.
    """

    database: str
    detector: int
    visit_id: int

    def build_contents(self, data_center: DataCenter) -> dict:
        # butler = data_center.butler
        # assert butler is not None
        # image = butler.get(, **data_id)
        return {}
