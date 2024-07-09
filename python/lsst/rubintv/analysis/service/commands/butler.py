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

from dataclasses import dataclass
from typing import TYPE_CHECKING

from ..command import BaseCommand

if TYPE_CHECKING:
    from lsst.afw.cameraGeom import Camera

    from ..data import DataCenter


def get_camera(instrument_name: str) -> Camera:
    """Load a camera based on the instrument name

    Parameters
    ----------
    instrument_name : str
        The name of the instrument.

    Returns
    -------
    camera : Camera
        The camera object.
    """
    # Import afw packages here to prevent tests from failing
    from lsst.obs.lsst import Latiss, LsstCam, LsstComCam

    instrument_name = instrument_name.lower()
    match instrument_name:
        case "lsstcam":
            camera = LsstCam.getCamera()
        case "lsstcomcam":
            camera = LsstComCam.getCamera()
        case "latiss":
            camera = Latiss.getCamera()
        case _:
            raise ValueError(f"Unsupported instrument: {instrument_name}")
    return camera


@dataclass(kw_only=True)
class LoadDetectorInfoCommand(BaseCommand):
    """Load the detector information from the Butler.

    Attributes
    ----------
    instrument : str
        The instrument name.
    """

    instrument: str
    response_type: str = "detector_info"

    def build_contents(self, data_center: DataCenter) -> dict:
        # Import afw packages here to prevent tests from failing
        from lsst.afw.cameraGeom import FOCAL_PLANE, Detector

        # Load the detector information from the Butler
        camera = get_camera(self.instrument)
        detector_info = {}
        for detector in camera:
            if isinstance(detector, Detector):
                detector_info[detector.getId()] = {
                    "corners": detector.getCorners(FOCAL_PLANE),
                    "id": detector.getId(),
                    "name": detector.getName(),
                }
        return detector_info


@dataclass(kw_only=True)
class LoadImageCommand(BaseCommand):
    """Load an image from the Butler.

    Attributes
    ----------
    collection : str
        The name of the collection to load the image from.
    image_name : str
        The name of the image to load.
    data_id : dict
        The data ID of the image. Depending on the type of image this could
        include things like "band" or "visit" or "detector".
    """

    repo: str
    image_name: str
    collection: dict
    data_id: dict
    response_type: str = "image"

    def build_contents(self, data_center: DataCenter) -> dict:
        # Load the image from the Butler
        assert data_center.butlers is not None
        image = data_center.butlers[self.repo].get(
            self.image_name, collections=[self.collection], **self.data_id
        )
        if hasattr(image, "image"):
            # Extract the Image from an Exposure or MaskedImage.
            image = image.image
        return {
            "image": image.array,
        }
