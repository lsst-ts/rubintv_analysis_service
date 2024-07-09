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

# This module was adapted from https://github.com/fred3m/toyz and has not yet
# been tested.

from __future__ import annotations

import datetime
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import numpy as np
import scipy.ndimage
from matplotlib import cm as cmap
from matplotlib.colors import Normalize
from PIL import Image

if TYPE_CHECKING:
    from lsst.geom import Box2I

# It may be desirabe in the future to allow users to choose
# what type of image they want to send to the client.
# For now the default is sent to png.
img_formats = {
    "png": "PNG",
    "bmp": "BMP",
    "eps": "EPS",
    "gif": "GIF",
    "im": "IM",
    "jpg": "JPEG",
    "j2k": "JPEG 2000",
    "msp": "MSP",
    "pcx": "PCX",
    "pbm": "PBM",
    "pgm": "PGM",
    "ppm": "PPM",
    "spi": "SPIDER",
    "tiff": "TIFF",
    "webp": "WEBP",
    "xbm": "XBM",
}


@dataclass(kw_only=True)
class ColorMap:
    name: str = "Spectral"
    color_scale: str = "linear"
    invert_color: bool = False
    px_min: float | None = None
    px_max: float | None = None

    @property
    def set_bounds(self) -> bool:
        return self.px_min is not None and self.px_max is not None

    def copy_with(
        self,
        name: str | None = None,
        color_scale: str | None = None,
        px_min: float | None = None,
        px_max: float | None = None,
    ) -> ColorMap:
        return ColorMap(
            name=name if name is not None else self.name,
            color_scale=color_scale if color_scale is not None else self.color_scale,
            invert_color=self.invert_color,
            px_min=px_min if px_min is not None else self.px_min,
            px_max=px_max if px_max is not None else self.px_max,
        )

    def to_json(self):
        return {
            "name": self.name,
            "color_scale": self.color_scale,
            "invert_color": self.invert_color,
            "px_min": self.px_min,
            "px_max": self.px_max,
        }


@dataclass(kw_only=True)
class FileInfo:
    data_id: dict[str, Any]
    tile_width: int = 400
    tile_height: int = 200
    image_type: str = "image"
    resampling: str = "NEAREST"
    invert_x: bool = False
    invert_y: bool = False
    tile_format: str = "png"
    data: np.ndarray
    bbox: Box2I
    colormap: ColorMap

    def to_json(self):
        return {
            "data_id": self.data_id,
            "tile_width": self.tile_width,
            "tile_height": self.tile_height,
            "img_type": self.image_type,
            "resampling": self.resampling,
            "invert_x": self.invert_x,
            "invert_y": self.invert_y,
            "tile_format": self.tile_format,
            "bbox": [self.bbox.getMinX(), self.bbox.getMinY(), self.bbox.getMaxX(), self.bbox.getMaxY()],
            "colormap": self.colormap.to_json(),
        }


@dataclass(kw_only=True)
class ImageFile:
    file_info: FileInfo
    data: np.ndarray
    created: datetime.datetime
    modified: datetime.datetime


# Cached images loaded on the image worker
images_loaded: dict[str, ImageFile] = {}


class ImageViewer:
    x_center: int
    y_center: int
    width: int
    height: int
    scale: float
    left: int = 0
    bottom: int = 0
    right: int = 0
    top: int = 0

    def __init__(self, x_center: int, y_center: int, width: int, height: int, scale: float):
        self.x_center = x_center
        self.y_center = y_center
        self.width = width
        self.height = height
        self.scale = scale
        self.left = x_center - int(width / 2)
        self.right = x_center + int(width / 2)
        self.bottom = y_center - int(height / 2)
        self.top = y_center + int(height / 2)

    @staticmethod
    def best_fit(data_width: int, data_height: int, viewer_width: int, viewer_height: int):
        x_scale = viewer_width / data_width * 0.97
        y_scale = viewer_height / data_height * 0.97
        scale = min(y_scale, x_scale)
        x_center = int(np.floor(data_width / 2 * scale))
        y_center = int(np.floor(data_height / 2 * scale))
        return ImageViewer(x_center, y_center, viewer_width, viewer_height, scale)

    def to_json(self):
        return {
            "x_center": self.x_center,
            "y_center": self.y_center,
            "width": self.width,
            "height": self.height,
            "scale": self.scale,
        }


class ImageInfo:
    viewer: ImageViewer
    width: int
    height: int
    scale: float
    scaled_width: int
    scaled_height: int
    columns: int
    rows: int
    invert_x: bool = False
    invert_y: bool = False
    tiles: dict = {}
    colormap: ColorMap | None = None

    def __init__(self, file_info: FileInfo, viewer: ImageViewer):
        self.viewer = viewer
        self.width = file_info.bbox.getWidth()
        self.height = file_info.bbox.getHeight()
        self.scaled_width = int(np.ceil(self.width * self.viewer.scale))
        self.scaled_height = int(np.ceil(self.height * self.viewer.scale))
        self.columns = int(np.ceil(self.scaled_width / file_info.tile_width))
        self.rows = int(np.ceil(self.scaled_height / file_info.tile_height))


@dataclass(kw_only=True)
class TileInfo:
    idx: str
    left: int
    right: int
    top: int
    bottom: int
    y0_idx: int
    yf_idx: int
    x0_idx: int
    xf_idx: int
    loaded: bool
    row: int
    col: int
    x: int
    y: int
    width: int
    height: int

    def to_json(self):
        return {
            "idx": self.idx,
            "left": self.left,
            "right": self.right,
            "top": self.top,
            "bottom": self.bottom,
            "y0_idx": self.y0_idx,
            "yf_idx": self.yf_idx,
            "x0_idx": self.x0_idx,
            "xf_idx": self.xf_idx,
            "loaded": self.loaded,
            "row": self.row,
            "col": self.col,
            "x": self.x,
            "y": self.y,
            "width": self.width,
            "height": self.height,
        }

    def to_basic_tile_info(self):
        return BasicTileInfo(
            y0_idx=self.y0_idx,
            yf_idx=self.yf_idx,
            x0_idx=self.x0_idx,
            xf_idx=self.xf_idx,
            width=self.width,
            height=self.height,
        )


@dataclass(kw_only=True)
class BasicTileInfo:
    y0_idx: int
    yf_idx: int
    x0_idx: int
    xf_idx: int
    width: int
    height: int


def get_all_tile_info(file_info: FileInfo, img_info: ImageInfo):
    """Get info for all tiles available in the viewer.

    If the tile has not been loaded yet, it is added to the new_tiles array.
    """
    all_tiles = []
    new_tiles = {}
    if img_info.invert_x:
        xmin = img_info.width * img_info.scale - img_info.viewer.right
        xmax = img_info.width * img_info.scale - img_info.viewer.left
    else:
        xmin = img_info.viewer.left
        xmax = img_info.viewer.right
    if img_info.invert_y:
        ymin = img_info.height * img_info.scale - img_info.viewer.bottom
        ymax = img_info.height * img_info.scale - img_info.viewer.top
    else:
        ymin = img_info.viewer.top
        ymax = img_info.viewer.bottom
    min_col = int(max(1, np.floor(xmin / file_info.tile_width))) - 1
    max_col = int(min(img_info.columns, np.ceil(xmax / file_info.tile_width)))
    min_row = int(max(1, np.floor(ymin / file_info.tile_height))) - 1
    max_row = int(min(img_info.rows, np.ceil(ymax / file_info.tile_height)))

    block_width = int(np.ceil(file_info.tile_width / img_info.scale))
    block_height = int(np.ceil(file_info.tile_height / img_info.scale))

    for row in range(min_row, max_row):
        y0 = row * file_info.tile_height
        yf = (row + 1) * file_info.tile_height
        y0_idx = int(y0 / img_info.scale)
        yf_idx = min(y0_idx + block_height, img_info.height)
        for col in range(min_col, max_col):
            all_tiles.append(str(col) + "," + str(row))
            tile_idx = str(col) + "," + str(row)
            if (
                tile_idx not in img_info.tiles
                or "loaded" not in img_info.tiles[tile_idx]
                or not img_info.tiles[tile_idx]["loaded"]
            ):
                x0 = col * file_info.tile_width
                xf = (col + 1) * file_info.tile_width
                x0_idx = int(x0 / img_info.scale)
                xf_idx = min(x0_idx + block_width, img_info.width)
                tile_width = int((xf_idx - x0_idx) * img_info.scale)
                tile_height = int((yf_idx - y0_idx) * img_info.scale)
                tile = TileInfo(
                    idx=tile_idx,
                    left=x0,
                    right=xf,
                    top=y0,
                    bottom=yf,
                    y0_idx=y0_idx,
                    yf_idx=yf_idx,
                    x0_idx=x0_idx,
                    xf_idx=xf_idx,
                    loaded=False,
                    row=row,
                    col=col,
                    x=col * file_info.tile_width,
                    y=row * file_info.tile_height,
                    width=tile_width,
                    height=tile_height,
                )
                if img_info.invert_y:
                    tile.top = yf
                    tile.bottom = y0
                if img_info.invert_x:
                    tile.left = xf
                    tile.right = x0
                new_tiles[tile_idx] = tile
    print("viewer:", img_info.viewer)
    print("new tiles", new_tiles.keys())
    return all_tiles, new_tiles


def scale_data(img_info: ImageInfo, tile_info: BasicTileInfo, data: np.ndarray):
    if img_info.scale == 1:
        data = data[tile_info.y0_idx : tile_info.yf_idx, tile_info.x0_idx : tile_info.xf_idx]
    else:
        data = data[tile_info.y0_idx : tile_info.yf_idx, tile_info.x0_idx : tile_info.xf_idx]
        data = scipy.ndimage.zoom(data, img_info.scale, order=0)
    return data


def create_tile(file_info: FileInfo, img_info: ImageInfo, tile_info: BasicTileInfo) -> Image.Image | None:
    if file_info.resampling == "NEAREST":
        data = scale_data(img_info, tile_info, file_info.data)
    else:
        data = file_info.data[tile_info.y0_idx : tile_info.yf_idx, tile_info.x0_idx : tile_info.xf_idx]
    # FITS images have a flipped y-axis from what browsers
    # and other image formats expect.
    if img_info.invert_y:
        data = np.flipud(data)
    if img_info.invert_x:
        data = np.fliplr(data)

    assert img_info.colormap is not None

    norm = Normalize(img_info.colormap.px_min, img_info.colormap.px_max, True)
    colormap_name = img_info.colormap.name
    if img_info.colormap.invert_color:
        colormap_name = colormap_name + "_r"
    colormap = getattr(cmap, colormap_name)
    cm = cmap.ScalarMappable(norm, colormap)
    img = np.uint8(cm.to_rgba(data) * 255)
    img = Image.fromarray(img)
    if file_info.resampling != "NEAREST":
        img = img.resize((tile_info.width, tile_info.height), getattr(Image, file_info.resampling))

    width, height = img.size
    if width > 0 and height > 0:
        return img

    return None


def get_img_data(
    file_info: FileInfo, img_info: ImageInfo, width: int, height: int, x: int, y: int, rescale: bool = False
):
    """
    Get data from an image or FITS file
    """
    assert file_info.data is not None
    data = file_info.data

    if rescale:
        width = int(width / 2 / img_info.viewer.scale)
        height = int(height / 2 / img_info.viewer.scale)
    else:
        width = int(width / 2)
        height = int(height / 2)
    x0 = max(0, x - width)
    y0 = max(0, y - height)
    xf = min(data.shape[1], x + width)
    yf = min(data.shape[0], y + height)
    if rescale:
        tile_info = BasicTileInfo(
            y0_idx=y0,
            yf_idx=yf,
            x0_idx=x0,
            xf_idx=xf,
            width=width,
            height=height,
        )
        data = scale_data(img_info, tile_info, data)
    else:
        data = data[y0:yf, x0:xf]
    response = {
        "id": "data",
        "min": float(data.min()),
        "max": float(data.max()),
        "mean": float(data.mean()),
        "median": float(np.median(data)),
        "std_dev": float(np.std(data)),
        "data": data.tolist(),
    }

    return response


def get_point_data(file_info: FileInfo, img_info: ImageInfo, x: int, y: int) -> dict[str, Any]:
    assert file_info.data is not None
    data = file_info.data

    if x < data.shape[1] and y < data.shape[0] and x >= 0 and y >= 0:
        response = {"id": "datapoint", "px_value": float(data[y, x])}
    else:
        response = {"id": "datapoint", "px_value": 0}
    return response
