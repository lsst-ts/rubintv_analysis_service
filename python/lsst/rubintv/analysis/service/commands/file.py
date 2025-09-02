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
import os
import shutil
from dataclasses import dataclass

from ..command import BaseCommand
from ..data import DataCenter

logger = logging.getLogger("lsst.rubintv.analysis.service.commands.file")
MAX_FILE_SIZE = 10 * 1024 * 1024  # 10 MB


def sanitize_path(base_path: str, user_path: list[str]) -> str:
    """Sanitize and validate a user-provided path.

    Parameters
    ----------
    base_path
        The root directory that shouldn't be escaped.
    user_path
        List of path components provided by the user.

    Returns
    -------
    result:
        A sanitized absolute path, or None if the path is invalid.
    """
    # Join the path components and normalize
    full_path = os.path.normpath(os.path.join(base_path, *user_path))

    # Check if the resulting path is within the base_path
    if not full_path.startswith(os.path.abspath(base_path)):
        raise ValueError(f"Invalid path: {full_path}")

    return full_path


class FileOperationError(Exception):
    """Custom exception for file operations."""

    pass


@dataclass(kw_only=True)
class LoadDirectoryCommand(BaseCommand):
    """Load the files and sub directories contained in a directory.

    Attributes
    ----------
    path
        The path to the directory to list.
    response_type
        The type of response to send back to the client.
    """

    path: list[str]
    response_type: str = "directory files"

    def build_contents(self, data_center: DataCenter) -> dict:
        try:
            full_path = sanitize_path(data_center.user_path, self.path)
            if full_path is None:
                raise FileOperationError("Invalid path")

            if not os.path.exists(full_path):
                raise FileOperationError(f"The path '{full_path}' does not exist.")

            if not os.path.isdir(full_path):
                raise FileOperationError(f"The path '{full_path}' is not a directory.")

            all_items = os.listdir(full_path)
            files = [f for f in all_items if os.path.isfile(os.path.join(full_path, f))]
            directories = [d for d in all_items if os.path.isdir(os.path.join(full_path, d))]
            # strip logs directory
            log_dir = data_center.logs_directory_name
            directories = [d for d in directories if d != log_dir]

            return {
                "path": self.path,
                "files": sorted(files),
                "directories": sorted(directories),
            }
        except FileOperationError as e:
            logger.error(f"File operation error: {str(e)}")
            return {"error": str(e)}
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            return {"error": f"An unexpected error occurred: {str(e)}"}

    def get_log_metadata(self) -> dict:
        """Log metadata specific to directory operations."""
        base_metadata = super().get_log_metadata()
        return {**base_metadata, "path": "/".join(self.path), "path_depth": len(self.path)}


@dataclass(kw_only=True)
class CreateDirectoryCommand(BaseCommand):
    """Create a new directory.

    Attributes
    ----------
    path
        The path to the parent directory.
    name
        The name of the new directory to create.
    """

    path: list[str]
    name: str
    response_type: str = "directory created"

    def build_contents(self, data_center: DataCenter) -> dict:
        try:
            parent_path = sanitize_path(data_center.user_path, self.path)
            if parent_path is None:
                raise FileOperationError("Invalid path")

            full_path = os.path.join(parent_path, self.name)
            if not full_path.startswith(data_center.user_path):
                raise FileOperationError("Invalid directory name")

            os.makedirs(full_path, exist_ok=True)
            logger.info(f"Directory created: {full_path}")
            return {"path": full_path, "parent_path": self.path, "name": self.name}
        except FileOperationError as e:
            logger.error(f"File operation error: {str(e)}")
            return {"error": str(e)}
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            return {"error": f"An unexpected error occurred: {str(e)}"}

    def get_log_metadata(self):
        base_metadata = super().get_log_metadata()
        return {
            **base_metadata,
            "path": self.path,
            "name": self.name,
        }


@dataclass(kw_only=True)
class RenameFileCommand(BaseCommand):
    """Rename a file or directory.

    Attributes
    ----------
    path
        The path to the file or directory to rename.
    new_name
        The new name to assign to the file or directory.
    """

    path: list[str]
    new_name: str
    response_type: str = "file renamed"

    def build_contents(self, data_center: DataCenter) -> dict:
        try:
            full_path = sanitize_path(data_center.user_path, self.path)
            if full_path is None:
                raise FileOperationError("Invalid path")

            new_path = os.path.join(os.path.dirname(full_path), self.new_name)
            if not new_path.startswith(data_center.user_path):
                raise FileOperationError("Invalid new name")

            if not os.path.exists(full_path):
                raise FileOperationError(f"The source path '{full_path}' does not exist.")

            if os.path.exists(new_path):
                raise FileOperationError(f"The new path '{new_path}' already exists. Cannot overwrite.")

            os.rename(full_path, new_path)
            logger.info(f"File renamed: {full_path} to {new_path}")
            return {"new_path": new_path, "new_name": self.new_name, "path": self.path}
        except FileOperationError as e:
            logger.error(f"File operation error: {str(e)}")
            return {"error": str(e)}
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            return {"error": f"An unexpected error occurred: {str(e)}"}

    def get_log_metadata(self):
        base_metadata = super().get_log_metadata()
        return {
            **base_metadata,
            "old_path": self.path,
            "new_name": self.new_name,
        }


@dataclass(kw_only=True)
class DeleteFileCommand(BaseCommand):
    """Delete a file or directory.

    Attributes
    ----------
    path
        The path to the file or directory to delete.
    """

    path: list[str]
    response_type: str = "file deleted"

    def build_contents(self, data_center: DataCenter) -> dict:
        try:
            full_path = sanitize_path(data_center.user_path, self.path)
            if full_path is None:
                raise FileOperationError("Invalid path")

            if not os.path.exists(full_path):
                raise FileOperationError(f"The path '{full_path}' does not exist.")

            if os.path.isfile(full_path):
                os.remove(full_path)
                logger.info(f"File deleted: {full_path}")
                return {"deleted_path": self.path, "type": "file"}
            elif os.path.isdir(full_path):
                shutil.rmtree(full_path)
                logger.info(f"Directory deleted: {full_path}")
                return {"deleted_path": self.path, "type": "directory"}
            else:
                raise FileOperationError(f"The path '{full_path}' is neither a file nor a directory.")
        except FileOperationError as e:
            logger.error(f"File operation error: {str(e)}")
            return {"error": str(e)}
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            return {"error": f"An unexpected error occurred: {str(e)}"}

    def get_log_metadata(self) -> dict:
        base_metadata = super().get_log_metadata()
        return {
            **base_metadata,
            "path": self.path,
        }


@dataclass(kw_only=True)
class DuplicateFileCommand(BaseCommand):
    """Duplicate a file or directory.

    Attributes
    ----------
    path
        The path to the file or directory to duplicate.
    """

    path: list[str]
    response_type: str = "file duplicated"

    def build_contents(self, data_center: DataCenter) -> dict:
        try:
            full_path = sanitize_path(data_center.user_path, self.path)
            if full_path is None:
                raise FileOperationError("Invalid path")

            if not os.path.exists(full_path):
                raise FileOperationError(f"The path '{full_path}' does not exist.")

            dir_path = os.path.dirname(full_path)
            base_name = os.path.basename(full_path)
            new_path = os.path.join(dir_path, f"{base_name}_copy")
            counter = 1

            while os.path.exists(new_path):
                new_path = os.path.join(dir_path, f"{base_name}_copy_{counter}")
                counter += 1

            new_filename = os.path.basename(new_path)

            if os.path.isfile(full_path):
                shutil.copy2(full_path, new_path)
                logger.info(f"File duplicated: {full_path} to {new_path}")
                return {
                    "path": self.path[:-1],
                    "old_name": self.path[-1],
                    "new_filename": new_filename,
                    "type": "file",
                }
            elif os.path.isdir(full_path):
                shutil.copytree(full_path, new_path)
                logger.info(f"Directory duplicated: {full_path} to {new_path}")
                return {"new_path": new_path, "type": "directory"}
            else:
                raise FileOperationError(f"The path '{full_path}' is neither a file nor a directory.")
        except FileOperationError as e:
            logger.error(f"File operation error: {str(e)}")
            return {"error": str(e)}
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            return {"error": f"An unexpected error occurred: {str(e)}"}

    def get_log_metadata(self) -> dict:
        base_metadata = super().get_log_metadata()
        return {
            **base_metadata,
            "path": self.path,
        }


@dataclass(kw_only=True)
class MoveFileCommand(BaseCommand):
    """Move a file or directory.

    Attributes
    ----------
    source_path
        The path to the file or directory to move.
    destination_path
        The path to move the file or directory to.
    """

    source_path: list[str]
    destination_path: list[str]
    response_type: str = "file moved"

    def build_contents(self, data_center: DataCenter) -> dict:
        try:
            full_source_path = sanitize_path(data_center.user_path, self.source_path)
            destination_path = sanitize_path(data_center.user_path, self.destination_path)
            full_destination_path = os.path.join(destination_path, os.path.basename(full_source_path))

            if full_source_path is None or full_destination_path is None:
                raise FileOperationError("Invalid path")

            if not os.path.exists(full_source_path):
                raise FileOperationError(f"The source path '{full_source_path}' does not exist.")

            if os.path.exists(full_destination_path):
                raise FileOperationError(
                    f"The destination path '{full_destination_path}' already exists. "
                    "Use overwrite=True to overwrite."
                )

            os.makedirs(os.path.dirname(full_destination_path), exist_ok=True)
            shutil.move(full_source_path, full_destination_path)

            logger.info(f"File moved: {full_source_path} to {full_destination_path}")
            return {
                "destination_path": self.destination_path,
                "source_path": self.source_path,
                "type": "file" if os.path.isfile(full_destination_path) else "directory",
            }
        except FileOperationError as e:
            logger.error(f"File operation error: {str(e)}")
            return {"error": str(e)}
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            return {"error": f"An unexpected error occurred: {str(e)}"}

    def get_log_metadata(self) -> dict:
        base_metadata = super().get_log_metadata()
        return {
            **base_metadata,
            "source_path": self.source_path,
            "destination_path": self.destination_path,
        }


@dataclass(kw_only=True)
class SaveFileCommand(BaseCommand):
    """Save a file with the provided content.

    Attributes
    ----------
    path
        The path to the file to save.
    content
        The content to write to the file.
    """

    path: list[str]
    content: str
    response_type: str = "file saved"

    def build_contents(self, data_center: DataCenter) -> dict:
        try:
            full_path = sanitize_path(data_center.user_path, self.path)
            if full_path is None:
                raise FileOperationError("Invalid path")

            if os.path.exists(full_path) and os.path.isdir(full_path):
                raise FileOperationError(f"The path '{full_path}' already exists as a directory.")

            with open(full_path, "w", encoding="utf-8") as f:
                f.write(self.content)

            logger.info(f"File saved: {full_path}")
            return {"saved_path": full_path}
        except FileOperationError as e:
            logger.error(f"File operation error: {str(e)}")
            return {"error": str(e)}
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            return {"error": f"An unexpected error occurred: {str(e)}"}

    def get_log_metadata(self) -> dict:
        base_metadata = super().get_log_metadata()
        return {
            **base_metadata,
            "path": self.path,
            "file_size_bytes": len(self.content) if hasattr(self, "content") else 0,
        }


@dataclass(kw_only=True)
class LoadFileCommand(BaseCommand):
    """Load the contents of a file.

    Attributes
    ----------
    path
        The path to the file to load.
    max_size
        The maximum allowed size of the file in bytes.
        This prevents accendentally loading large files.
    """

    path: list[str]
    max_size: int = MAX_FILE_SIZE
    response_type: str = "file content"

    def build_contents(self, data_center: DataCenter) -> dict:
        try:
            full_path = sanitize_path(data_center.user_path, self.path)
            if full_path is None:
                raise FileOperationError("Invalid path")

            if not os.path.exists(full_path):
                raise FileOperationError(f"The file '{full_path}' does not exist.")

            if not os.path.isfile(full_path):
                raise FileOperationError(f"The path '{full_path}' is not a file.")

            file_size = os.path.getsize(full_path)
            if file_size > self.max_size:
                raise FileOperationError(
                    f"The file '{full_path}' exceeds the maximum allowed size of {self.max_size} bytes."
                )

            with open(full_path, "r", encoding="utf-8") as f:
                content = f.read()

            logger.info(f"File loaded: {full_path}")
            return {"content": content, "path": full_path, "size": file_size, "encoding": "utf-8"}
        except FileOperationError as e:
            logger.error(f"File operation error: {str(e)}")
            return {"error": str(e)}
        except UnicodeDecodeError:
            logger.error(f"Unicode decode error: {full_path}")
            return {
                "error": f"Unable to decode '{full_path}' as UTF-8. "
                "The file might be binary or use a different encoding."
            }
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            return {"error": f"An unexpected error occurred: {str(e)}"}

    def get_log_metadata(self) -> dict:
        base_metadata = super().get_log_metadata()
        return {
            **base_metadata,
            "path": self.path,
        }


# Register the commands
LoadDirectoryCommand.register("list directory")
CreateDirectoryCommand.register("create directory")
RenameFileCommand.register("rename")
DeleteFileCommand.register("delete")
DuplicateFileCommand.register("duplicate")
MoveFileCommand.register("move")
LoadFileCommand.register("load")
