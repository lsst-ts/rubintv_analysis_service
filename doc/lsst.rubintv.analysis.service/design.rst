.. _rubintv_analysis_service-design:

=====================================
Design of rubintv_analysis_service
=====================================

.. contents:: Table of Contents
   :depth: 2

Overview
========

The ``rubintv_analysis_service`` is a backend Python service designed to support the Derived Data Visualization (DDV) tool within the Rubin Observatory's software ecosystem. It provides a set of libraries and scripts that facilitate the analysis and visualization of astronomical data.

Architecture
============

The service is structured around a series of commands and tasks, each responsible for a specific aspect of data processing and visualization. Key components include:

- **Worker Script**: A script that initializes and runs the service, handling configuration and database connections.

  - [`rubintv_worker.py`](rubintv_analysis_service/scripts/rubintv_worker.py)

The script is designed to be run on a worker POD that is part of a Kubernetes cluster. It is responsible for initializing the service, loading configuration, and connecting to the Butler and consDB. It listens for incoming commands from the web application, executes them, and returns the results.

There is also a [`mock server`](rubintv_analysis_service/scripts/mock_server.py) that can be used for testing the service before being built on either the USDF or summit.

- **Commands**: Modular operations that perform specific tasks, such as loading columns, detector images, and detector information. These are implemented in various Python modules within the ``commands`` directory, for example the[`db.py`](rubintv_analysis_service/python/lsst/rubintv/analysis/service/commands/db.py) module contains commands for loading information from the consolidated database (consDB), while the [`image.py`](rubintv_analysis_service/python/lsst/rubintv/analysis/service/commands/image.py) module contains commands for loading detector images (not yet implemented), and [`butler.py`](rubintv_analysis_service/python/lsst/rubintv/analysis/service/commands/butler.py) contains commands for loading data from a Butler repository.

All commands derive from the `BaseCommand` class, which provides a common interface for command execution. All inherited classes are required to have parameters as keyword arguments, and implement the `BaseCommand.build_contents` method. This is done to separate the different steps in processing a command:
1. Reading the JSON command and converting it into a python dictionary.
2. Parsing the command and converting it from JSON into a `BaseCommand` instance.
3. Executing the command.
4. Packaging the results of the command into a JSON response and sending it to the rubintv web application.

The `BaseCommand.build_contents` method is called during execution, and must return the result as a `dict` that will be converted into JSON and returned to the user.

Values in a result
------------------

A result is passed through ``to_json_safe`` in ``command.py`` before it is encoded, so a command can return what the database driver, numpy or the stack hands it and need not convert values itself. The rules follow what the Flutter client (``lib/workspace/data.dart``) does with each ConsDB ``datatype``:

- ``int``, ``long``, ``float``, ``double``: sent as JSON numbers; the client reads them with ``toDouble()``. A float that is ``NaN`` or infinite is a problem: Python would write the bare tokens ``NaN`` and ``Infinity``, which are not JSON and make Dart's ``jsonDecode`` reject the whole message. A "load columns" query therefore drops any row holding one in a requested column, exactly as it already excludes rows where a column is ``NULL``, and any such value left elsewhere (an aggregate of an all-``NaN`` column, say) is sent as ``null``. The count that precedes a load can include those rows and so slightly overstate what arrives.
- ``string``, ``text``, ``boolean``: sent as JSON strings and booleans. The client ignores boolean columns.
- ``timestamp``: the driver returns a ``datetime``, which is sent as ``YYYY-MM-DD HH:MM:SS`` with ``.ffffff`` appended when there is a fractional second, e.g. ``2023-05-19 20:20:20.123456``. This is what ``rubin_chart``'s ``dateFromString`` parses: it splits on the space, so the ISO ``T`` separator and any zone suffix would break it. ConsDB stores TAI without a zone; an aware datetime from elsewhere is converted to UTC and its zone dropped. A ``date`` or ``time`` is sent as its ISO string and a ``timedelta`` as seconds.
- numpy scalars and arrays become the equivalent Python values, ``Decimal`` a float, ``UUID`` a string, tuples and sets lists.

Anything else fails in ``json.dumps`` and is reported to the client as a ``command response error`` rather than being stringified, so a new value type shows up as an error instead of a wrongly-typed column. ``tests/test_flutter_contract.py`` pins the timestamp format and the absence of ``NaN``; change the client's parsing and that test together.

Configuration
=============

Configuration for the service is managed through the following YAML files, allowing for flexible deployment and customization of the service's behavior:

- **config.yaml**: Main configuration file specifying service parameters.
- **joins.yaml**: Configuration for database joins.

Configuration options can be overwritten using commad line arguments, which are parsed using the `argparse` module.

Dart/Flutter Frontend
=====================

The frontend of the DDV tool is implemented using the Dart programming language and the Flutter framework. It provides a web-based interface for users to interact with the service, submit commands, and visualize the results, and is located at https://github.com/lsst-ts/rubintv_visualization, which is built on top of [`rubin_chart`](https://github.com/lsst-sitcom/rubin_chart), an open source plotting library in flutter also written by the project.
