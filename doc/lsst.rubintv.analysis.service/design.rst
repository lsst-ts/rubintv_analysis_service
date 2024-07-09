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

Configuration
=============

Configuration for the service is managed through the following YAML files, allowing for flexible deployment and customization of the service's behavior:

- **config.yaml**: Main configuration file specifying service parameters.
- **joins.yaml**: Configuration for database joins.

Configuration options can be overwritten using commad line arguments, which are parsed using the `argparse` module.

Dart/Flutter Frontend
=====================

The frontend of the DDV tool is implemented using the Dart programming language and the Flutter framework. It provides a web-based interface for users to interact with the service, submit commands, and visualize the results, and is located at https://github.com/lsst-ts/rubintv_visualization, which is built on top of [`rubin_chart`](https://github.com/lsst-sitcom/rubin_chart), an open source plotting library in flutter also written by the project.
