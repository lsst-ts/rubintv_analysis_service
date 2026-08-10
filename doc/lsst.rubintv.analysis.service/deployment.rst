.. _rubintv_analysis_service-deployment:

=========================================
Deployment of rubintv_analysis_service
=========================================

.. contents:: Table of Contents
   :depth: 2

Overview
========

This service runs as a **worker pod** in a Kubernetes cluster. It is not a
server: nothing binds a port and nothing connects *to* it. The worker opens an
**outbound websocket** to the RubinTV web app, waits for job messages, executes
them, and sends results back over the same connection.

Because the connection is outbound, a worker that cannot reach the web app has
nothing to do and simply stops. See `Failure modes`_ for why that matters.

Where it runs
=============

The service is deployed at three sites plus a development configuration. The
site is chosen with ``--location`` and its settings come from
``scripts/config.yaml``:

==========  =============================================  ==========
Location    ConsDB host                                    Butler
==========  =============================================  ==========
``summit``  postgresdb01.cp.lsst.org                       embargo
``base``    postgresdb01.ls.lsst.org                       LSSTCam
``usdf``    usdf-summitdb-replica.slac.stanford.edu        embargo
``dev``     usdf-summitdb-replica.slac.stanford.edu        embargo
==========  =============================================  ==========

At USDF the pod lives in the ``rubintv`` namespace, alongside the RubinTV
frontend and a Redis instance:

.. code-block:: text

   rubintv-frontend-...    the RubinTV web app this worker connects to
   rubintv-redis-0         used by the web app, not by this service
   rubintv-workers-...     this service

The deployment is *not* defined in this repository. It is a Helm chart managed
by Argo CD; the pod spec, image, and environment live there. Note that
searching Phalanx for this service may not find it — check the ``rubintv``
namespace's chart directly.

How the pod is assembled
========================

The container image is `rubintv_production
<https://github.com/lsst-sitcom/rubintv_production>`_ (tagged with a weekly
science-pipelines build, e.g. ``w_2025_38``). That image is a **generic worker
shell**, not specific to this service: it provides the LSST stack and a
launcher that clones repositories at startup and runs whatever it is told to.

This service is one payload that shell can run. Two environment variables
select it:

.. code-block:: yaml

   SCRIPTS_LOCATION: /repos/rubintv_analysis_service/scripts
   RUN_ARG: rubintv_worker.py -a rubintv -p 8080 -l usdf

**The code is not baked into the image.** At every container start the launcher
clones this repository fresh:

.. code-block:: yaml

   RA_PULL_DIRECTORIES: rubintv_analysis_service
   DEPLOY_BRANCH: deploy-slac

This has an important consequence: **deploying a change means pushing to the
site's deploy branch**, not rebuilding an image. The pod picks it up on its
next restart. The deploy branches are:

- ``deploy-slac`` — USDF
- ``deploy-summit`` — summit
- ``deploy-bts`` — base test stand

A branch is **not** one pod. USDF runs two instances against different
endpoints, both tracking ``deploy-slac``, so a push reaches both on their next
restart. Anything that differs between instances — notably the web app endpoint
path — therefore has to be set in ``RUN_ARG`` for whichever instance departs
from the default, and a change to a default must be checked against *every*
pod on the branch, not just the one being worked on.

Keeping the deploy branches up to date
--------------------------------------

Ordinary work goes to ``main`` through a PR as usual. A merged PR changes
nothing at any site: the pods track deploy branches, so **a site only gets a
change when its deploy branch does**.

Once a change is reviewed and merged to ``main``, bring the deploy branches
forward with::

   scripts/update_deploy_branches.sh              # dry run: what each would take
   scripts/update_deploy_branches.sh --push       # merge main in and push
   scripts/update_deploy_branches.sh --push deploy-slac   # one site only

Updating one site at a time is often the right call — it lets a change soak at
USDF before it reaches the summit.

Merge, don't rebase
~~~~~~~~~~~~~~~~~~~

The deploy branches are published and are pulled by running pods, so their
history must not be rewritten: a pod that cloned mid-rebase would be sitting on
a commit that no longer exists. The script merges for this reason, and the same
applies to any manual update.

Site-only commits
~~~~~~~~~~~~~~~~~

A deploy branch may carry commits that must never reach ``main`` — a site's
database host, or a ``--path`` default that suits one web app version. Merging
``main`` in preserves them; rebasing or force-pushing would not.

This also means a deploy branch is **not** a staging area for changes headed to
``main``. Anything of general value belongs in a PR against ``main``, or it
will be lost the next time someone reconciles the branches.

Stale branches
~~~~~~~~~~~~~~

``origin`` also carries ``deploy-usdf`` and ``deploy-slac-comcam``, which are
predecessors and are **not** deployed. The script skips them deliberately.
Check a branch is live — that some pod's ``DEPLOY_BRANCH`` names it — before
updating it.

Mounted paths
-------------

The pod supplies configuration and credentials by volume mount, which is why
``config.yaml`` refers to absolute paths that do not exist on a laptop:

=========================================  ==============================
Path                                       Contents
=========================================  ==============================
``/etc/secrets/``                          Postgres and AWS credentials,
                                           assembled by an init container
``/var/ddv-config``                        DDV user configuration
``/sdf/group/rubin``, ``/sdf/data/rubin``  Shared Rubin filesystems
=========================================  ==============================

``$SDM_SCHEMAS_DIR`` must also be set; the worker reads the ConsDB schema YAML
files from ``$SDM_SCHEMAS_DIR/yml`` at startup.

Connecting to the web app
=========================

The worker connects to ``ws://<address>:<port><path>``, set by ``--address``,
``--port`` and ``--path``.

**The endpoint path depends on which version of the web app a site runs**, so
it is a command-line option rather than a hardcoded value:

=================  ==================================  ================================
Web app            Worker endpoint                     Repository
=================  ==================================  ================================
v3 (current)       ``/rubintv/internal/ddv/worker``    `rubintv-v3 <https://github.com/lsst-ts/rubintv-v3>`_
v2 (legacy)        ``/ws/worker``                      lsst-ts/rubintv
=================  ==================================  ================================

The v3 path is built from the app's configured prefix (``SAFIR_PATH_PREFIX``,
``/rubintv`` at USDF) plus ``/internal/ddv/worker``. If a site's prefix differs,
the path changes with it. To find the prefix of a running frontend:

.. code-block:: bash

   kubectl get pod <frontend-pod> -n rubintv \
     -o jsonpath='{range .spec.containers[*].env[*]}{.name}={.value}{"\n"}{end}' \
     | grep -i prefix

``--path`` defaults to the **v3** endpoint. Web app instances upgrade
independently, so a pod still connecting to a v2 web app overrides it in
``RUN_ARG``:

.. code-block:: yaml

   RUN_ARG: rubintv_worker.py -a rubintv -p 8080 -l usdf --path /ws/worker

At USDF as of August 2026 the dev instance runs the v3 web app (and so takes
the default) while prod still runs v2 (and so carries the override). Confirm
before relying on this — check the frontend's image tag, or GET
``<prefix>/api/subapps``, which only the v3 app serves.

The message contract is unchanged between v2 and v3: opaque text frames, one
request and one response per job. Only the path moved.

Failure modes
=============

A worker that cannot connect exits **1** with a ``WorkerConnectionError``. This
is deliberate: under the pod's ``restartPolicy: Always`` a zero exit is
indistinguishable from a clean shutdown, so a misconfigured worker would
restart silently and forever rather than surfacing as a failure.

Diagnosing a worker pod
-----------------------

A worker in ``CrashLoopBackOff`` usually cannot be ``exec``'d into, but its
logs and spec are still available:

.. code-block:: bash

   kubectl logs <worker-pod> -n rubintv --previous
   kubectl get pod <worker-pod> -n rubintv -o yaml

Check ``lastState.terminated.exitCode`` first — it separates the categories
below.

**Handshake status 403 Forbidden.** The endpoint path is wrong. FastAPI
rejects an unmatched websocket route with 403 rather than 404, so this reads as
an authentication failure but usually is not. Compare ``--path`` against the
routes the frontend actually registers.

**Could not find credentials for ...** No line in
``/etc/secrets/postgres-credentials.txt`` matches both the site's ConsDB host
and the ``--database`` name. Check the mounted secret and the host in
``config.yaml``.

**FileNotFoundError on a schema file.** ``$SDM_SCHEMAS_DIR`` is unset or the
SDM schemas checkout is missing a file listed under ``schemas:`` in
``config.yaml``. Note that an unset ``$SDM_SCHEMAS_DIR`` fails here rather than
at startup, because the path is expanded without validation.

**Butler connection errors, worker still running.** Butler failures are caught
and logged rather than fatal, so the worker runs in a degraded state. This will
not cause a restart loop.

Verifying a fix
---------------

The frontend logs a ``ddv.worker.connected`` event when a worker successfully
connects, which is the clearest confirmation that the endpoint and path are
right:

.. code-block:: bash

   kubectl logs <frontend-pod> -n rubintv | grep ddv.worker

Running locally
===============

``scripts/mock_server.py`` stands in for the web app so the worker can be run
without a deployment:

.. code-block:: bash

   python scripts/mock_server.py                                     # terminal 1
   python scripts/rubintv_worker.py -l dev --path /ws/worker         # terminal 2

``--path`` is needed because the mock server routes ``/ws/<type>`` only, while
the worker now defaults to the v3 web app's endpoint. Without it the handshake
is rejected with a 403.

The ``dev`` location expects credentials at ``~/.lsst/postgres-credentials.txt``
rather than the mounted secret path — the other locations read a secret mounted
into the pod at ``/etc/secrets/``, which does not exist outside the cluster.
The file is in ``.pgpass`` format and its first field has to match the ``dev``
host in ``config.yaml`` exactly::

   <consdb-host>:5432:exposurelog:<user>:<password>

``$SDM_SCHEMAS_DIR`` must also point at an SDM schemas checkout.

If the worker exits with ``Could not find credentials for ...`` it never opened
a connection — the message means no line in that file matched both the host and
the ``--database`` name (default ``exposurelog``). Check the file exists at the
path the chosen location implies before looking at its contents.
