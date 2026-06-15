Installation
============

Base Installation
-----------------

Install the core package when you only need the built-in protocol backends:

.. code-block:: bash

   pip install aiomegfile

Optional Extras
---------------

Use extras to enable optional integrations and tooling:

.. list-table::
   :header-rows: 1

   * - Extra
     - Install command
     - Purpose
   * - WebDAV
     - ``pip install "aiomegfile[webdav]"``
     - Enables ``webdav://`` and ``webdavs://`` support.

Development Setup
-----------------

This repository uses PDM for local development. A typical setup is:

.. code-block:: bash

   pdm install -G dev

Documentation can be built locally with:

.. code-block:: bash

   make doc
