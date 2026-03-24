Protocol Support
================

Overview
--------

``aiomegfile`` exposes a unified async API over several path protocols. In most
cases you can switch between local and remote paths without changing the higher
level calling code.

Supported Backends
------------------

.. list-table::
   :header-rows: 1

   * - Protocol
     - Path format
     - Availability
     - Notes
   * - Local filesystem
     - ``/tmp/file`` or ``file:///tmp/file``
     - Built in
     - Full read, write, scan, stat, and path operations.
   * - Amazon S3
     - ``s3://bucket/key``
     - Built in
     - Async object access backed by ``aiobotocore``.
   * - HTTP / HTTPS
     - ``http://...`` / ``https://...``
     - Built in
     - Read-oriented access for streaming and metadata inspection.
   * - SFTP
     - ``sftp://host/path``
     - Built in
     - Remote filesystem access backed by ``asyncssh``.
   * - stdio
     - ``stdio://-`` / ``stdio://0`` / ``stdio://1`` / ``stdio://2``
     - Built in
     - Bridge stdin, stdout, and stderr through the same async API.
   * - HDFS
     - ``hdfs://cluster/path``
     - Optional
     - Install ``aiomegfile[hdfs]`` to enable HDFS support.
   * - WebDAV / WebDAVS
     - ``webdav://host/path`` / ``webdavs://host/path``
     - Optional
     - Install ``aiomegfile[webdav]`` to enable WebDAV support.

Path Semantics
--------------

- Plain paths without a protocol are treated as local filesystem paths.
- ``SmartPath`` preserves the protocol, so ``SmartPath("s3://bucket/key")`` and
  ``SmartPath("/tmp/file")`` work through the same abstraction.
- Utility functions such as ``smart_open()``, ``smart_copy()``, and
  ``smart_sync()`` dispatch to the correct backend automatically.

Alias Support
-------------

Aliases are loaded from ``~/.config/megfile/megfile.conf``. They let you define
short protocol prefixes for repeated locations.

Example:

.. code-block:: ini

   [alias]
   datasets = s3://company-datasets/
   web = https://static.example.com/

After that, these URIs become valid inputs:

.. code-block:: text

   datasets://images/train-0001.jpg
   web://releases/latest.json
