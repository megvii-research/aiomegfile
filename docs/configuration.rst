Configuration
=============

Config Files
------------

``aiomegfile`` reads configuration from a few standard locations:

- ``~/.config/megfile/megfile.conf`` for generic runtime environment values and
  alias mappings
- ``~/.aws/credentials`` for S3 profiles written by ``amf config s3``
- ``~/.hdfscli.cfg`` for HDFS profiles written by ``amf config hdfs``

Main Runtime File
-----------------

The main config file supports the ``[env]`` and ``[alias]`` sections.

Example:

.. code-block:: ini

   [env]
   MEGFILE_MAX_WORKERS = 16
   MEGFILE_READER_BLOCK_SIZE = 16MB
   MEGFILE_WRITER_BLOCK_SIZE = 16MB
   MEGFILE_WEBDAV_MAX_RETRY_TIMES = 6
   WEBDAV_USERNAME = alice

   [alias]
   datasets = s3://company-datasets/
   reports = file:///srv/reports/

Common Environment Variables
----------------------------

.. list-table::
   :header-rows: 1

   * - Variable
     - Meaning
   * - ``MEGFILE_MAX_RETRY_TIMES``
     - Global retry limit used when a protocol-specific retry limit is not set.
   * - ``MEGFILE_MAX_WORKERS``
     - Global concurrency limit for background async work.
   * - ``MEGFILE_READER_BLOCK_SIZE``
     - Chunk size for buffered readers.
   * - ``MEGFILE_READER_MAX_BUFFER_SIZE``
     - Maximum prefetch buffer size for readers.
   * - ``MEGFILE_READER_LAZY_PREFETCH``
     - Enables lazy prefetch behavior when supported.
   * - ``MEGFILE_WRITER_BLOCK_SIZE``
     - Chunk size for buffered writers, especially important for S3 multipart uploads.
   * - ``MEGFILE_WRITER_MAX_BUFFER_SIZE``
     - Maximum in-memory write buffer size.
   * - ``MEGFILE_WRITER_BLOCK_AUTOSCALE``
     - Enables writer block auto-scaling when no explicit writer block size is set.
   * - ``MEGFILE_S3_MAX_RETRY_TIMES``
     - Retry limit for S3 operations.
   * - ``MEGFILE_HDFS_MAX_RETRY_TIMES``
     - Retry limit for HDFS operations.
   * - ``MEGFILE_HTTP_MAX_RETRY_TIMES``
     - Retry limit for HTTP and HTTPS operations.
   * - ``MEGFILE_SFTP_MAX_RETRY_TIMES``
     - Retry limit for SFTP operations.
   * - ``MEGFILE_WEBDAV_MAX_RETRY_TIMES``
     - Retry limit for WebDAV and WebDAVS operations.

Protocol-specific Settings
--------------------------

Some backends read their own environment variables in addition to the generic
settings above.

SFTP
^^^^

- ``SFTP_PORT``
- ``SFTP_USERNAME``
- ``SFTP_PASSWORD``
- ``SFTP_PRIVATE_KEY_PATH``
- ``SFTP_PRIVATE_KEY_PASSPHRASE``
- ``SFTP_CONNECT_TIMEOUT``
- ``SFTP_KEEPALIVE_INTERVAL``
- ``SFTP_MAX_UNAUTH_CONNECTIONS``
- ``MEGFILE_SFTP_HOST_KEY_POLICY``

WebDAV
^^^^^^

- ``WEBDAV_USERNAME``
- ``WEBDAV_PASSWORD``
- ``WEBDAV_TOKEN``
- ``WEBDAV_TOKEN_COMMAND``
- ``WEBDAV_TIMEOUT``
- ``WEBDAV_INSECURE``

HDFS
^^^^

- ``HDFS_USER``
- ``HDFS_URL``
- ``HDFS_ROOT``
- ``HDFS_TIMEOUT``
- ``HDFS_TOKEN``
- ``HDFS_CONFIG_PATH``

Using CLI Helpers
-----------------

The CLI can write the common config files for you:

.. code-block:: bash

   amf config s3 <access_key> <secret_key> --profile-name default
   amf config hdfs http://namenode:9870 --profile-name prod --user hdfs
   amf config alias datasets s3://company-datasets/
   amf config env MEGFILE_MAX_WORKERS=16

Alias Resolution
----------------

If you define:

.. code-block:: ini

   [alias]
   datasets = s3://company-datasets/

Then ``datasets://images/cat.jpg`` resolves to
``s3://company-datasets/images/cat.jpg``.
