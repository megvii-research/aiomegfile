CLI
===

aiomegfile ships with a CLI named ``amf``.

Installation
------------

.. code-block:: bash

   pip install "aiomegfile[cli]"

Quick Examples
--------------

.. code-block:: bash

   amf ls ./data
   amf ls s3://my-bucket/prefix -l
   amf cp -r ./data s3://my-bucket/backup
   amf sync ./data s3://my-bucket/backup --progress-bar
   amf cat https://example.com/data.txt
   printf 'payload' | amf to s3://my-bucket/stdin-demo.txt

Command Groups
--------------

The CLI covers several common workflows:

- Inspection: ``ls``, ``ll``, ``stat``, ``size``, ``mtime``, ``md5sum``
- Transfer: ``cp``, ``mv``, ``sync``, ``rm``, ``mkdir``, ``touch``
- Streaming: ``cat``, ``head``, ``tail``, ``to``, ``edit``
- Configuration: ``config``
- Shell integration: ``completion``

Protocol Support
----------------

The CLI works with the same backend registry as the Python API:

- local paths and ``file://``
- ``s3://``
- ``http://`` and ``https://``
- ``sftp://``
- ``stdio://``
- ``hdfs://`` when the HDFS extra is installed
- ``webdav://`` and ``webdavs://`` when the WebDAV extra is installed

Configuration
-------------

Helper commands are available for common configuration files:

.. code-block:: bash

   amf config s3 <access_key> <secret_key> --profile-name default
   amf config hdfs http://namenode:9870 --profile-name prod --user hdfs
   amf config alias datasets s3://company-datasets/
   amf config env AIOMEGFILE_MAX_WORKERS=16

Completion
----------

The CLI can append completion setup to your shell config:

.. code-block:: bash

   amf completion bash
   amf completion zsh
   amf completion fish

Notes
-----

- ``sync`` modifies the destination tree to match the source.
- ``cp`` supports recursive copies with ``-r`` and progress bars with ``-g``.
- ``to`` reads from stdin and writes to the target path.
- ``cat``, ``head``, and ``tail`` are useful for quick remote inspection.

Reference
---------

.. click:: aiomegfile.cli:cli
   :prog: amf
   :nested: full
