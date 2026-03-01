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

Supported protocols match the current aiomegfile backend set (``file://`` and
``s3://``).

Configuration
-------------

For S3 credentials, use the helper command (writes to ``~/.aws/credentials``):

.. code-block:: bash

   amf config s3 <access_key> <secret_key> --profile-name default

If you need a broader protocol matrix or advanced configuration options,
refer to the megfile documentation at
http://megvii-research.github.io/megfile.

Reference
---------

.. click:: aiomegfile.cli:cli
   :prog: amf
   :nested: full
