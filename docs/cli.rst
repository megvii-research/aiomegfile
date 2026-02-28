CLI
===

aiomegfile ships with a CLI that mirrors the core workflow of megfile while
running all operations asynchronously under the hood.

Installation
------------

.. code-block:: bash

   pip install "aiomegfile[cli]"

Quick Examples
--------------

.. code-block:: bash

   aiomegfile ls ./data
   aiomegfile ls s3://my-bucket/prefix -l
   aiomegfile cp -r ./data s3://my-bucket/backup
   aiomegfile sync ./data s3://my-bucket/backup --progress-bar

Supported protocols match the current aiomegfile backend set (``file://`` and
``s3://``).

Configuration
-------------

For S3 credentials, use the helper command (writes to ``~/.aws/credentials``):

.. code-block:: bash

   aiomegfile config s3 <access_key> <secret_key> --profile-name default

If you need a broader protocol matrix or advanced configuration options,
refer to the megfile documentation at
http://megvii-research.github.io/megfile.

Reference
---------

.. click:: aiomegfile.cli:cli
   :prog: aiomegfile
   :nested: full
