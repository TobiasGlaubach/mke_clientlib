
mke_clientlib
===============
MeerKAT Extension (MKE)
(r)emote (i)nterface (m)anagement (lib)rary
interface library for accessing remote experiment and analysis data in a dbserver

Installing
============

.. code-block:: bash

    pip install mke_clientlib

Usage
=====

.. code-block:: bash

    >>> from mke_clientlib.mke_rimlib import Experiment Analysis
    >>> remote_analysis = Analysis(my_dbserver_url, my_id)
