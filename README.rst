
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

.. code-block:: python

    >>> from mke_clientlib.rimlib import Experiment Analysis
    >>> remote_analysis = Analysis(my_dbserver_url, my_id)

See also `examples/example_experiment` for an full example on how to build test scripts using this library

