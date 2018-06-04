Quickstart
==========

``intake-solr`` provides quick and easy access to tabular data stored in
Apache `SOLR`_

.. _SOLR: http://lucene.apache.org/solr/

This plugin reads SOLR query results without random access: there is only ever
a single partition.

Installation
------------

To use this plugin for `intake`_, install with the following command::

   conda install -c intake intake-solr

.. _intake: https://github.com/ContinuumIO/intake

Usage
-----

Ad-hoc
~~~~~~

After installation, the functions ``intake.open_solr_table`` and
``intake.open_solr_sequence`` will become available. The former method can be
used to return the results of a SOLR query into a dataframe, but the latter will
produce a generic sequence of dictionaries.

Given the query ``text:test``, the following would load into a dataframe::

  import intake
  source = intake.open_solr_dataframe("text:test")
  dataframe = source.read()

Three parameters are of interest when defining a data source:

- query: the query to execute, which can be defined either using `Lucene`_ or
  `JSON`_ syntax, both of which are to be provided as a string.

.. _Lucene: https://www.elastic.co/guide/en/kibana/current/lucene-query.html

Creating Catalog Entries
~~~~~~~~~~~~~~~~~~~~~~~~

To include in a catalog, the plugin must be listed in the plugins of the catalog::

   plugins:
     source:
       - module: intake_solr

and entries must specify ``driver: solr_table`` or ``driver: solr_sequence``.
The further arguments are exactly the same as for the ``open_solr_*`` functions.

Using a Catalog
~~~~~~~~~~~~~~~

Assuming a catalog file called ``cat.yaml``, containing a SOLR source ``data``,
one could load it into a dataframe as follows::

  import intake
  cat = intake.Catalog('cat.yaml')
  df = cat.data.read()

The type of the output will depend on the plugin that was defined in the
catalog. You can inspect this before loading by looking at the ``.container``
attribute, which will be either ``"dataframe"`` or ``"python"``.
