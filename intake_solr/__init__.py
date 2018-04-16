from intake.source import base
from ._version import get_versions
__version__ = get_versions()['version']
del get_versions


class SequencePlugin(base.Plugin):
    """Plugin for SOLR reader"""

    def __init__(self):
        super(SequencePlugin, self).__init__(name='solr-sequence',
                                             version=__version__,
                                             container='python',
                                             partition_access=False)

    def open(self, query, base_url, core, **kwargs):
        """
        Create SOLRSequenceSource instance

        Parameters
        ----------
        query, base_url, core, qargs, kwargs:
            See ``SOLRSource``.
        """
        from intake_solr.source import SOLRSequenceSource
        base_kwargs, source_kwargs = self.separate_base_kwargs(kwargs)
        qargs = source_kwargs.pop('qargs', {})
        return SOLRSequenceSource(query, base_url, core, qargs=qargs,
                                  metadata=base_kwargs['metadata'],
                                  **source_kwargs)


class TablePlugin(base.Plugin):
    """Plugin for SOLR reader"""

    def __init__(self):
        super(TablePlugin, self).__init__(
            name='solr-table', version=__version__, container='dataframe',
            partition_access=False)

    def open(self, query, base_url, core, **kwargs):
        """
        Create SOLRTableSource instance

        Parameters
        ----------
        query, base_url, core, qargs, kwargs:
            See ``SOLRSource``.
        """
        from intake_solr.source import SOLRTableSource
        base_kwargs, source_kwargs = self.separate_base_kwargs(kwargs)
        qargs = source_kwargs.pop('qargs', {})
        return SOLRTableSource(query, base_url, core, qargs=qargs,
                               metadata=base_kwargs['metadata'],
                               **source_kwargs)
