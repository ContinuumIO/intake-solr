from intake.source import base
import pandas as pd
from requests_kerberos import HTTPKerberosAuth, OPTIONAL
import pysolr
__version__ = '0.0.1'


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
        base_kwargs, source_kwargs = self.separate_base_kwargs(kwargs)
        qargs = source_kwargs.pop('qargs', {})
        return SOLRTableSource(query, base_url, core, qargs=qargs,
                               metadata=base_kwargs['metadata'],
                               **source_kwargs)


class SOLRSequenceSource(base.DataSource):
    """Execute a query on SOLR

    Parameters
    ----------
    query: str
        Query to execute, in Lucene syntax, e.g., ``"*:*"``
    base_url: str
        Connection on which to reach SOLR, including protocol (http), server,
        port and base path. If using Zookeeper, this should be the full
        comma-separated list of service:port/path elements.
    core: str
        Named segment of the SOLR storage to query
    qargs: dict
        Further parameters to pass with the query (e.g., highlighting)
    metadata: dict
        Additional information to associate with this source
    auth: None, "kerberos" or (username, password)
        Authentication to attach to requests
    cert: str or None
        Path to SSL certificate, if required
    zoocollection: bool or str
        If using Zookeeper to orchestrate SOLR, this is the name of the
        collection to connect to.
    """
    container = 'python'

    def __init__(self, query, base_url, core, qargs=None, metadata=None,
                 auth=None, cert=None, zoocollection=False):
        self.query = query
        self.qargs = qargs or {}
        self.metadata = metadata or {}
        self._schema = None
        if auth == 'kerberos':
            auth = HTTPKerberosAuth(mutual_authentication=OPTIONAL,
                                    sanitize_mutual_error_response=False)
        if zoocollection:
            url = ','.join(['/'.join([b, core]) for b in base_url.split(',')])
            zoo = pysolr.ZooKeeper(url)
            self.solr = pysolr.SolrCloud(zoo, zoocollection, auth=auth,
                                         verify=cert)
        else:
            url = '/'.join([base_url, core])
            if auth or cert:
                self.solr = pysolr.Solr(url, auth=auth, verify=cert)
            else:
                # conda released pysolr does not support auth=
                self.solr = pysolr.Solr(url)

        super(SOLRSequenceSource, self).__init__(container=self.container,
                                                 metadata=metadata)
    def _get_schema(self):
        return base.Schema(datashape=None,
                           dtype=None,
                           shape=None,
                           npartitions=1,
                           extra_metadata={})

    def _do_query(self):
        out = []
        data = self.solr.search(self.query, **self.qargs).docs
        for d in data:
            out.append({k: (v[0] if isinstance(v, (tuple, list)) else v)
                        for k, v in d.items()})
        return out

    def _get_partition(self, _):
        """Downloads all data
        """
        return self._do_query()


class SOLRTableSource(SOLRSequenceSource):

    container = 'dataframe'
    _dataframe = None

    def _get_schema(self, retry=2):
        """Get schema from first 10 hits or cached dataframe"""
        if self._dataframe is None:
            self._get_partition(0)
        return base.Schema(datashape=None,
                           dtype=self._dataframe[:0],
                           shape=self._dataframe.shape,
                           npartitions=1,
                           extra_metadata={})

    def _get_partition(self, _):
        """Downloads all data
        """
        if self._dataframe is None:
            df = pd.DataFrame(self._do_query())
            self._dataframe = df
            self._schema = None
            self.discover()
        return self._dataframe

    def _close(self):
        self._dataframe = None
