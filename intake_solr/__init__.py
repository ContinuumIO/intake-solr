from intake.source import base
from requests_kerberos import HTTPKerberosAuth, OPTIONAL
import pysolr
__version__ = '0.0.1'


class Plugin(base.Plugin):
    """Plugin for SOLR reader"""

    def __init__(self):
        super(Plugin, self).__init__(name='solr',
                                     version=__version__,
                                     container='python',
                                     partition_access=False)

    def open(self, query, base_url, core, **kwargs):
        """
        Create SOLRSource instance

        Parameters
        ----------
        query, base_url, core, qargs, kwargs:
            See ``SOLRSource``.
        """
        base_kwargs, source_kwargs = self.separate_base_kwargs(kwargs)
        qargs = source_kwargs.pop('qargs', {})
        return SOLRSource(query, base_url, core, qargs=qargs,
                          metadata=base_kwargs['metadata'], **source_kwargs)


class SOLRSource(base.DataSource):
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
    def __init__(self, query, base_url, core, qargs=None, metadata=None,
                 auth=None, cert=None, zoocollection=False):
        self.query = query
        self.qargs = qargs or {}
        self.metadata = metadata or {}
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
            self.solr = pysolr.Solr(url, auth=auth, verify=cert)

    def read(self):
        return self.solr.search(self.query, **self.qargs).docs
