import math

from intake.source import base
import pandas as pd
import pysolr
from . import __version__


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
    partition_len: int or None
        The desired partition size. [default: 1024]
    """
    container = 'python'
    name = 'solr'
    version = __version__
    partition_access = True

    def __init__(self, query, base_url, core, qargs=None, metadata=None,
                 auth=None, cert=None, zoocollection=False,
                 partition_len=1024):
        self.query = query
        self.qargs = qargs or {}
        self.metadata = metadata or {}
        self._schema = None
        self.partition_len = partition_len

        if partition_len and partition_len <= 0:
            raise ValueError(f"partition_len must be None or positive, got {partition_len}")
        if auth == 'kerberos':
            from requests_kerberos import HTTPKerberosAuth, OPTIONAL
            auth = HTTPKerberosAuth(mutual_authentication=OPTIONAL,
                                    sanitize_mutual_error_response=False)
        if zoocollection:
            url = ','.join(['/'.join([b, core]) for b in base_url.split(',')])
            zoo = pysolr.ZooKeeper(url)
            if auth or cert:
                self.solr = pysolr.SolrCloud(zoo, zoocollection, auth=auth,
                                             verify=cert)
            else:
                # conda released pysolr does not support auth=
                self.solr = pysolr.SolrCloud(zoo, zoocollection)
        else:
            url = '/'.join([base_url, core])
            if auth or cert:
                self.solr = pysolr.Solr(url, auth=auth, verify=cert)
            else:
                # conda released pysolr does not support auth=
                self.solr = pysolr.Solr(url)

        super(SOLRSequenceSource, self).__init__(metadata=metadata)

    def _get_schema(self):
        """Do a 0 row query and get the number of hits from the response"""
        qargs = self.qargs.copy()
        qargs["rows"] = 0
        start = qargs.get("start", 0)
        results = self.solr.search(self.query, **qargs)

        if self.partition_len is None:
            npartitions = 1
        else:
            npartitions = math.ceil((results.hits - start) / self.partition_len)

        return base.Schema(
            datashape=None,
            dtype=None,
            shape=(results.hits - start,),
            npartitions=npartitions,
            extra_metadata={},
        )

    def _do_query(self, index):
        qargs = self.qargs.copy()
        if self.partition_len is not None:
            qargs["start"] = qargs.get("start", 0) + index * self.partition_len
            qargs["rows"] = self.partition_len
        return self.solr.search(self.query, **qargs)

    def _get_partition(self, index):
        """Downloads all data in query response"""
        solr_rv = self._do_query(index)
        out = []
        for d in solr_rv.docs:
            out.append({k: (v[0] if isinstance(v, (tuple, list)) else v)
                        for k, v in d.items()})
        return out

    def _close(self):
        pass

    def read(self):
        self._load_metadata()
        from itertools import chain
        return chain(*(self._get_partition(index) for index in range(self.npartitions)))

    def to_dask(self):
        from dask import delayed
        import dask.bag

        self._load_metadata()
        return dask.bag.from_delayed(
            [delayed(self.read_partition)(i) for i in range(self.npartitions)]
        )


class SOLRTableSource(SOLRSequenceSource):
    """Execute a query on SOLR, return as dataframe

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
    partition_len: int or None
        The desired partition size. [default: 1024]
    """

    name = 'solrtab'
    container = 'dataframe'
    partition_access = True

    def _get_schema(self, retry=2):
        """Get schema from first 10 hits or cached dataframe"""
        schema = super()._get_schema()

        prev_partition_len = self.partition_len
        try:
            self.partition_len = 10
            df = self._get_partition(0)
            schema["dtype"] = {k: str(v) for k, v in df.dtypes.to_dict().items()}
            schema["shape"] = (schema["shape"][0], *df.shape[1:])
        finally:
            self.partition_len = prev_partition_len

        return schema

    def _get_partition(self, index):
        """Downloads all data in the partition
        """
        seq = super()._get_partition(index)
        # Columns are sorted unless the user defines the field list (fl)
        columns = self.qargs["fl"] if "fl" in self.qargs else sorted(seq[0].keys())
        return pd.DataFrame(seq, columns=columns)

    def read(self):
        self._load_metadata()
        return pd.concat(self._get_partition(index) for index in range(self.npartitions))

    def to_dask(self):
        from dask import delayed
        import dask.dataframe

        self._load_metadata()
        return dask.dataframe.from_delayed(
            [delayed(self.read_partition)(i) for i in range(self.npartitions)]
        )

