import os

import pysolr
import pytest
import pandas as pd
import time

from intake_solr import SOLRSequenceSource, SOLRTableSource
from .util import start_solr, stop_docker, TEST_CORE

CONNECT = {'host': 'localhost', 'port': 9200}
TEST_DATA_DIR = 'tests'
TEST_DATA = 'sample1.csv'
df = pd.read_csv(os.path.join(TEST_DATA_DIR, TEST_DATA))


@pytest.fixture(scope='module')
def engine():
    """Start docker container for ES and cleanup connection afterward."""
    stop_docker('intake-solr', let_fail=True)
    cid = None
    try:
        cid = start_solr()

        url = 'http://localhost:8983/solr'
        solr = pysolr.Solr(url + '/' + TEST_CORE)
        data = df.to_dict(orient='records')
        timeout = 20
        while True:
            try:
                solr.add(data)
                break
            except pysolr.SolrError:
                if timeout < 0:
                    raise RuntimeError('Timeout while waiting for SOLR')
                time.sleep(0.5)
                timeout -= 0.5
        solr.commit()
        yield url
    finally:
        stop_docker(cid=cid)


def test_sequence_read(engine):
    source = SOLRSequenceSource('score:[0 TO 150]', engine, TEST_CORE)
    out = source.read()
    out2 = pd.DataFrame(out)[df.columns]
    assert out2.equals(df)


def test_table_read(engine):
    source = SOLRTableSource('score:[0 TO 150]', engine, TEST_CORE)
    out = source.read()
    out2 = out[df.columns]
    assert out2.equals(df)
