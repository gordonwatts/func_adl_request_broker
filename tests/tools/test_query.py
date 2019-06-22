# Test the query app

from tools.query_web import query, BadASTException
import pytest
from unittest.mock import Mock
import pickle
import io
import ast
import os

from adl_func_client.event_dataset import EventDataset

@pytest.fixture
def good_query_ast_pickle_data():
    'A good query ast to be used for testing below'
    f_ds = EventDataset(r'localds://mc16_13TeV.311309.MadGraphPythia8EvtGen_A14NNPDF31LO_HSS_LLP_mH125_mS5_ltlow.deriv.DAOD_EXOT15.e7270_e5984_s3234_r9364_r9315_p3795')
    a = f_ds \
        .SelectMany('lambda e: e.Jets("AntiKt4EMTopoJets")') \
        .Select('lambda j: j.pt()/1000.0') \
        .AsROOTTTree('output.root', 'dudetree', 'JetPt') \
        .value(executor=lambda a: a)
    return pickle.dumps(a)

@pytest.fixture
def bad_query_ast_pickle_data_pandas():
    'A good query ast to be used for testing below'
    f_ds = EventDataset(r'localds://mc16_13TeV.311309.MadGraphPythia8EvtGen_A14NNPDF31LO_HSS_LLP_mH125_mS5_ltlow.deriv.DAOD_EXOT15.e7270_e5984_s3234_r9364_r9315_p3795')
    a = f_ds \
        .SelectMany('lambda e: e.Jets("AntiKt4EMTopoJets")') \
        .Select('lambda j: j.pt()/1000.0') \
        .AsPandasDF('JetPt') \
        .value(executor=lambda a: a)
    return pickle.dumps(a)

@pytest.fixture
def bad_query_random():
    return pickle.dumps({'hi': 'dude', 'there': 'fork', 'omg': 'shirtballs'})

class Holder:
    def __init__ (self, b):
        self.stream = io.BytesIO(b)
        self.stream_len = len(b)

@pytest.fixture
def good_query_ast_body(good_query_ast_pickle_data):
    return Holder(good_query_ast_pickle_data)

@pytest.fixture
def query_pandas_body(bad_query_ast_pickle_data_pandas):
    return Holder(bad_query_ast_pickle_data_pandas)

@pytest.fixture
def query_random_body(bad_query_random):
    return Holder(bad_query_random)

@pytest.fixture
def mock_good_rabbit_call(monkeypatch):
    do_rpc_call_mock = Mock()
    do_rpc_call_mock.return_value = {'files': [['file.root', 'dudetree3']], 'phase': 'done', 'done': True, 'jobs': 1}
    monkeypatch.setattr('tools.query_web.do_rpc_call', do_rpc_call_mock)

@pytest.fixture
def no_prefix_env():
    if 'FILE_URL' in os.environ:
        del os.environ['FILE_URL']

@pytest.fixture
def with_prefix_env():
    os.environ['FILE_URL'] = 'file://'
    yield 'ok'
    del os.environ['FILE_URL']

def test_good_call_no_prefix(good_query_ast_body, mock_good_rabbit_call, no_prefix_env):
    a = query(good_query_ast_body)
    assert a is not None
    assert 'files' in a
    fd = a['files']
    assert len(fd) == 1
    assert len(fd[0]) == 2
    fspec, tname = fd[0]
    assert fspec == 'file.root'
    assert tname == 'dudetree3'

def test_good_call_with_prefix(good_query_ast_body, mock_good_rabbit_call, with_prefix_env):
    a = query(good_query_ast_body)
    assert a is not None
    assert 'files' in a
    fd = a['files']
    assert len(fd) == 1
    assert len(fd[0]) == 2
    fspec, tname = fd[0]
    assert fspec == 'file://file.root'
    assert tname == 'dudetree3'

def test_pandas_ast(query_pandas_body):
    try:
        _ = query(query_pandas_body)
        assert False
    except BadASTException:
        return
    assert False

def test_random_ast(query_random_body):
    try:
        _ = query(query_random_body)
        assert False
    except BadASTException:
        return
    assert False