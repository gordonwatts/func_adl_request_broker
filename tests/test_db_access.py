# Test access to the mongodb.
# WARNING: Make sure this is running first:
#  docker run --rm -it -v mongodata_test:/data/db -p:27000:27017  mongo:latest

from func_adl_request_broker.db_access import FuncADLDBAccess, ADLRequestInfo
import pytest
import ast
import pymongo


@pytest.fixture
def empty_db():
    db_server = "mongodb://localhost:27000/"
    client = pymongo.MongoClient(db_server)
    db = client.adl_queries
    db.drop_collection('query_collection')
    yield db_server

def test_hash_not_there(empty_db):
    db = FuncADLDBAccess(empty_db)
    r = db.lookup_results("bogus2")
    assert r is None

def test_hash_save (empty_db):
    db = FuncADLDBAccess(empty_db)
    db.save_results('bogus', ADLRequestInfo(done=False, files=['file://root1.root'], jobs=5, phase='downloading', hash=''))
    r = db.lookup_results('bogus')
    assert r is not None
    assert r.done == False
    assert len(r.files) == 1
    assert r.files[0] == 'file://root1.root'
    assert r.jobs == 5
    assert r.phase == 'downloading'

def test_hash_update(empty_db):
    db = FuncADLDBAccess(empty_db)
    db.save_results('bogus1', ADLRequestInfo(done=False, files=['file://root1.root'], jobs=5, phase='running', hash=''))
    db.save_results('bogus1', ADLRequestInfo(done=False, files=['file://root1.root'], jobs=1, phase='done', hash=''))
    r = db.lookup_results('bogus1')
    assert r is not None
    assert r.done == False
    assert len(r.files) == 1
    assert r.files[0] == 'file://root1.root'
    assert r.jobs == 1
    assert r.phase == 'done'

def test_ast_not_there(empty_db):
    db = FuncADLDBAccess(empty_db)
    r = db.lookup_results(ast.Attribute())
    assert r is None

def test_ast_is_there(empty_db):
    a = ast.Add()
    db = FuncADLDBAccess(empty_db)
    db.save_results(a, ADLRequestInfo(done=False, files=['file://root1.root'], jobs=5, phase='downloading', hash=''))
    r = db.lookup_results(a)
    assert r is not None
    assert r.done == False
    assert len(r.files) == 1
    assert r.files[0] == 'file://root1.root'
    assert r.jobs == 5
    assert r.phase == 'downloading'
