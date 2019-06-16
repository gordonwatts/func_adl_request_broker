# Test out db access
import ast
from collections import namedtuple
from typing import Optional, Union
import pymongo
from adl_func_backend.ast.ast_hash import calc_ast_hash

ADLRequestInfo = namedtuple('ADLRequestInfo', 'done files jobs phase hash')

def hash_from_arg (arg:Union[ast.AST, str]):
    'Return the hash from the arg. If it is a string, then it is the hash. Otherwise it is an ast to be hashed.'
    if isinstance(arg, str):
        return arg
    return calc_ast_hash(arg)    

def obj_from_dict (r:dict) -> ADLRequestInfo:
    return ADLRequestInfo(done=r['done'], files=r['files'], jobs=r['jobs'], phase=r['phase'], hash=r['hash'])

class FuncADLDBAccess:
    def __init__ (self, db_server):
        self._client = pymongo.MongoClient(db_server)
        self._db = self._client.adl_queries
        self._query_collection = self._db.query_collection

    def lookup_results(self, arg:Union[str,ast.AST]) -> Optional[ADLRequestInfo]:
        r'''
        Look up the ast hash in the database to see if this query has been requested already. Return its info if it has, otherwise
        return none.

        Arguments:
            arg                   The AST that we are going to do the lookup for

        Returns:
            None                Nothing was found in the DB
            (done, [file])      A list of associated files. Done is true if the query has been processed, otherwise it is marked
                                as in progress.
        '''
        r = self._query_collection.find_one({'hash': hash_from_arg(arg)})
        return None if r is None else obj_from_dict(r)

    def save_results(self, arg:str, results:ADLRequestInfo) -> ADLRequestInfo:
        '''
        Save the data into the db with the appropriate hash.

        Arguments:
            arg             The hash of the AST that we use as a lookup key (or the AST itself)
            results         The results that are to be stored in the db.
        '''
        hash = hash_from_arg(arg)
        d = {
            'done': results.done,
            'files': results.files,
            'jobs': results.jobs,
            'phase': results.phase,
            'hash': hash
        }

        self._query_collection.update({'hash': hash}, d, upsert=True)
        return obj_from_dict(d)