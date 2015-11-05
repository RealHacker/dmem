from dbase import dbase
import random, string
import contextlib
from utils import *

class RedisStr(dbase):
    def __init__(self, s=None):
        dbase.__init__(self)
        self._type_ = "dmem:str"
        # save when initializing
        if s:
            ret = self.client.set(self._addr_, s)
            if not ret:
                raise RedisOperationFailure()
        
    def getvalue(self):
        # refresh the value
        s = self.client.get(self._addr_)
        return s

    def setvalue(self, s):
        ret = self.client.set(self._addr_, s)
        if not ret:
            raise RedisOperationFailure()

    def __iadd__(self, more):
        if isinstance(more, RedisStr):
            more = more.getvalue()
        if not isinstance(more, basestring):
            raise TypeError("The argument is not a string")
        ret = self.client.append(self._addr_, more)
        if not ret:
            raise RedisOperationFailure()
        return self

    def __add__(self, more):
        # return a new RedisStr
        s = self.getvalue()
        if isinstance(more, RedisStr):
            more = more.getvalue()
        news = RedisStr(s+more)
        return news

    def __str__(self):
        return s.getvalue()

    def __contains__(self, sub):
        return sub in self.getvalue()

    def __getslice__(self, i, j):
        ret = self.client.getrange(self._addr_, i, j)
        if not ret:
            raise RedisOperationFailure()
        return ret

    def __setslice__(self, i, j, val):
        ret = self.client.setrange(self._addr_, i, j, val)
        if not ret:
            raise RedisOperationFailure()
    
    def __len__(self):
        ret = self.client.strlen(self._addr_)
        return ret

    def __getitem__(self, idx):
        s = self.getvalue()
        return s[idx]

    def __getattr__(self, attr):
        # delegate everything else to string value
        s = self.getvalue()
        if hasattr(s, attr):
            return getattr(s, attr)
        








