class dbase(object):
    def addr(self):
        return self._addr_

    def setaddr(self, addr):
        self._addr_ = addr

    @classmethod
    def _from_addr(cls, addr):
        obj = cls.__new__(cls)
        obj.setaddr(addr)
        return obj

    def __eq__(self, other):
        if isinstance(other, dbase):
            return self._addr_ == other._addr_
        return False

    def __ne__(self, other):
        if isinstance(other, dbase):
            return self._addr_ != other._addr_
        return True