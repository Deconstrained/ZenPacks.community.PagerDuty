import Globals

from Products.ZenModel.ZenPack import ZenPack as ZenPackBase
from Products.ZenUtils.Utils import unused

unused(Globals)


class ZenPack(ZenPackBase):

     # All zProperties defined here will automatically be created when the
     # ZenPack is installed.
     packZProperties = [
        ('zPDZenossServer', 'localhost', 'string'),
        ('zPDZenossUser', 'admin', 'string'),
        ('zPDZenossPass', 'notsecure', 'password'),
        ('zPDDomain', '', 'string'),
        ('zPDToken', '', 'string'),
        ('zPDUser', '', 'string'),
        ('zPDServiceKey', '', 'string'),
        ]

