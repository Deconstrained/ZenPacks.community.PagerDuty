"""
PagerDutyService
ZenHub service for providing configuration to the zenpdsync collector daemon.
    This provides the daemon with unique configurations against which to run the sync script
"""
import logging
log = logging.getLogger('zen.zenpdsync')

import Globals
from Products.ZenCollector.services.config import CollectorConfigService

class PagerDutyService(CollectorConfigService):

    uniqs = []
    
    def __init__(self, dmd, instance):
        self.uniqs = []
        deviceProxyAttributes = (
                                 'zPDZenossServer',
                                 'zPDZenossUser',
                                 'zPDZenossPass',
                                 'zPDDomain',
                                 'zPDToken',
                                 'zPDUser',
                                 )
        CollectorConfigService.__init__(self, dmd, instance, deviceProxyAttributes)
        
    
    def _filterDevice(self, device):
        use = False
        #log.debug("examining device %s" % device)
        filter = CollectorConfigService._filterDevice(self, device)
        
        dataset = {
                   'zenserver' : device.zPDZenossServer,
                   'zenuser' : device.zPDZenossUser,
                   'zenpass': device.zPDZenossPass,
                   'pddomain' : device.zPDDomain,
                   'pdtoken' : device.zPDToken,
                   'pduser': device.zPDUser,
                   }
        if dataset not in self.uniqs and device.productionState == 1000:
            self.uniqs.append(dataset)
            log.debug("found device %s" % device)
            return filter

    def _createDeviceProxy(self, device):
        proxy = CollectorConfigService._createDeviceProxy(self, device)
        log.debug("creating proxy for device %s" % device.id)
        proxy.configCycleInterval = 30
        proxy.device = device.id
        proxy.zenhost = device.zPDZenossServer
        proxy.zenuser = device.zPDZenossUser
        proxy.zenpass = device.zPDZenossPass
        proxy.pdhost = device.zPDDomain 
        proxy.pdtoken = device.zPDToken
        proxy.pduser = device.zPDUser 
        return proxy

if __name__ == '__main__':
    from Products.ZenHub.ServiceTester import ServiceTester
    tester = ServiceTester(PagerDutyService)
    def printer(config):
        print config.datapoints
    tester.printDeviceProxy = printer
    tester.showDeviceInfo()
    
