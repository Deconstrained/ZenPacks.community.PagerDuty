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
        ''''''
        use = False
        #log.debug("examining device %s" % device)
        filter = CollectorConfigService._filterDevice(self, device)
        dataset = (device.zPDZenossServer, device.zPDZenossUser, device.zPDZenossPass,
                   device.zPDDomain, device.zPDToken, device.zPDUser)
        # device has to be monitored and available
        if device.productionState > -1 and device.getStatus() == 0:
            if dataset not in self.uniqs: 
                self.uniqs.append(dataset)
                log.info("Pagerduty found device %s" % device.id)
                return True
        return False
    
    def _createDeviceProxy(self, device):
        proxy = CollectorConfigService._createDeviceProxy(self, device)
        log.debug("creating Pagerduty proxy for device %s" % device.id)
        proxy.configCycleInterval = 120
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

