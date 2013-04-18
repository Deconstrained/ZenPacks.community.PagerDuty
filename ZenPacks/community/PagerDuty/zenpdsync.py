import logging
log = logging.getLogger('zen.zenpdsync')
import Globals
import zope.component
import zope.interface
from twisted.internet import defer
from Products.ZenCollector.daemon import CollectorDaemon
from Products.ZenCollector.interfaces import ICollectorPreferences, IScheduledTask, IEventService, IDataService
from Products.ZenCollector.tasks import SimpleTaskFactory, SimpleTaskSplitter, TaskStates
from Products.ZenUtils.observable import ObservableMixin
from Products.ZenEvents.Event import Warning, Clear
from Products.ZenUtils.Utils import unused
from twisted.internet import base, defer, reactor
from Products.ZenUtils.Driver import drive
from twisted.python.failure import Failure
import time

from ZenPacks.community.PagerDuty.services.PagerDutyService import PagerDutyService
from ZenPacks.community.PagerDuty.libexec.zenpagerduty import *

unused(Globals)
unused(PagerDutyService)

class ZenPagerDutyPreferences(object):
    zope.interface.implements(ICollectorPreferences)

    def __init__(self):
        self.collectorName = 'zenpdsync'
        self.configurationService = "ZenPacks.community.PagerDuty.services.PagerDutyService"
        # How often the daemon will collect each device. Specified in seconds.
        self.cycleInterval = 60
        self.configCycleInterval = 300  
        self.options = None

    def buildOptions(self, parser):
        """
        Required to implement the ICollectorPreferences interface.
        """
        parser.add_option('--eventsBuffer', dest='eventsBuffer',
                        default=20, type='int',
                        help='Number of events to pull from remote sources'
                        )

    def postStartup(self):
        """
        Required to implement the ICollectorPreferences interface.
        """
        pass
    
    def getEventsBuffer(self):
        return self.options.eventsBuffer
    
class ZenPagerDutyTask(ObservableMixin):
    zope.interface.implements(IScheduledTask)
    
    CLEAR_EVENT = dict(component="zenpdsync", severity=Clear, eventClass='/Cmd/Fail')
    WARNING_EVENT = dict(component="zenpdsync", severity=Warning, eventClass='/Cmd/Fail')
    
    def __init__(self, taskName, deviceId, interval, taskConfig):
        log.debug("__init__ %s" % deviceId)
        super(ZenPagerDutyTask, self).__init__()
        self._taskConfig = taskConfig
        self._eventService = zope.component.queryUtility(IEventService)
        self._dataService = zope.component.queryUtility(IDataService)
        self._preferences = zope.component.queryUtility(ICollectorPreferences, 'zenpdsync')
        self.eventsBuffer = self._preferences.getEventsBuffer()
        
        self.name = taskName
        self.configId = deviceId
        self.interval = interval
        self.state = TaskStates.STATE_IDLE
        self.zenhost = self._taskConfig.zenhost
        self.zenuser = self._taskConfig.zenuser
        self.zenpass = self._taskConfig.zenpass
        self.pdhost = self._taskConfig.pdhost
        self.pdtoken = self._taskConfig.pdtoken
        self.pduser = self._taskConfig.pduser
        self.sync = None
        log.debug("__init__  using %s %s %s %s %s %s" % (self._taskConfig.zenhost,self._taskConfig.zenuser,self._taskConfig.zenpass,
                                                         self._taskConfig.pdhost,self._taskConfig.pdtoken,self._taskConfig.pduser))
    
    def writeLogs(self):
        '''
        '''
        log.debug("writeLogs %s msgs" % len(self.sync.logs))
        for msg in self.sync.logs:
            log.debug(msg)
        #self.sync.logs = []      
       
    def _connectZenoss(self):
        '''
            connect to Zenoss
        '''
        log.debug("_connectZenoss")
        def inner(driver):
            try:
                log.debug("syncing with %s" % self.configId)
                log.debug("using params: %s %s %s" % (self.zenhost, self.zenuser, self.zenpass))                
                self.sync.zenoss = ZenossHandler(self.zenhost, self.zenuser, self.zenpass, verbose=True)
                self.sync.zenoss.buffersize = self.eventsBuffer
                yield defer.succeed(None)
            except:
                yield defer.fail("Could not connect to Zenoss")
        #self.writeLogs()
        return drive(inner)
        
    
    def _connectPagerDuty(self):
        '''
            connect to PagerDuty
        '''
        log.debug("_connectPagerDuty")
        def inner(driver):
            try:
                log.debug("syncing with %s" % self.configId)
                log.debug("using params: %s %s %s " % (self.pdhost, self.pdtoken, self.pduser))
                self.sync.pagerduty = PagerDutyHandler(self.pdhost, self.pdtoken, True)
                self.sync.pagerduty.buffersize = self.eventsBuffer
                yield defer.succeed(None)
            except:
                yield defer.fail("Could not connect to PagerDuty")
        #self.writeLogs()
        return drive(inner)
        
    
    def _connectCallback(self, result):
        '''
            Callback for a successful asynchronous connection request.
        '''
        log.debug("_connectCallback")
        #self.writeLogs()
    
    def _collectCallback(self, result):
        '''
            Callback used to begin performance data collection asynchronously after
            a connection or task setup.
        '''
        log.debug("_collectCallback")
        d = self.fetch()
        d.addCallbacks(self._collectSuccessful, self._failure)
        d.addCallback(self._collectCleanup)
        #self.writeLogs()
        return d
        
    
    def _collectSuccessful(self, result):
        '''
            Callback for a successful asynchronous performance data collection
            request.
        '''
        log.debug("_collectSuccessful")
        self._eventService.sendEvent(ZenPagerDutyTask.CLEAR_EVENT, device=self.configId, summary="Device collected successfully")
        #self.writeLogs()
        
    def _collectCleanup(self, result):
        '''
            Callback after a successful collection to perform any cleanup after the
            data has been processed.
        '''
        log.debug("_collectCleanup")
        self.state = TaskStates.STATE_CLEANING
        self.close()
        #self.writeLogs()
        return defer.succeed(None)
        
    def close(self):
        '''
        '''
        #self.writeLogs()
        pass

    def _failure(self, result):
        '''
            Errback for an unsuccessful asynchronous connection or collection
            request.
        '''
        log.debug("_failure")
        err = result.getErrorMessage()
        log.error("Failed with error: %s" % err)
        self._eventService.sendEvent(ZenPagerDutyTask.WARNING_EVENT, device=self.configId, summary="Error collecting performance data: %s" % err)
        self.close()
        #self.writeLogs()
        return result
    
    def _finished(self, result):
        '''
            post collection activities
        '''
        if not isinstance(result, Failure):
            log.info("Successful scan of %s completed", self.configId)
        else:
            log.error("Unsuccessful scan of %s completed, result=%s", self.configId, result.getErrorMessage())
        #self.writeLogs()
        return result
    
    def cleanup(self):
        '''
            required post-collection method
        '''
        log.debug("cleanup")
        self.state = TaskStates.STATE_COMPLETED
        #self.writeLogs()
        return defer.succeed(None)
    
    def doTask(self):
        '''
            main task loop
        '''
        log.debug("doTask")
        self.state = TaskStates.STATE_RUNNING
        log.debug("USING %s" % self.configId)
        self.sync = Sync(self.zenhost, self.zenuser, self.zenpass, self.pdhost, self.pdtoken, self.pduser,verbose=True)
        log.debug("starting SYNC")
        # establish  connection to Zenoss
        d = self._connectZenoss()
        d.addCallbacks(self._connectCallback, self._failure)
        # establish  connection to PagerDuty
        d = self._connectPagerDuty()
        d.addCallbacks(self._connectCallback, self._failure)
        # now correlate/sync the events
        d.addCallback(self._collectCallback)
        d.addBoth(self._finished)
        #self.writeLogs()
        return d
    
    def fetch(self):
        '''
            data synchronization between RabbitMQ and OpenTSDB
        '''
        log.debug("fetch")
        def inner(driver):
            try:
                log.debug("correlate")
                self.sync.correlate()
                #self.writeLogs()
                log.debug("synchronize")
                self.sync.synchronize()
                self.writeLogs()
                yield defer.succeed("collection succeeded for %s" % self.configId)
            except:
                yield defer.fail("collection failed for %s" % self.configId)
        #self.writeLogs()
        return drive(inner)

if __name__ == '__main__':
    myPreferences = ZenPagerDutyPreferences()
    myTaskFactory = SimpleTaskFactory(ZenPagerDutyTask)
    myTaskSplitter = SimpleTaskSplitter(myTaskFactory)

    daemon = CollectorDaemon(myPreferences, myTaskSplitter)
    daemon.run()
    
