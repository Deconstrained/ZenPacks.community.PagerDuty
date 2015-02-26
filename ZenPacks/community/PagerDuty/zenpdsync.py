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
        self.cycleInterval = 120
        # How often the daemon will reload configuration. In minutes.
        self.configCycleInterval = 10 #360  
        self.options = None
    
    def buildOptions(self, parser):
        """ Required to implement the ICollectorPreferences interface. """
        parser.add_option('--eventsBuffer', dest='eventsBuffer',
                        default=100, type='int',
                        help='Number of events to pull from remote sources'
                        )

    def postStartup(self):
        """ Required to implement the ICollectorPreferences interface. """
        pass
    
    def getEventsBuffer(self): return self.options.eventsBuffer

class ZenPagerDutyTask(ObservableMixin):
    zope.interface.implements(IScheduledTask)
    
    CLEAR_EVENT = dict(device='localhost', component="zenpdsync", severity=Clear, eventClass='/Cmd/Fail')
    WARNING_EVENT = dict(device='localhost',component="zenpdsync", severity=Warning, eventClass='/Cmd/Fail')
    
    def __init__(self, taskName, deviceId, interval, taskConfig):
        super(ZenPagerDutyTask, self).__init__()
        self._taskConfig = taskConfig
        self._eventService = zope.component.queryUtility(IEventService)
        self._dataService = zope.component.queryUtility(IDataService)
        self._preferences = zope.component.queryUtility(ICollectorPreferences, 'zenpdsync')
        self.eventsBuffer = self._preferences.getEventsBuffer()
        
        self.name = taskName
        self.configId = deviceId
        #self.configId = "localhost"
        self.interval = interval
        self.state = TaskStates.STATE_IDLE
        self.zenhost = self._taskConfig.zenhost
        self.zenuser = self._taskConfig.zenuser
        self.zenpass = self._taskConfig.zenpass
        self.pdhost = self._taskConfig.pdhost
        self.pdtoken = self._taskConfig.pdtoken
        self.pduser = self._taskConfig.pduser
        self.sync = None
    
    def writeLogs(self):
        ''' '''
        #log.debug("writeLogs %s msgs" % len(self.sync.logs))
        for msg in self.sync.logs: log.debug(msg)
        self.sync.logs = []      
    
    def _connectZenoss(self):
        ''' connect to Zenoss '''
        log.info("Connecting to Zenoss")
        def inner(driver):
            try:
                #log.debug("using params: %s %s %s" % (self.zenhost, self.zenuser, self.zenpass))                
                self.sync.initZenoss()
                self.sync.zenoss.http.verbose = False
                yield defer.succeed("Connected to Zenoss")
            except:
                yield defer.fail("Could not connect to Zenoss")
        self.writeLogs()
        return drive(inner)
    
    def _connectPagerDuty(self):
        ''' connect to PagerDuty '''
        log.info("Connecting to PagerDuty")
        def inner(driver):
            try:
                #log.debug("using params: %s %s %s " % (self.pdhost, self.pdtoken, self.pduser))
                self.sync.initPagerDuty()
                self.sync.pagerduty.http.verbose = False
                yield defer.succeed("Connected to PagerDuty")
            except: yield defer.fail("Could not connect to PagerDuty")
        self.writeLogs()
        return drive(inner)
    
    def _connectCallback(self, result):
        '''  Callback for a successful connection request. '''
        log.debug(result)
        self.writeLogs()
    
    def _connectFailure(self, result):
        '''  Callback for a failed connection request. '''
        err = result.getErrorMessage()
        log.error("Connection failed with error: %s" % err)
        self._eventService.sendEvent(ZenPagerDutyTask.WARNING_EVENT, device='localhost', summary="Connection failed with error: %s" % err)
        self.close()
        self.writeLogs()
        return result
    
    def _collectCallback(self, result):
        '''
            Callback used to begin performance data collection asynchronously after
            a connection or task setup.
        '''
        log.debug("Starting correlation/synchronization")
        d = self.fetch()
        d.addCallbacks(self._collectSuccessful, self._collectFailure)
        d.addCallback(self._collectCleanup)
        self.writeLogs()
        return d
    
    def _collectSuccessful(self, result):
        '''
            Callback for a successful asynchronous performance data collection
            request.
        '''
        log.info("Event synchronization completed sucessfully")
        self._eventService.sendEvent(ZenPagerDutyTask.CLEAR_EVENT, device='localhost', summary="Event synchronization completed sucessfully")
        self.writeLogs()
    
    def _collectFailure(self, result):
        '''
            Errback for an unsuccessful asynchronous connection or collection
            request.
        '''
        err = result.getErrorMessage()
        log.error("Event synchronization failed with error: %s" % err)
        self._eventService.sendEvent(ZenPagerDutyTask.WARNING_EVENT, device='localhost', summary="Event synchronization failed with error: %s" % err)
        self.writeLogs()
        return result
    
    def _collectCleanup(self, result):
        '''
            Callback after a successful collection to perform any cleanup after the
            data has been processed.
        '''
        self.state = TaskStates.STATE_CLEANING
        self.close()
        self.writeLogs()
        return defer.succeed(None)
    
    def _finished(self, result):
        ''' post collection activities '''
        if not isinstance(result, Failure): log.info("Successful synchronization finished")
        else: log.error("Failed synchronization finished, result=%s" % result.getErrorMessage())
        self.writeLogs()
        return result
    
    def close(self): 
        '''required method'''
        pass
    
    def cleanup(self):
        ''' required post-collection method '''
        self.state = TaskStates.STATE_COMPLETED
        self.writeLogs()
        return defer.succeed(None)
    
    def doTask(self):
        ''' main task loop '''
        log.debug("Beginning correlation/synchronization task")
        self.state = TaskStates.STATE_RUNNING
        self.sync = Sync(self.zenhost, self.zenuser, self.zenpass, self.pdhost, self.pdtoken, self.pduser, verbose=True)
        self.sync.buffersize = self.eventsBuffer
        # establish  connection to Zenoss
        d = self._connectZenoss()
        d.addCallbacks(self._connectCallback, self._connectFailure)
        # establish  connection to PagerDuty
        d = self._connectPagerDuty()
        d.addCallbacks(self._connectCallback, self._connectFailure)
        # now correlate/sync the events
        d.addCallback(self._collectCallback)
        d.addBoth(self._finished)
        self.writeLogs()
        return d
    
    def fetch(self):
        ''' data synchronization between Zenoss and PagerDuty '''
        def inner(driver):
            try:
                self.sync.synchronize()
                self.writeLogs()
                yield defer.succeed("Synchronization succeeded for %s" % 'localhost')
            except: yield defer.fail("Synchronization failed for %s" % 'localhost')
        self.writeLogs()
        return drive(inner)



if __name__ == '__main__':
    myPreferences = ZenPagerDutyPreferences()
    myTaskFactory = SimpleTaskFactory(ZenPagerDutyTask)
    myTaskSplitter = SimpleTaskSplitter(myTaskFactory)
    daemon = CollectorDaemon(myPreferences, myTaskSplitter)
    daemon.run()

