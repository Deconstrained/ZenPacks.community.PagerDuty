#!/usr/bin/env python
import logging
log = logging.getLogger('zen.zenpdsync')

import datetime,time
from optparse import OptionParser
from HTTPHandler import HTTPHandler
from ZenossHandler import ZenossHandler
from PagerDutyHandler import PagerDutyHandler
from MessageHandler import MessageHandler
import Globals
from Products.ZenModel.ZVersion import VERSION as ZENOSS_VERSION

# optionally populate below or supply via command line
ZENOSS_HOST = ''
ZENOSS_USERNAME = ''
ZENOSS_PASSWORD = ''
EVID = ''

PAGERDUTY_TOKEN = ''
PAGERDUTY_HOST = ''
PAGERDUTY_SERVICE  = ''
PAGERDUTY_SERVICEKEY = ''
PAGERDUTY_USER = ''
     
class Main():
    """
        Either:
            1) Create Pagerduty Incident based on new Zenoss event
            2) Synchronize Zenoss and Pagerduty Events and Incidents
    """
    def __init__(self):
        """
        """
        self.getOptions()
        self.sync = Sync(self.options.zenhost,self.options.zenuser,self.options.zenpass,
                         self.options.pdhost,self.options.pdtoken,self.options.pduser,
                         self.options.verbose)
         
    def getOptions(self):
        """
            Command line runtime arguments
        """
        usage = "usage: %prog [options] arg"
        parser = OptionParser(usage)
        # options for Zenoss
        parser.add_option("-z", "--zenhost", dest="zenhost", help="Zenoss Server hostname", default=ZENOSS_HOST)
        parser.add_option("-u", "--zenuser", dest="zenuser", help="Zenoss admin username", default=ZENOSS_USERNAME)
        parser.add_option("-p", "--zenpass", dest="zenpass", help="Zenoss admin password", default=ZENOSS_PASSWORD)
        parser.add_option("-e", "--evid", dest="evid", help="Zenoss Event ID", default=EVID)
        
        # options for Pagerduty 
        parser.add_option("-H", "--pdhost", dest="pdhost", help="Pagerduty hostname", default=PAGERDUTY_HOST)
        parser.add_option("-T", "--pdtoken", dest="pdtoken", help="Pagerduty token", default=PAGERDUTY_TOKEN)
        parser.add_option("-U", "--pduser", dest="pduser", help="Pagerduty User Key (for tracking auto updates)", default=PAGERDUTY_USER)
        parser.add_option("-S", "--servicekey", dest="servicekey", help="Pagerduty Service Key", default=PAGERDUTY_SERVICEKEY)
        
        # action to be performed
        parser.add_option("-a", "--action", dest="action", help="one of [create|update]", default="update")
        parser.add_option("-v", "--verbose", dest="verbose", help="Show additional output", action="store_true")

        # options for zenoss interaction
        (self.options, self.args) = parser.parse_args()
        if self.options.verbose != True:
            self.options.verbose = False
            
    def run(self):
        """
            control script execution
        """
        self.sync.initialize()
        # when creating ticket from command line
        if self.options.action == 'create':
            self.sync.messenger.newId(self.options.evid)
            self.sync.createPagerDutyIncident(self.options.evid, self.options.servicekey)
            self.sync.updateCreatedIssue()
        # same as actions by zenpdsync
        if self.options.action == 'update':
            self.sync.correlate()
            self.sync.synchronize()

class Sync():
    ''''''
    def __init__(self, zenhost, zenuser, zenpass, pdhost, pdtoken, pduser, verbose=False):
        ''''''
        self.zenhost = zenhost
        self.zenuser = zenuser
        self.zenpass = zenpass
        self.pdhost = pdhost
        self.pdtoken = pdtoken
        self.pduser = pduser
        self.logs = [] # array to hold log messages
        self.verbose = verbose
        self.buffersize = 100
        self.statusData = {
                           "ack": {
                                   "zenoss":"Acknowledged",
                                   "pagerduty":"acknowledged",
                                   "zenaction":"acknowledge",
                                   "num":1
                                   },
                           "close":{
                                    "zenoss":"Closed",
                                    "pagerduty":"resolved",
                                    "zenaction":"close",
                                    "num":2
                                    },
                           "open":{
                                   "zenoss":"New",
                                   "pagerduty":"triggered",
                                   "zenaction":"reopen",
                                   "num":0},
                           }
        self.zenver = self.isVersion4()         
        if self.zenver is True: #zenoss 4 changed "unacknowledge" action to "reopen"
            self.statusData["open"]["zenaction"] = "reopen"
        self.messenger = MessageHandler()
        self.statusDict = {}
        self.commonkeys = []
    
    def mapCurrentStatus(self, evid, data, pd=False):
        '''
            map event/incident status to local dictionary
        '''
        ZENMAP = {'New': 'open', 'Acknowledged': 'ack', 'Closed': 'close', 'Cleared': 'close', 'Aged': 'close'}
        PDMAP = {'triggered': 'open', 'acknowledged': 'ack', 'resolved': 'close'}
        if pd is False:
            self.statusDict[evid]['zenoss'] = data
            self.statusDict[evid]["zencurrent"] = ZENMAP[data['eventStateString']]
        else:
            self.statusDict[evid]['pagerduty'] = data
            self.statusDict[evid]["pdcurrent"] = PDMAP[data['status']]
    
    def initZenoss(self):
        '''initialize connection to Zenoss'''
        self.zenoss = ZenossHandler(self.zenhost, self.zenuser, self.zenpass, self.verbose)
        self.zenoss.buffersize = self.buffersize
    
    def initPagerDuty(self):
        '''initialize connection to PagerDuty'''
        self.pagerduty = PagerDutyHandler(self.pdhost, self.pdtoken, self.verbose)
        self.pagerduty.buffersize = self.buffersize
    
    def initialize(self):
        '''initialize connection to both Zenoss and PagerDuty'''
        self.initZenoss()
        self.initPagerDuty()
    
    def isVersion4(self):
        '''detect Zenoss version'''
        from Products.ZenUtils.Version import Version
        if Version.parse('Zenoss ' + ZENOSS_VERSION) >= Version.parse('Zenoss 4'): return True
        return False
    
    def getMaintenanceWindows(self, service):
        """
            return list of maintenance windows (ongoing) for a given service
        """
        if self.verbose is True: self.writelog("Finding PagerDuty Maintenance Windows for %s" % service['id'])
        output = []
        windows = self.pagerduty.getMaintenanceWindows()["maintenance_windows"]
        for w in windows:
            services = w["services"]
            for s in services:
                if s["id"] == service["id"]:
                    beg = self.messenger.getLocalTime(w["start_time"])
                    fin = self.messenger.getLocalTime(w["end_time"])
                    start = self.messenger.getTimestamp(beg)
                    end = self.messenger.getTimestamp(fin) 
                    now = time.time()
                    if start <= now and now <=end: output.append(w)
        return output
    
    def getZenEvent(self, evid, history=False):
        ''''''
        output = self.zenoss.getEventDetails(evid)
        if 'result' not in output.keys(): return None
        if 'event' not in output['result'].keys(): return None
        if len(output['result']['event']) != 1: return None
        return output["result"]["event"][0]
    
    def getPDIncident(self, evid):
        ''''''
        output = self.pagerduty.getIncidentByKey(evid)
        if 'incidents' not in output.keys(): return None
        if len(output['incidents']) != 1: return None
        self.addStatusDictEntry(evid, output['incidents'][0], True)
        return output['incidents'][0]
    
    def getZenEventDict(self, evid):
        '''
            standardize zenoss 3.x vs. 4.x output
        '''
        if self.verbose is True: self.writelog("Finding Zenoss event details for %s" % evid)
        # map of string to numeric event status
        STATUSMAP3X = { 0: 'New', 1: 'Acknowledged', 2: 'Closed', }
        STATUSMAP4X = { 'New': 0, 'Acknowledged': 1, 'Closed': 2, 'Cleared': 2, 'Aged': 2, }
        data = {'history': False}
        # try getting from current console
        event = self.getZenEvent(evid)
        if self.zenver == False: # was not pulled from history
            # if nothing, try looking in history
            if event is None: 
                event = self.getZenEvent(evid, True)
                data['eventState'] = 2 # setting to close since this is in history
                data['history'] = True # was pulled from history
            if event is None: return None
            details = event["properties"]
            for d in details: data[str(d["key"])] = str(d["value"])
            data['eventStateInt'] = data['eventState']
            data['eventStateString'] = STATUSMAP3X[data['eventState']]
            data['log'] = output['log']
        else: # zenoss 4.x output
            if event is None: return None
            data.update(event)
            data['history'] = False
            data['eventStateString'] = data['eventState']
            data['eventStateInt'] = STATUSMAP4X[data['eventState']]
        self.addStatusDictEntry(evid, data)
        return data
    
    def addStatusDictEntry(self, evid, data, pd=False):
        '''
            add entry to status dict for this evid
        '''
        if evid not in self.statusDict.keys(): 
            self.statusDict[evid] = {'target': None, 'zencurrent': None, 'pdcurrent': None, 'zenoss' : None, 'pagerduty': None,}
        self.mapCurrentStatus(evid, data, pd)
    
    def createIncidentDetails(self, evid, servicekey):
        """
            Retrieve event detail from Zenoss and format it 
            for PagerDuty incident creation
        """
        if self.verbose is True: self.writelog("Getting incident details")
        data = self.getZenEventDict(evid)
        info = {
                "service_key": servicekey,
                "incident_key": evid,
                "details" : self.statusDict[evid]['zenoss'],
                "description" : None,
                }
        self.statusDict[evid]["target"] = "open"
        info["details"] = data
        info["description" ] = ' | '.join([str(data['device']), str(data['component']), str(data['summary'])])
        return info
    
    def createPagerDutyIncident(self, evid, servicekey):
        """
        1) check whether the destination service is in a maintenance window.  
            a) If so, ack the local alert in zenoss 
            b) and add a "in maintenance window" log message to the Zenoss event details.

        2) if it is not in maintenance, proceed with the event submission. as usual
            a) send event to pagerduty
            b) update Zenoss console with submit status info
            c) update Zenoss Event with incident details (incident URL, service URL, oncall)
        3) check issues in Zenoss w/pagerduty incidents:
            a) if acked in Zenoss, ack in PagerDuty
            b) if closed in Zenoss, resolve in PagerDuty
        """
        if self.verbose is True: self.writelog("Creating PagerDuty Incident")
        info = self.createIncidentDetails(evid, servicekey)
        # first ensure that event is open in zenoss
        if self.statusDict[evid]["zencurrent"]  == 'open':
            # first find the appropriate PD service definition
            if self.verbose is True: self.writelog("looking for service using key %s" % servicekey)
            service = self.pagerduty.findService(servicekey)
            if service is not None:
                if self.verbose is True: self.writelog("found service using key %s" % servicekey)
                 # in maintenance, so ack the zenoss alert but note the window detail
                if self.pagerduty.inMaintenance(service) is True:
                    if self.verbose is True: self.writelog("service in maintenance using key %s" % servicekey)
                    self.statusDict[evid]["target"] = "ack"
                    mws = self.getMaintenanceWindows(service) 
                    for mw in mws: self.messenger.serviceInMaintenance(evid, "Acknowledged", service, mw, self.pagerduty.weburl)
                # disabled, so leave event unacked in Zenoss, but that service is disabled    
                elif self.pagerduty.isDisabled(service) is True:
                    if self.verbose is True: self.writelog("service disabled using key %s" % servicekey)
                    self.messenger.serviceIsDisabled(evid, "No Incident created", service, self.pagerduty.weburl)
                # assuming service is enabled, create PD incident, note output in zenoss event console.
                else:
                    if self.verbose is True: self.writelog("Creating incident for %s using key %s" % (servicekey, evid))
                    output = self.pagerduty.manageIncident(info, "trigger")
                    if 'errors' in output.keys():  errors = output['errors']
                    else: errors = None
                    self.messenger.serviceIncidentCreated(evid, service, self.pagerduty.weburl, errors)
            else:
                if self.verbose is True: self.writelog("No service found using key %s" % servicekey)
                self.messenger.serviceNotFound(evid, servicekey)
        else: # if not open in zenoss, set the target to whatever is current
            if self.verbose is True: self.writelog("Event id %s not open in Zenoss" % evid)
            self.statusDict[evid]["target"] = self.statusDict[evid]["zencurrent"]
    
    def getPDKeys(self, open=False):
        '''get incident ids for last N incidents (buffersize)'''
        pdkeys = []
        # get PagerDuty incidents
        incidents = self.pagerduty.getIncidentList(open=open)
        if 'incidents' not in incidents.keys(): return pdkeys
        # make list of event ids for both
        for i in incidents["incidents"]: pdkeys.append(str(i['incident_key']))
        return pdkeys
    
    def getZenKeys(self, open=False):
        '''get the evids for the last N events (buffersize)'''
        zenkeys = []
        try: events = self.zenoss.getEvents(open=open)["events"]
        except: events = []
        # zenoss 3.x compat
        if self.zenver is False: 
            try: events += self.zenoss.getEvents(history=True, open=open)["events"]
            except:  pass
        # make list of event ids for both
        for e in events:  zenkeys.append(str(e['evid']))
        self.writelog('Found %s open Zenoss events' % len(zenkeys))
        return zenkeys

    def correlate(self):
        '''
            1) get list of open PD incidents
            2) correlate with open or closed Zenoss events
            3) get list of open Zenoss incidents not in 2)
            4) correlate with open or closed PD incidents
            build dictionary of Zenoss events matched to PagerDuty incidents
            1) get list of zenoss events
            2) get list of pagerduty incidents based on date range of zenoss events
            3) match them by evid - incident_key
        '''
        if self.verbose is True: self.writelog("Finding correlated Zenoss events and PagerDuty incidents")
        # look at all open PD incidents
        pdkeys = self.getPDKeys(True)
        # find the matching PD/zenoss data
        for id in pdkeys:
            zdata = self.getZenEventDict(id) # get zenoss data
            if zdata is None: continue
            pddata = self.getPDIncident(id)
            if pddata is None:  continue
            if id not in self.commonkeys: self.commonkeys.append(id)
        # look at all open zenoss events
        zenkeys = self.getZenKeys(True)
        # find matching PD data
        for id in zenkeys:
            # no reason to do work twice
            if id in self.commonkeys: 
                self.writelog('skipping already processed id: %s' % id)
                continue
            pddata = self.getPDIncident(id)
            if pddata is None:  continue
            zdata = self.getZenEventDict(id) # get zenoss data
            if zdata is None: continue
            if id not in self.commonkeys: self.commonkeys.append(id)
    
    def synchronize(self):
        """
            update pagerduty based on Zenoss event console activities (acknowledge, resolve, etc)

            4) if acked in one, ack in the other
            5) if closed in one, close in the other
        """
        self.correlate()
        if self.verbose is True: self.writelog("Synchronizing correlated Zenoss events and PagerDuty incidents")
        for id, data in self.statusDict.items():
            if self.verbose is True: self.writelog('Synchronizing %s' % self.eventInfoMsg(data))
            # map current states of each to statusDict
            self.evalStatus(data)
            # determine which was updated last
            self.findNewest(data)
            # update pd and zenoss
            self.updateStatus(data)
            # gather/format any log messages in pagerduty
            self.buildMessages(data)
            # update Zenoss event logs 
            self.updateZenossMessages(data)
                
    def evalStatus(self, data):
        '''update the status dictionary entry'''
        if self.verbose is True: self.writelog("Evaluating status data: %s" % self.eventInfoMsg(data))
        for status, tdata in self.statusData.items():
            if tdata["zenoss"] == data['zenoss']['eventStateString']: data['zencurrent'] = status
            if tdata["pagerduty"] == data['pagerduty']['status']: data['pdcurrent'] = status
    
    def buildMessages(self, data):
        ''' build the log messages that will show in the Zenoss console'''
        if self.verbose is True: self.writelog("Building messages for %s" % self.eventInfoMsg(data))
        # gather/format any log messages in pagerduty
        evid = str(data['zenoss']['evid'])
        pdid = str(data['pagerduty']['id'])
        # basic incident creation message
        self.messenger.incidentCreated(evid, data['pagerduty'])
        # any others from pagerduty
        logs = self.pagerduty.getIncidentDetail(pdid)["log_entries"]
        if self.verbose is True: self.writelog("Found %s incident logs for %s" % (len(logs), self.eventInfoMsg(data)))
        self.messenger.incidentLogs(evid, logs, self.pagerduty.weburl)
    
    def findNewest(self, data):
        ''''''
        if self.verbose is True: self.writelog("finding newest source for %s" % self.eventInfoMsg(data))
        # event was pulled from history
        if data['zenoss']['history'] is True: zenosstime = data['zenoss']['deletedTime']
        # otherwise just the last time it changed
        else:  zenosstime = data['zenoss']['stateChange']
        pagerdutytime = data['pagerduty']["last_status_change_on"]
        # both are converted to local time 
        zenosstime_local = self.messenger.getZenossTime(zenosstime)
        pagerdutytime_local = self.messenger.getPagerDutyTime(pagerdutytime)
        if self.verbose is True: self.writelog("%s updated in Zenoss at %s and PagerDuty at %s" % (self.eventInfoMsg(data), zenosstime_local, pagerdutytime_local))
        # then both are converted to integers
        zenosstimestamp = self.messenger.getTimestamp(zenosstime_local)
        pagerdutytimestamp = self.messenger.getTimestamp(pagerdutytime_local)
        if zenosstimestamp < pagerdutytimestamp: # pagerduty updated last
            data['target'] = data['pdcurrent']
            newest = "PAGERDUTY"
        else:
            data['target'] = data['zencurrent']
            newest = "ZENOSS"
        if self.verbose is True: self.writelog("%s last updated in %s with %s status" % (self.eventInfoMsg(data), newest, data['target'].upper()))
    
    def updateStatus(self, data):
        '''
            perform updates on Zenoss and Pagerduty
        '''
        if self.verbose is True: self.writelog("Updating status for %s" % self.eventInfoMsg(data))
        # zenoss event id
        evid = str(data['zenoss']['evid'])
        # pagerduty incident id
        pdid = data['pagerduty']['id'] 
        # zenoss current state
        zencurrent = data['zencurrent']
        # pagerduty current state
        pdcurrent = data['pdcurrent']
        # target state
        target = data['target']
        # check Zenoss status against target
        if zencurrent != target: 
            # ze:enoss 3.x does not change the status of a closed event
            if target == "close" and data['zenoss']['history'] is True:
                if self.verbose is True: self.writelog("Cannot update historical event %s" % self.eventInfoMsg(data))
            else:
                if self.verbose is True: self.writelog("Changing Zenoss event %s status from %s to %s" % (self.eventInfoMsg(data), zencurrent.upper(), target.upper()))
                self.zenoss.manageEventStatus([evid], self.statusData[target]['zenaction'])
        # check PagerDuty status against target
        if pdcurrent != target:
            if self.verbose is True: self.writelog("Changing PagerDuty incident %s status from %s to %s" % (self.eventInfoMsg(data), pdcurrent.upper(), target.upper()))
            try:
                pdData = {"id": pdid, "status":  self.statusData[target]["pagerduty"]} 
                # test individually
                update = {"incidents": [pdData], "requester_id": self.pduser}
                self.pagerduty.updateStatus(update)
            except: self.writelog("%s update pagerduty failed changing %s to %s" % (self.eventInfoMsg(data), pdcurrent.upper(), target.upper()))
    
    def updateZenossStatus(self, data):
        """
            update Zenoss event status if target is not current
        """
        if self.verbose is True: self.writelog("Checking Zenoss status for %s" % self.eventInfoMsg(data))
        # zenoss event id
        evid = str(data['zenoss']['evid'])
        current = data["zencurrent"]
        target = data["target"]
        if current != target:
            if self.verbose is True: self.writelog("Changing %s status to %s" % (self.eventInfoMsg(data), target.upper()))
            self.zenoss.manageEventStatus([evid], self.statusData[target]['zenaction'])
    
    def updateZenossMessages(self, data):
        """
            update Zenoss event console with messages if they don't exist
        """
        if self.verbose is True: self.writelog("Checking Zenoss messages for %s" % self.eventInfoMsg(data))
        # zenoss event id
        evid = str(data['zenoss']['evid'])
        # get list of current event log messages
        zlogs = []
        for e in data['zenoss']['log']: zlogs.append(str(e[-1]))
        for msg in self.messenger.messages[evid]:
            #self.writelog('examining message: "%s"' % msg)
            if msg not in zlogs:
                if self.verbose is True:  self.writelog('Adding message to %s: "%s"' % (self.eventInfoMsg(data), msg))
                self.zenoss.addEventMessage(evid, msg)
    
    def updateCreatedIssue(self):
        ''' update zenoss status and messages for new issue'''
        if self.verbose is True: self.writelog("updating created issue")
        for id, data in self.statusDict.items():
            self.updateZenossStatus(data)
            self.updateZenossMessages(data)
    
    def writelog(self,msg):
        ''' print log message and append to list for log handling'''
        self.logs.append(msg)
        print msg
    
    def eventInfoMsg(self, data):
        '''convenience for log messages'''
        try: return '%s (%s) "%s"' % (str(data['zenoss']['evid']), data['pagerduty']['id'], data['zenoss']['summary'])
        except: return '%s "%s"' % (str(data['zenoss']['evid']), data['zenoss']['summary'])
    

if __name__ == "__main__":
    u = Main()
    u.run()

