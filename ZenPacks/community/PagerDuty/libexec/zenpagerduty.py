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
        #parser.add_option("-V", "--zenver", dest="zenver", help="True if Zenoss version >= 4", action="store_true")
        
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
        #if self.options.zenver != True:
        #    self.options.zenver = False
        if self.options.verbose != True:
            self.options.verbose = False
            
    def run(self):
        """
            control script execution
        """
        self.sync.initialize()
        if self.options.action == 'create':
            #self.sync.zenver = ZENVER
            self.sync.evid = self.options.evid
            self.sync.servicekey= self.options.servicekey
            self.sync.messenger.newId(self.options.evid)
            self.sync.createPagerDutyIncident()
            self.sync.updateZenossMessages()
            self.sync.updateZenossStatus()
            
        if self.options.action == 'update':
            self.sync.correlate()
            self.sync.synchronize()

class Sync():
    ''''''
    def __init__(self,zenhost,zenuser,zenpass,pdhost,pdtoken,pduser,verbose=False):
        ''''''
        self.zenhost = zenhost
        self.zenuser = zenuser
        self.zenpass = zenpass
        self.pdhost = pdhost
        self.pdtoken = pdtoken
        self.pduser = pduser
        self.servicekey = None
        self.evid = None
        self.logs = [] # array to hold log messages
        self.verbose = verbose
        self.buffersize = 20
        self.statusData = {
                           "ack": {"zenoss":"Acknowledged","pagerduty":"acknowledged","zenaction":"acknowledge","num":1},
                           "close":{"zenoss":"Closed","pagerduty":"resolved","zenaction":"close","num":2},
                           "open":{"zenoss":"New","pagerduty":"triggered","zenaction":"unacknowledge","num":0},
                           }
        self.zenver = self.isVersion4()         
        if self.verbose == True:
            self.writelog("ZENOSS > 4.0 is %s" % self.zenver)
            
        if self.zenver == True: #zenoss 4 changed "unacknowledge" action to "reopen"
            self.statusData["open"]["zenaction"] = "reopen"
        self.messenger = MessageHandler()
        self.statusDict = {}
        
    def writelog(self,msg):
        '''
        '''
        self.logs.append(msg)
        print "now %s logs" % len(self.logs)
        print msg
    
    def initialize(self):
        #self.zenoss = ZenossHandler(self.zenhost, self.zenuser, self.zenpass, self.verbose)
        #self.pagerduty = PagerDutyHandler(self.pdhost, self.pdtoken, self.verbose)
        self.zenoss = ZenossHandler(self.zenhost, self.zenuser, self.zenpass, False)
        self.pagerduty = PagerDutyHandler(self.pdhost, self.pdtoken, False)
        self.zenoss.buffersize = self.pagerduty.buffersize = self.buffersize
    
    def isVersion4(self):
        from Products.ZenUtils.Version import Version
        if Version.parse('Zenoss ' + ZENOSS_VERSION) >= Version.parse('Zenoss 4'):
            return True
        else:
            return False
    
    def lastDisabled(self,service):
        """
            return PagerDuty newest disabled entry
        """
        if self.verbose == True:
            self.writelog("lastDisabled")
        message = ""
        details = self.pagerduty.getServiceLog(service["id"])["log_entries"]
        last = 0
        lastentry = None
        for d in details:
            if d["maintenance_window"]["type"] == "disable":
                starttime = self.messenger.getLocalTime(d["maintenance_window"]["time"])
                start = self.messenger.getTimestamp(starttime)
                if start > last:
                    lastentry = d
                    last = start
        return lastentry
        
    def getMaintenanceWindows(self,service):
        """
            return list of maintenance windows (ongoing) for a given service
        """
        if self.verbose == True:
            self.writelog("getMaintenanceWindows")
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
                    if start <= now and now <=end:
                        output.append(w)
        return output
    
    def eventDetailDict(self,evid):
        '''
            standardize zenoss 3.x vs. 4.x output
        '''
        if self.verbose == True:
            self.writelog("eventDetailDict %s" % evid)
        data = {}
        #output = self.zenoss.getEventDetails(evid)["result"]["event"][0]
        if self.zenver == False:
            try: # zenoss 3.x output
                output = self.zenoss.getEventDetails(evid)["result"]["event"][0]
            except:
                output = self.zenoss.getEventDetails(evid,True)["result"]["event"][0]
            details = output["properties"]
            for d in details:
                k = d["key"]
                v = d["value"]
                data[k] = v
            data['eventStateInt'] = data['eventState']
            if data['eventState'] == 0:
                data['eventStateString'] = 'New'
            elif data['eventState'] == 1:
                data['eventStateString'] = 'Acknowledged'
            elif data['eventState'] == 2:
                data['eventStateString'] = 'Closed'
            data['log'] = output['log']
            
        else: # zenoss 4.x output
            output = self.zenoss.getEventDetails(evid)["result"]["event"][0]
            data = output#self.zenoss.getEventDetails(evid)["result"]["event"][0]
            data['eventStateString'] = data['eventState']
            if data['eventState'] == 'New':
                data['eventStateInt'] = 0
            elif data['eventState'] == 'Acknowledged':
                data['eventStateInt'] = 1
            elif data['eventState'] == 'Closed':
                data['eventStateInt'] = 2
            elif data['eventState'] == 'Cleared':
                data['eventStateInt'] = 2
        
        self.addStatusDictEntry(evid, data)
        return data
    
    def addStatusDictEntry(self,evid,data,pd=False):
        '''
            add entry to status dict for this evid
        '''
        self.statusDict[evid] = {
                                 'target': None,
                                 'zencurrent': None,
                                 'pdcurrent': None,
                                 'zenoss' : None,
                                 'pagerduty': None,
                                 'messages' : [],
                                 'pdnewer' : False,
                                 }
        self.mapCurrentStatus(evid, data, pd)

    def mapCurrentStatus(self,evid,data,pd=False):
        if pd == False:
            self.statusDict[evid]['zenoss'] = data
            if data['eventStateString'] == 'New':
                self.statusDict[evid]["zencurrent"] = "open"
                
            elif data['eventStateString'] == 'Acknowledged':
                self.statusDict[evid]["zencurrent"] = "ack"
            elif data['eventStateString'] == 'Closed':
                self.statusDict[evid]["zencurrent"] = "close"
            elif data['eventStateString'] == 'Cleared':
                self.statusDict[evid]["zencurrent"] = "close"
        else:
            self.statusDict[evid]['pagerduty'] = data
            if data['status'] == 'triggered':
                self.statusDict[evid]["pdcurrent"] = "open"
            elif data['status'] == 'acknowledged':
                self.statusDict[evid]["pdcurrent"] = "ack"
            elif data['status'] == 'resolved':
                self.statusDict[evid]["pdcurrent"] = "close"

    def getIncidentLogs(self,id,evid):
        if self.verbose == True:
            self.writelog("getIncidentLogs %s %s" % (id,evid))
        details = self.pagerduty.getIncidentDetail(evid)["log_entries"]
        self.messenger.incidentLogs(evid, details, self.pagerduty.weburl)
    
    def createIncidentDetails(self):
        """
            Retrieve event detail from Zenoss and format it 
            for PagerDuty incident creation
        """
        if self.verbose == True:
            self.writelog("createIncidentDetails")
        data = self.eventDetailDict(self.evid)
        self.incidentData = {
                "service_key": self.servicekey,
                "incident_key": self.evid,
                "details" : self.statusDict[self.evid]['zenoss'],
                "description" : None,
                }
        self.statusDict[self.evid]["target"] = "open"
        self.incidentData["details"] = data
        self.incidentData["description" ] = "%s | %s | %s" % (data['device'], data['component'], data['summary'])
            
    def createPagerDutyIncident(self):
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
        if self.verbose == True:
            self.writelog("createPagerDutyIncident")
        self.createIncidentDetails()
        # first ensure that event is open in zenoss
        if self.statusDict[self.evid]["zencurrent"]  == 'open':
            # first find the appropriate PD service definition
            if self.verbose == True:
                self.writelog("looking for service using key %s" % self.servicekey)
            service = self.pagerduty.findService(self.servicekey)
            if service:
                if self.verbose == True:
                    self.writelog("found service using key %s" % self.servicekey)
                 # in maintenance, so ack the zenoss alert but note the window detail
                if self.pagerduty.inMaintenance(service) == True:
                    if self.verbose == True:
                        self.writelog("service in maintenance using key %s" % self.servicekey)
                    self.statusDict[self.evid]["target"] = "ack"
                    mws = self.getMaintenanceWindows(service) 
                    for mw in mws:
                        self.messenger.serviceInMaintenance(self.evid, "Acknowledged", service, mw, self.pagerduty.weburl)
                # disabled, so leave event unacked in Zenoss, but that service is disabled    
                elif self.pagerduty.isDisabled(service) == True:
                    if self.verbose == True:
                        self.writelog("service disabled using key %s" % self.servicekey)
                    self.messenger.serviceIsDisabled(self.evid, "No Incident created", service, self.lastDisabled(service), self.pagerduty.weburl)
                
                # assuming service is enabled, create PD incident, note output in zenoss event console.
                else:
                    if self.verbose == True:
                        self.writelog("creating ticket for %s using key %s" % (self.servicekey,self.evid))
                    output = self.pagerduty.manageIncident(self.incidentData,"trigger")
                    try:
                        self.messenger.serviceIncidentCreated(self.evid, service, self.pagerduty.weburl, output["errors"])
                    except KeyError:
                        self.messenger.serviceIncidentCreated(self.evid, service, self.pagerduty.weburl)

            else:
                if self.verbose == True:
                    self.writelog("no service found using key %s" % self.servicekey)
                self.messenger.serviceNotFound(self.evid, self.servicekey)
        else: # if not open in zenoss, set the target to whatever is current
            if self.verbose == True:
                self.writelog("event id %s not open" % self.evid)
            self.statusDict[self.evid]["target"] = self.statusDict[self.evid]["zencurrent"]
        # updated messages
        self.statusDict[self.evid]['messages'] = self.messenger.messages[self.evid]
       
    def correlate(self):
        '''
            build dictionary of Zenoss events matched to PagerDuty incidents
            1) get list of zenoss events
            2) get list of pagerduty incidents based on date range of zenoss events
            3) match them by evid - incident_key
        '''
        if self.verbose == True:
            self.writelog("correlate")
        events = self.zenoss.getEvents()["events"]
        if self.zenver == False: # get history events if zenoss 3.x
            events += self.zenoss.getEvents(True)["events"]
        incidents = self.pagerduty.getIncidentList()["incidents"]
        # list of current zenoss events
        evids = []
        inids = []
        # make list of event ids for both
        for e in events:
            evids.append(e['evid'])
        for i in incidents:
            inids.append(i['incident_key'])
        eventids = list(set(evids).intersection(set(inids)))
        
        if self.verbose == True:
            self.writelog("using last %s zenoss events" % len(evids))
            self.writelog("using last %s pagerduty incidents" % len(inids))
            # find intersection of both lists (set that exists on both sides)
            self.writelog("using %s shared events" % len(eventids))
        # build status dictionary for all events
        for evid in eventids:
            # zenoss data
            self.eventDetailDict(evid)
            # pagerduty data
            data = self.pagerduty.getIncidentByKey(evid)["incidents"][0]
            self.mapCurrentStatus(evid,data,True)

    def synchronize(self):
        """
            update pagerduty based on Zenoss event console activities (acknowledge, resolve, etc)

            4) if acked in one, ack in the other
            5) if closed in one, close in the other
        """
        if self.verbose == True:
            self.writelog("synchronize")
        for id, data in self.statusDict.items():
            if self.verbose == True:
                self.writelog("examining %s summary %s" % (id,data['zenoss']['summary']))
            zen = data['zenoss']
            pd = data['pagerduty']
            # map current states of each to statusDict
            for target, tdata in self.statusData.items():
                if tdata["zenoss"] == zen['eventStateString']:
                    data['zencurrent'] = target
                if tdata["pagerduty"] == pd['status']:
                    data['pdcurrent'] = target
            # gather/format any log messages in pagerduty
            self.messenger.newId(id)
            self.messenger.incidentCreated(id, pd)
            details = self.pagerduty.getIncidentDetail(pd["id"])["log_entries"]
            self.messenger.incidentLogs(id, details, self.pagerduty.weburl)
            # add log messages to status dict
            data['messages'] = self.messenger.messages[id]
        # determine which was updated last
        self.findNewest()
        # determine target states
        self.setTargetStates()
        # update pd and zenoss
        self.updateEntries()
        # update Zenoss event logs 
        self.updateZenossMessages()
        
    def findNewest(self):
        '''
            determine whether Zenoss event or PD incident was updated last
        '''
        if self.verbose == True:
            self.writelog("findNewest")
        for id, data in self.statusDict.items():
            zen = data['zenoss']
            pd = data['pagerduty']
            if zen is not None and pd is not None:
                eupdated = zen['stateChange']
                iupdated = pd["last_status_change_on"] 
                # both are converted to local time 
                zt = self.messenger.getZenossTime(eupdated)
                pt = self.messenger.getPagerDutyTime(iupdated)
                # then both are converted to integers
                zentime = self.messenger.getTimestamp(zt)
                pdtime = self.messenger.getTimestamp(pt)
                if zentime < pdtime: # pagerduty updated last
                    if self.verbose == True:
                       self.writelog("%s pd time %s newer than zenoss time %s" % (id,pt,zt))
                    data['pdnewer'] = True
                else:
                    if self.verbose == True:
                        self.writelog("%s pd time %s older than zenoss time %s" % (id,pt,zt))
            
    def setTargetStates(self):
        '''
            determine whether each entry in statusDict should be open or closed
        '''
        if self.verbose == True:
            self.writelog("setTargetStates")
        for id, data in self.statusDict.items():
            zen = data['zenoss']
            pd = data['pagerduty']
            if data['pdnewer'] == True:
                data['target'] = data['pdcurrent']
            else:
                data['target'] = data['zencurrent']
            if self.verbose == True:
                self.writelog("%s target: %s zencurrent: %s pdcurrent: %s" % (id,data['target'],data['zencurrent'],data['pdcurrent']))

    def updateEntries(self):
        '''
            perform updates on Zenoss and Pagerduty
        '''
        if self.verbose == True:
            self.writelog("updateEntries")
        updates = {"incidents": [], "requester_id": self.pduser}
        for id, data in self.statusDict.items():
            pd = data['pagerduty']
            zencurrent = data['zencurrent']
            pdcurrent = data['pdcurrent']
            target = data['target']
            pdnewer = data['pdnewer']

            if pdnewer == True: # pagerduty updated last, so update zenoss
                if zencurrent != target:
                    if self.verbose == True:
                        self.writelog("updating zenoss event id %s from %s to %s" % (id,zencurrent,target))
                    self.zenoss.manageEventStatus([id], self.statusData[target]['zenaction'])
            else: # zenoss updated last, so update pagerduty
                if pdcurrent != target and target != "open":
                    if self.verbose == True:
                        self.writelog("updating pagerduty incident id %s from %s to %s" % (id,pdcurrent,target))
                    pdData = {"id": pd['id'], "status":  self.statusData[target]["pagerduty"]} 
                    updates["incidents"].append(pdData)
        
        # perform pagerduty updates         
        if len(updates["incidents"]) > 0:
            self.pagerduty.updateStatus(updates)
            
    def updateZenossStatus(self):
        """
            update Zenoss event status if target is not current
        """
        if self.verbose == True:
            self.writelog("updateZenossStatus")
        for id, data in self.statusDict.items():
            current = data["zencurrent"]
            target = data["target"]
            if current != target:
                if self.verbose == True:
                    self.writelog("CHANGING STATUS %s IN ZENOSS TO %s" % (id,target))
                self.zenoss.manageEventStatus([id], self.statusData[target]['zenaction'])
            
    def updateZenossMessages(self):
        """
            update Zenoss event console with messages if they don't exist
        """
        if self.verbose == True:
            self.writelog("updateZenossMessages")
        for id, data in self.statusDict.items():
            eventlog = data['zenoss']['log']
            # get list of current event log messages
            zlogs = []
            for e in eventlog:
                zlogs.append(e[-1])
            pdlogs = data['messages']
            for msg in pdlogs:
                if msg not in zlogs:
                    if self.verbose == True:
                        self.writelog("adding message for %s: %s" % (id,msg))
                    self.zenoss.addEventMessage(id,msg)
    
if __name__ == "__main__":
    u = Main()
    u.run()    
