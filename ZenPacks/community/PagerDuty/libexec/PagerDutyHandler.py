from HTTPHandler import *
import urllib

import logging
log = logging.getLogger('zen.zenpdsync')


class PagerDutyHandler():
    """
        Python wrapper for PagerDuty WWW API
    """
    def __init__(self, host, token, verbose=False):
        """
        """
        self.verbose = verbose
        self.host = host
        self.token = token
        self.incidentEndpoint = "https://events.pagerduty.com/generic/2010-04-15/create_event.json"
        self.weburl = "https://%s" % self.host
        self.baseurl = "https://%s/api/v1" % self.host
        self.buffersize = 50
        self.http = HTTPHandler()
        self.http.verbose = verbose
        self.http.headers['Content-type'] = 'application/json'
    
    def request(self, endpoint, data=None, token=True, method="GET"): # data should be list of dicts
        """
            Handle HTTP GET / POST operations against PagerDuty API
        """
        if data and method=="GETARG": endpoint += '?%s' % urllib.urlencode(data)
        if token == True: self.http.headers['Authorization'] = 'Token token=%s' % self.token
        self.http.connect(endpoint)
        if data:
            if method == "POST":  self.http.post(data)
            if method == "PUT": self.http.put(data)
            if method == "GET": self.http.session.add_data(json.dumps(data))    
        self.http.submit()
        return self.http.response
        
    def manageIncident(self, data, action="trigger"): 
        """
            action = [trigger|acknowledge|resolve] 
            Manage PagerDuty Incidents
        """
        if self.verbose is True: print "Managing %s Incident with %s" % (action, data)
        data["event_type"] = action
        output = self.request(self.incidentEndpoint,data,token=False,method="POST")
        return output 
    
    def updateStatus(self, data): 
        """
            action = [trigger|acknowledge|resolved] 
            Manage PagerDuty Incidents
        """
        if self.verbose is True: print "Updating status with %s" % (data)
        return self.request("%s/incidents" % self.baseurl, data, token=True, method="PUT")
    
    def getServiceList(self):
        """
            Retrive list of services
        """
        if self.verbose is True: print "Getting list of services"
        return self.request("%s/services" % self.baseurl)
    
    def getServiceDetail(self,id):
        """
            Retrive details for service
        """
        if self.verbose is True: print "Getting service details for %s" % id
        return self.request("%s/services/%s" % (self.baseurl,id))
    
    def getServiceLog(self,id):
        """
            Retrive logs for service
        """
        if self.verbose is True: print "Getting service log for %s" % id
        return self.request("%s/services/%s/log" % (self.baseurl, id))
    
    def getIncidentList(self, open=False):
        """
            Retrive list of all incidents
        """
        if self.verbose is True: print "Getting list of ALL incidents"
        data = {
                "sort_by":"created_on:desc",
                'limit': self.buffersize
                }
        if open is True:  data['status'] = 'triggered,acknowledged'
        return self.request("%s/incidents" % self.baseurl, data, token=True, method="GETARG")
    
    def getIncidentByKey(self, key):
        """
            Retrieve incident by its incident key
        """
        if self.verbose is True: print "Getting incident details for key: %s" % key
        data = {"incident_key":key}
        return self.request("%s/incidents" % self.baseurl, data, token=True, method="GETARG")
    
    def getIncidentDetail(self, id):
        """
            return incident details given the ID
        """
        if self.verbose is True: print "Getting incident details for id: %s" % id 
        return self.request("%s/incidents/%s/log_entries" % (self.baseurl, id))
    
    def getIncidentLog(self, id):
        """
            return incident log given the ID
        """
        if self.verbose is True: print "Getting incident details for log: %s" % id 
        return self.request("%s/incidents/%s/log" % (self.baseurl, id))
    
    def getMaintenanceWindows(self):
        """
            return maintenance window information
        """
        if self.verbose is True: print "Getting list of maintenance windows"
        #data = {"type":"ongoing"}
        return self.request("%s/maintenance_windows" % self.baseurl)
    
    def findService(self, serviceKey):
        """
            find service info for given key
        """
        if self.verbose is True: print "Finding service for key: %s" % serviceKey
        services = self.getServiceList()
        for s in services["services"] :
            if s["service_key"] == serviceKey: return s
        return None
        
    def getServiceStatus(self, data):
        ''' Return the servcie status'''
        if self.verbose is True: print "Getting service status for %s" % data
        return data["status"]
        
    def inMaintenance(self, data):
        ''' Return True if service is in maintenance'''
        if self.verbose is True: print "Getting maintenance status for %s" % data
        if self.getServiceStatus(data) == 'maintenance': return True
        return False
    
    def isDisabled(self, data):
        ''' Return True if service is disabled'''
        if self.verbose is True: print "Getting disabled status for %s" % data
        if self.getServiceStatus(data) == 'disabled': return True
        return False
    
    def isActive(self, data):
        ''' Return True if service is active'''
        if self.verbose is True: print "Getting active status for %s" % data
        if self.getServiceStatus(data) == 'active': return True
        return False

