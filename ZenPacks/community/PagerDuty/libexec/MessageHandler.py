import datetime,time,re

class MessageHandler():
    """
        Standardize messages to pass between systems
    """
    def __init__(self):
        self.messages = {}

    def newId(self,id):
        if id not in self.messages.keys(): self.messages[id] = []
    
    def addMessage(self, id, msg):
        ''''''
        self.newId(id)
        msg = str(msg)
        if msg not in self.messages[id]:  
            self.messages[id].append(msg)
    
    def serviceIncidentCreated(self, id, service, baseurl, error=None):
        """
            message for initial service notification
        """
        url = '<a href="%s%s">%s</a>' % (baseurl, service["service_url"], service["name"])
        if error is None: self.addMessage(id, 'Incident created for %s' % url)
        else: self.addMessage(id, 'Failed to create incident for %s with error: "%s"' % (url, error))

    def incidentCreated(self, id, incident):
        """
            message for new PagerDuty Incident
        """
        incidenturl = '<a href="%s">%s</a>' % (incident["html_url"], incident["id"])
        serviceurl = '<a href="%s">%s</a>' % (incident["service"]["html_url"], incident["service"]["name"])
        self.addMessage(id, "Incident %s created for %s" % (incidenturl, serviceurl))
        
    def incidentAssigned(self, id, incident):
        """
            message for new PagerDuty Incident
        """
        incidenturl = '<a href="%s">%s</a>' % (incident["html_url"], incident["id"])
        userurl = '<a href="%s">%s</a>' % (incident["assigned_to_user"]["html_url"], incident["assigned_to_user"]["name"])
        self.addMessage(id, "Incident %s assigned to %s" % (incidentur, userurl))
    
    def incidentStatusChange(self, id, incident):
        """
            message for PagerDuty Incident status changes
        """
        updated = self.getPagerDutyTime(incident["created_on"])
        incidenturl = '<a href="%s">%s</a>' % (incident["html_url"], incident["id"])
        userurl = '<a href="%s">%s</a>' % (incident["last_status_change_by"]["html_url"], incident["last_status_change_by"]["name"])
        message = "Incident %s %s by %s at %s" % (incidenturl, incident["status"], userurl, updated)
        self.addMessage(id, message)

    def incidentLogs(self, id, logs, baseurl):
        """
            reformat incident log data
        """
        for d in logs:
            # time the log message was added
            created = self.getPagerDutyTime(d["created_at"])
            # type of log message
            type = str(d["type"])
            # url to the PD user
            try: agenturl = '<a href="%s%s">%s</a>' % (baseurl, d["agent"]["user_url"], d["agent"]["name"])
            except: agenturl = None
            # method by which log was added
            try: updatemethod = d["channel"]["type"]
            except: updatemethod = None
            # formatted log message
            msg = None
            #try:
            if type == "annotate":
                msg = '"%s" - %s at %s' % (d['channel']['summary'], agenturl, created)
            # notification messages
            if type == "notify":
                notif = d["notification"]
                msg = 'Notification to %s via %s was a %s at %s' % (notif["address"], notif["type"], notif["status"], created)
            # incident acknowledgment
            elif type == "acknowledge":
                msg = "Acknowledged by %s via %s at %s" % (agenturl, updatemethod, created)
            # incident unacknowledgment
            elif type == "unacknowledge":
                msg = "Unacknowledged due to %s at %s" % (updatemethod, created)
            elif type == "resolve":
                msg = "Resolved by %s via %s at %s" % (agenturl, updatemethod, created)
            elif type == "assign":
                url = "<a href=\"%s%s\">%s</a>" % (baseurl,d["assigned_user"]["user_url"],d["assigned_user"]["name"])
                msg = 'Assigned to %s at %s' % (url, created)
            else: 
                print "no message created for %s :%s" % (type, d)
                msg = None
            if msg is not None:  self.addMessage(id, msg)
    
    def serviceInMaintenance(self, id, action, svc, mw, baseurl):
        """
            message with PagerDuty Maintenance Window details
        """
        serviceurl = '<a href="%s%s">%s</a>' % (baseurl, svc["service_url"], svc["name"])
        windowurl = '<a href="%s/maintenance_windows#/show/%s">%s</a>' % (baseurl,mw["id"], mw["description"])
        starting = self.getLocalTime(mw["start_time"])
        ending = self.getLocalTime(mw["end_time"])
        msg = "%s due to %s maintenance window %s starting: %s ending: %s" % (action, serviceurl, windowurl, starting, ending)
        self.addMessage(id, msg)

    def serviceIsDisabled(self, id, action, service, baseurl):
        """
            message for PagerDuty Service disabled
        """
        serviceurl = '<a href="%s%s">%s</a>' % (baseurl, service["service_url"], service["name"])
        msg = "%s because %s service is disabled" % (action, serviceurl)
        self.addMessage(id, msg)
    
    def serviceNotFound(self, id, key):
        """
        """
        self.addMessage(id, "PagerDuty Service not found with KEY: %s" % key)
    
    def getOffset(self):
        '''return True if in DST zone'''
        loc = time.localtime()
        if loc.tm_isdst == 0: return time.timezone
        else: return time.altzone
    
    def utcToLocal(self,dt):
        """
            convert local time to UTC
        """
        return dt - datetime.timedelta(seconds=self.getOffset())
    
    def localToUtc(self,dt):
        """
            convert local time to UTC
        """
        return dt + datetime.timedelta(seconds=self.getOffset())
    
    def getPagerDutyTime(self, ts):
        """
            return Pagerduty UTC time as local time
        """
        return self.utcToLocal(datetime.datetime.strptime(ts,'%Y-%m-%dT%H:%M:%SZ'))

    def getLocalTime(self,ts):
        sub = ts[:-6]
        return datetime.datetime.strptime(sub,'%Y-%m-%dT%H:%M:%S')

    def getZenossTime(self, ts):
        if '.' in ts:
            ts = ts[:-4]
            dt = datetime.datetime.strptime(ts,'%Y/%m/%d %H:%M:%S')
        else:
            dt = datetime.datetime.strptime(ts,'%Y-%m-%d %H:%M:%S')
        return dt
        
    def getTimestamp(self,dt):
        """
            return seconds given a datetime object
        """
        return time.mktime(dt.timetuple())

