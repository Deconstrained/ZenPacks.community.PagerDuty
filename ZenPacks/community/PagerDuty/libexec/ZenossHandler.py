from HTTPHandler import *

class ZenossHandler():
    """
    """
    def __init__(self, host, username, password, verbose=False):
        """
        """
        self.verbose = verbose
        self.routers = {
                        'MessagingRouter': 'messaging',
                        'EventsRouter': 'evconsole',
                        'ProcessRouter': 'process',
                        'ServiceRouter': 'service',
                        'DeviceRouter': 'device',
                        'NetworkRouter': 'network',
                        'TemplateRouter': 'template',
                        'DetailNavRouter': 'detailnav',
                        'ReportRouter': 'report',
                        'MibRouter': 'mib',
                        'ZenPackRouter': 'zenpack' 
                        }
        self.host = host
        self.username = username
        self.password = password
        self.baseurl = "http://%s:8080" % self.host
        self.login = {
                      "__ac_name": self.username,
                      "__ac_password": self.password,
                      "submitted": "true",
                      "came_from": "%s/zport/dmd" % self.baseurl
                      }
        self.buffersize = 50
        self.http = HTTPHandler()
        self.http.verbose = verbose
        self.http.persist("%s/zport/acl_users/cookieAuthHelper/login" % self.baseurl ,self.login)

    def request(self,router,method,data=[]):
        """
            Handle HTTP GET / POST operations against Zenoss API
        """
        self.http.headers['Content-type'] = 'application/json; charset=utf-8'
        self.http.connect("%s/zport/dmd/%s_router" % (self.baseurl,self.routers[router]))
        data = {
                "action": router,
                "method": method,
                "data": data,
                "type": 'rpc',
                "tid" : self.http.counter
                }
        self.http.transact(data)
        return self.http.response
        
    def getEvents(self, history=False, open=False):
        """
            retrieve current console events
        """
        if self.verbose is True: print "Getting list of console events with history: %s and open: %s" % (history, open)
        data = dict(start=0, limit=self.buffersize, dir='DESC', sort='lastTime')
        # get results from history
        if history is True: data['history'] = True
        # only get open events
        if open is True: data['params'] = dict(eventState=[0,1], severity=[5,4,3])
        else: data['params'] = dict(severity=[5,4,3,0])
        return self.request('EventsRouter','query',[data])["result"]
    
    def manageEventStatus(self, evids=[], action="acknowledge"):
        """
            manage event status
            action = [acknowledge|close]
        """
        if self.verbose is True: print "Changing event status to %s for evids: %s" % (action, ', '.join(evids))
        data = [{"evids": evids}]
        return self.request('EventsRouter',action,data)
    
    def addEventMessage(self, evid, message):
        """
            manage event open/close/acknowledge status
        """
        if self.verbose is True: print 'Adding message: "%s" to event: %s' % (message, evid)
        data = {
                'evid': evid,
                'message':message
                }
        return self.request('EventsRouter','write_log',[data])
      
    def getEventDetails(self, evid, history=False):
        """
            manage event open/close/acknowledge status
        """
        if self.verbose is True: print "Getting event details for %s with history: %s" % (evid, history)
        data = {
                'evid': evid,
                }
        if history == True:
            data['history'] = 'True'
        return self.request('EventsRouter','detail',[data])


