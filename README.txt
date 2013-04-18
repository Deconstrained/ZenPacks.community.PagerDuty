AUTHOR: 

Joseph Anderson

DESCRIPTION:

This ZenPack is designed to facilitate a tight integration between Zenoss (www.zenoss.com) and Pagerduty (www.pagerduty.com). It 
provides the following capabilities:

	1.  A script designed to be run by Zenoss "Event Commands" (3.x) or "Notifications" (4.x) that creates Pagerduty incidents for specific Zenoss events.
		a.  incident is created for Pagerduty service by specifying the service key as a runtime argument.
		b.  if Pagerduty service is in maintenance, the Zenoss event will be acknowledged and updated with a relevant message
		c.  if Pagerduty service is disabled, the Zenoss event will be left unacknowledged, but updated with a relevant message
	2.  A Zenoss daemon that runs periodically and synchronizes Zenoss event/Pagerduty incidents.  The daemon:
		a.  determines which side (Zenoss or Pagerduty) was most recently updated.
		b.  changes the status of the non-authoritative event/incident to match the authoritative one
		c.  copies/formats Pagerduty incident logs for view within the Zenoss event console details.

This ZenPack uses the web service APIs of both Zenoss and PagerDuty.

CREATING PAGERDUTY INCIDENTS:

Since Pagerduty uses a key to identify the "Services" that notifications should be assigned to, the idea is to create a default service key event attribute (via transform)
that Zenoss will use when creating Pagerduty Incidents.  The default service key can then be overwritten by other event transforms according to the administrator's needs. 

For example, the following event transform might be applied at the root of the event class hierarchy.  
This transform determines whether a given device is a Windows or Unix device, and assigns the appropriate service key accordingly:

####################
unixServiceKey = 'UNIXKEY' # Unix Team
windowsServiceKey = 'WINDOWSKEY' # Windows Team
defaultServiceKey = 'DEFAULTKEY' # Default Team
try:
	devClass = device.deviceClass().getOrganizerName() # string representing device class organizer
	if 'Linux' in devClass:
		evt.pdServiceKey = unixServiceKey
	elif 'AIX' in devClass:
		evt.pdServiceKey = unixServiceKey
	elif 'WMI' in devClass:
		evt.pdServiceKey = windowsServiceKey
	elif 'Windows' in devClass:
		evt.pdServiceKey = windowsServiceKey
	else:
		evt.pdServiceKey = device.zPDServiceKey
except: # set to default if nothing found
	evt.pdServiceKey = defaultServiceKey
####################
This initial transform can then be overridden later by other event transforms depending on the event class (or whatever the administrator designs).
Once the event has a corresponsing service key assigned, it can be passed as a parameter to an "Event Command" (Zenoss 3.x) or "Notification" (Zenoss 4.x) such as:


python zenpagerduty.py -a create -z ${dev/zPDZenossServer} -u ${dev/zPDZenossUser} -p ${dev/zPDZenossPass} -H ${dev/zPDDomain} -T ${dev/zPDToken} -U ${dev/zPDUser} -e ${evt/evid} -S ${evt/pdServiceKey}

which creates the Pagerduty Incident with the provided arguments.

SYNCHRONIZING SERVICE:

This ZenPack provides a service daemon called "zenpdsync" which periodically (default 60 seconds) pulls the last N ('eventsBuffer' option default 20) events from both Zenoss and Pagerduty.  It
correlates these into pairs and determines which was last updated.  If the status of one of the pair differs from the other, then the most recently updated one's status is
copied to the other.  Relevant Pagerduty incident log details are also copied to the Zenoss console.

ZPROPERTIES PROVIDED:

	zPDZenossServer:  hostname of zenoss server
	zPDZenossUser:  zenoss user allowed to query events
	zPDZenossPass:  password for zenoss user
	zPDDomain:  YOURNAMEHERE.pagerduty.com
	zPDToken:  Token key needed for API calls
	zPDUser:  Pagerduty user used for automatic updates (this will show in the console, I use a fake user called "Zenoss")
	zPDServiceKey:  optional per-device service key (would need to be assigned in transform if used, however)
	
COMPONENTS:

The ZenPack has the following objects:
	
	An example notification (Zenoss 4.x)
	An example event command (Zenoss 3.x)
	
INSTALLATION:

It is recommended to run the "zenpdsync" from only one hub or collector, since the process does not need to be run multiple times for a single Zenoss installation.
This means disabling the "zenpdsync" daemon on all but one of the hub/collectors.

Be sure also to set defaults for the zProperties, as well as creating an event transform under the root class similar to the sample above.  The bare minimum event 
transform would be:

try:
	evt.pdServiceKey = device.zPDServiceKey
except:
	evt.pdServiceKey = 'YOURSERVICEKEY'

A transform has not been provided, as the author has encountered complications in the past related to event classes (they get removed if the Zenpack is uninstalled).

REQUIREMENTS:

    Zenoss Versions Supported: 3.x, 4.x
    External Dependencies: None
    ZenPack Dependencies:
    Installation Notes: zenoss restart after installing thisZenPack.

HISTORY:

Change History:

    1.0 initial release

Tested
======
This ZenPack was tested with versions 3.2.1, 4.2.3

Source:
https://github.com/j053ph4/ZenPacks.community.PagerDuty

Known issues:  
