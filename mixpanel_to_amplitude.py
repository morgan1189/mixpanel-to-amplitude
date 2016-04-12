import hashlib
import urllib
import urllib2
import time
import requests
import threading
import datetime
from time import sleep
import copy

try:
    import json
except ImportError:
    import simplejson as json

# The keys below are required for the script to work
MIXPANEL_API_KEY = ''
MIXPANEL_API_SECRET = ''
AMPLITUDE_API_KEY = ''

# No need to change this if your project is using UTC
MIXPANEL_TIME_OFFSET = 0 # Set this to your project offset in hours relative to UTC

FROM_DATE = '2016-03-01' # required (could just be early enough)
TO_DATE = '2016-03-02' # optional

MIXPANEL_USER_ID_KEY = 'identity_id' # use this id to specify what to use as your user_id for Amplitude

# Technical stuff (can leave as is)

# Set to zero or delete if you don't want to use threads
THREADS_NUMBER = 8 # number of threads to parse Mixpanel
CHUNK_SIZE = 2000 # size of the single event batch sent to Amplitude

mixpanel_people_profiles = {}

class Mixpanel(object):
    ENDPOINT = 'https://mixpanel.com/api'
    DATA_ENDPOINT = 'https://data.mixpanel.com/api'

    VERSION = '2.0'

    def __init__(self, api_key, api_secret, data = False):
        self.api_key = api_key
        self.api_secret = api_secret
        self.endpoint = self.ENDPOINT
        if data:
            self.endpoint = self.DATA_ENDPOINT

    def request(self, methods, params, http_method='GET', read_byte_size = 1024000, format='json'):
        """
            methods - List of methods to be joined, e.g. ['events', 'properties', 'values']
                      will give us http://mixpanel.com/api/2.0/events/properties/values/
            params - Extra parameters associated with method
        """
        params['api_key'] = self.api_key
        params['expire'] = int(time.time()) + 1200   # Grant this request 20 minutes.
        # params['format'] = format
        if 'sig' in params: del params['sig']
        params['sig'] = self.hash_args(params)

        request_url = '/'.join([self.endpoint, str(self.VERSION)] + methods)
        if http_method == 'GET':
            data = None
            request_url = request_url + '/?' + self.unicode_urlencode(params)
        else:
            data = self.unicode_urlencode(params)

        request = urllib2.Request(request_url, data)
        if (self.endpoint == self.ENDPOINT):
            response = urllib2.urlopen(request, timeout = 120)
            return json.loads(response.read())
        else:
            response = urllib2.urlopen(request, timeout = 1200)

            # We now need to read the whole thing according to Mixpanel docs
            overall_data = ''
            while True:
                data = response.read(read_byte_size)
                if len(data) == 0:
                    break
                overall_data += data
            return overall_data

    def unicode_urlencode(self, params):
        """
            Convert lists to JSON encoded strings, and correctly handle any
            unicode URL parameters.
        """
        if isinstance(params, dict):
            params = params.items()
        for i, param in enumerate(params):
            if isinstance(param[1], list):
                params[i] = (param[0], json.dumps(param[1]),)

        return urllib.urlencode(
            [(k, isinstance(v, unicode) and v.encode('utf-8') or v) for k, v in params]
        )

    def hash_args(self, args, secret=None):
        """
            Hashes arguments by joining key=value pairs, appending a secret, and
            then taking the MD5 hex digest.
        """
        for a in args:
            if isinstance(args[a], list): args[a] = json.dumps(args[a])

        args_joined = ''
        for a in sorted(args.keys()):
            if isinstance(a, unicode):
                args_joined += a.encode('utf-8')
            else:
                args_joined += str(a)

            args_joined += '='

            if isinstance(args[a], unicode):
                args_joined += args[a].encode('utf-8')
            else:
                args_joined += str(args[a])

        hash = hashlib.md5(args_joined)

        if secret:
            hash.update(secret)
        elif self.api_secret:
            hash.update(self.api_secret)
        return hash.hexdigest()

class Amplitude(object):

    def __init__(self, api_key, batch_size = CHUNK_SIZE/5):
        self.api_key = api_key
        self.events_not_sent = []
        self.slice_size = batch_size

    @classmethod
    def extractUserAndDeviceIdFromEvent(cls, event):
        user_id = event['properties'].get(MIXPANEL_USER_ID_KEY)
        device_id = event['properties'].get('$ios_ifa') or event['properties'].get('$android_devices')
        if (user_id is None and device_id is None):
            # Can not identify this user, no need to send to Amplitude
            return None
        else:
            return (user_id, device_id)

    @classmethod
    def getPeopleProfile(cls, event):
        global mixpanel_people_profiles
        people_mixpanel = Mixpanel(MIXPANEL_API_KEY, MIXPANEL_API_SECRET)
        distinct_id = event['properties'].get('distinct_id')
        if (distinct_id is not None):
            user_profile = mixpanel_people_profiles.get(distinct_id)
            if (user_profile is not None): return user_profile

            where_string = ''
            if event['properties'].get('$ios_ifa') is not None:
                where_string += 'properties["$ios_ifa"] == ' + '\"' + str(event['properties'].get('$ios_ifa')) + '\"'
            elif event['properties'].get('$android_devices') is not None:
                where_string += 'properties["$android_devices"] == ' + '\"' + str(event['properties'].get('$android_devices')) + '\"'

            request_results = people_mixpanel.request(
                ['engage'], {
                    'where' : where_string
                }
            )

            for result in request_results['results']:
                if result['$distinct_id'] == distinct_id:
                    # Found our guy
                    user_profile = result['$properties']
                    mixpanel_people_profiles[distinct_id] = copy.deepcopy(user_profile)
                    cls.sendRevenueEvents(user_profile)
                    return user_profile

        # Could not find a corresponding profile
        return None

    @staticmethod
    def convertISODatetimeToMSTimestamp(iso_string):
        time_datetime = datetime.datetime.strptime(iso_string, '%Y-%m-%dT%H:%M:%S')
        timestamp = (time_datetime - datetime.datetime(1970, 1, 1)).total_seconds()
        return (timestamp - MIXPANEL_TIME_OFFSET * 3600) * 1000

    @classmethod
    def sendRevenueEvents(cls, user_profile):
        if user_profile.get('$transactions') is not None:
            transactions = user_profile['$transactions']
            for transaction in transactions:
                amount = transaction.get('$amount')
                time_string = transaction.get('$time')
                timestamp = cls.convertISODatetimeToMSTimestamp(time_string)

                # Basically just take those transactions and send them as separate revenue events
                # Not implemented yet
                pass

    @classmethod
    def makeEventFromMixpanelEvent(cls, event):
        amplitude_event = {}
        # Setting event name
        amplitude_event['event_type'] = event['event']

        user_device_id_tuple = cls.extractUserAndDeviceIdFromEvent(event)
        if (user_device_id_tuple is None):
            return None
        else:
            user_id, device_id = user_device_id_tuple
            if (user_id is not None): amplitude_event['user_id'] = user_id
            if (device_id is not None): amplitude_event['device_id'] = device_id

        for field in event['properties']:
            key_value = cls.convertFieldFromMixpanelToAmplitude(event, field)
            if (key_value is not None):
                # Setting reserved Amplitude properties from Mixpanel
                keys = key_value[0]
                value = key_value[1]
                if type(keys) is list:
                    for key in keys:
                        amplitude_event[key] = value
                else:
                    amplitude_event[keys] = value
            else:
                if (cls.isPeopleProperty(field)):
                    if ('user_properties' not in amplitude_event):
                        amplitude_event['user_properties'] = {}
                    amplitude_event['user_properties'][str(field)] = event['properties'][field]

                elif (cls.isEventProperty(field)):
                    if ('event_properties' not in amplitude_event):
                        amplitude_event['event_properties'] = {}
                    amplitude_event['event_properties'][str(field)] = event['properties'][field]

        # Adding Mixpanel People properties to this event
        user_profile = cls.getPeopleProfile(event)
        if user_profile is not None:
            for field in user_profile:
                if field == MIXPANEL_USER_ID_KEY:
                    amplitude_event['user_id'] = user_profile[MIXPANEL_USER_ID_KEY]
                elif ((('user_properties' in amplitude_event and field not in amplitude_event['user_properties']) or
                        ('user_properties' not in amplitude_event)) and
                        cls.isEventProperty(field)):
                    if 'user_properties' not in amplitude_event:
                        amplitude_event['user_properties'] = {}
                    amplitude_event['user_properties'][field] = user_profile[field]
        return amplitude_event

    @classmethod
    def convertFieldFromMixpanelToAmplitude(cls, event, field):
        key_value = None

        # Device and system info
        if field == '$manufacturer':        key_value = ['device_manufacturer', 'device_brand'],
        elif field == '$model':             key_value = 'device_model',
        elif field == '$os':                key_value = ['os_name', 'platform'],
        elif field == '$os_version':        key_value = 'os_version',
        elif field == '$carrier':           key_value = 'carrier',

        # Geo info
        elif field == 'mp_country_code':    key_value = 'country',
        elif field == '$region':            key_value = ['region', 'dma'],
        elif field == '$city':              key_value = 'city',

        # Platform specific device IDs
        elif field == '$ios_ifa':           key_value = 'idfa',
        elif field == '$android_id':        key_value = 'adid',

        # Timestamp
        elif field == 'time':
            time_value = event['properties'][field]
            time_length = len(str(time_value))
            time_value = (int(time_value) - MIXPANEL_TIME_OFFSET * 3600) * (10 ** (13 - time_length)) # converting to milliseconds
            key_value = ('time', time_value)

        # Revenue
        # elif field == 'product_price_usd':  key_value = 'revenue',

        elif field == '$app_release':       key_value = 'app_version',

        if key_value is not None and len(key_value) == 1:
            # Only key is filled out, filling with default value
            key_value = (key_value[0], event['properties'][field])
        return key_value

    @classmethod
    def isPeopleProperty(cls, field):
        if (field == 'gender' or field == 'age' or field == '$last_seen' or ('name' in field)):
            return True
        return False

    @classmethod
    def isEventProperty(cls, field):
        if (field not in ['$app_build_number', '$app_version', '$app_version_string', '$lib_version', '$radio']):
            return True
        return False

    def uploadEventsToAmplitude(self, events):
        event_subarrays = [events[i:i+self.slice_size] for i in xrange(0, len(events), self.slice_size)]
        for event_array in event_subarrays:
            r = requests.post('https://api.amplitude.com/httpapi', {'api_key':self.api_key, 'event':json.dumps(event_array)})
            if (r.status_code != 200):
                self.events_not_sent.extend(event_array)
            print (r.status_code, r.reason, len(event_array))

def sliceDateInterval(from_date, to_date, N):
    d = (to_date - from_date) / N
    prev_date = from_date
    dates_ranges = []
    for i in range(N):
        range_from_date = prev_date
        if (range_from_date > to_date):
            break
        range_to_date = prev_date + d
        if (range_to_date >= to_date):
            range_to_date = to_date
        prev_date = range_to_date + datetime.timedelta(days = 1)
        dates_ranges.append((range_from_date.strftime('%Y-%m-%d'), range_to_date.strftime('%Y-%m-%d')))
    return dates_ranges

def exportFromMixpanelToAmplitude(from_to_dates_tuple):
    from_date, to_date = from_to_dates_tuple

    mixpanel = Mixpanel(MIXPANEL_API_KEY, MIXPANEL_API_SECRET, data = True)
    event_data = mixpanel.request(['export'], {
        'from_date': from_date,
        'to_date': to_date
    })

    amplitude = Amplitude(AMPLITUDE_API_KEY)
    events_to_send = []
    for event_line in event_data.splitlines():
        try:
            mixpanel_event = json.loads(event_line)
            amplitude_event = Amplitude.makeEventFromMixpanelEvent(mixpanel_event)
            if amplitude_event is not None:
                events_to_send.append(amplitude_event)
            if len(events_to_send) >= CHUNK_SIZE:
                amplitude.uploadEventsToAmplitude(events_to_send)
                events_to_send = []
        except Exception as e:
            print e
    print amplitude.events_not_sent

try:
    MULTITHREADING_ENABLED = (THREADS_NUMBER > 0)
except NameError:
    MULTITHREADING_ENABLED = False

try:
    end = TO_DATE
except NameError:
    end = datetime.datetime.now().strftime('%Y-%m-%d')
start = FROM_DATE

if (MULTITHREADING_ENABLED):
    daterange_list = sliceDateInterval(datetime.datetime.strptime(start, '%Y-%m-%d'), datetime.datetime.strptime(end, '%Y-%m-%d'), THREADS_NUMBER)
    real_jobs_num = len(daterange_list)

    threads = []
    for i in range(real_jobs_num):
        t = threading.Thread(target=exportFromMixpanelToAmplitude, args=(daterange_list[i],))
        threads.append(t)
        t.start()
        sleep(5)
else:
    exportFromMixpanelToAmplitude((start, end))