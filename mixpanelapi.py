import base64
from time import strftime
import urllib  # for url encoding
import urllib2  # for sending requests
from itertools import chain
import cStringIO
import eventlet

try:
    import fastcsv as csv
except ImportError:
    import csv

try:
    import ujson as json
except ImportError:
    try:
        import json
    except ImportError:
        import simplejson as json


class Mixpanel(object):
    API_URL = 'https://mixpanel.com/api'
    DATA_URL = 'https://data.mixpanel.com/api'
    IMPORT_URL = 'https://api.mixpanel.com'
    VERSION = '2.0'

    def __init__(self, api_secret, token=None, timeout=120, pool_size=10):
        self.api_secret = api_secret
        self.token = token
        self.timeout = timeout
        self.pool_size = pool_size

    def request(self, base_url, methods, params):
        if base_url == self.IMPORT_URL:
            data = self.unicode_urlencode(params)
            request_url = '/'.join([base_url] + methods) + '/'
        else:
            data = None
            request_url = '/'.join([base_url, str(self.VERSION)] + methods) + '/?' + self.unicode_urlencode(params)
        print request_url
        headers= {'Authorization': 'Basic {encoded_secret}'.format(encoded_secret=base64.b64encode(self.api_secret))}
        request = urllib2.Request(request_url, data, headers)
        response = urllib2.urlopen(request, timeout=self.timeout)
        return response.read()

    @staticmethod
    def unicode_urlencode(params):
        if isinstance(params, dict):
            params = params.items()
        for i, param in enumerate(params):
            if isinstance(param[1], list):
                params[i] = (param[0], json.dumps(param[1]),)

        result = urllib.urlencode([(k, isinstance(v, unicode) and v.encode('utf-8') or v) for k, v in params])
        return result

    def query_engage(self, params):
        response = self.request(self.API_URL, ['engage'], params)
        first_page = json.loads(response)
        try:
            profiles = first_page['results']
            if first_page['total'] > first_page['page_size']:
                self._paginator(first_page['session_id'], first_page['page'], first_page['total'], profiles)
            else:
                pass
            return profiles
        except KeyError:
            print "Invalid response from /engage: " + response

    def query_export(self, params):
        response = self.request(self.DATA_URL, ['export'], params)
        file_like_object = cStringIO.StringIO(response)
        raw_data = file_like_object.getvalue().split('\n')
        raw_data.pop()
        events = []
        for line in raw_data:
            events.append(json.loads(line))
        return events

    def _paginator(self, session_id, page, total, profiles):
        next_page = json.loads(self.request(self.API_URL, ['engage'], {'session_id': session_id, 'page': page + 1}))
        profiles.extend(next_page['results'])
        if len(profiles) < total:
            self._paginator(session_id, next_page['page'], total, profiles)
        else:
            return

    def _export_data(self, data, output_file, format='json'):
        with open(output_file, 'w') as output:
            if format == 'json':
                json.dump(data, output)
            elif format == 'csv':
                self.write_to_csv(data, output)
            else:
                print "Invalid format - must be 'json' or 'csv': format = " + str(format)
                print "Dumping json to " + output_file
                json.dump(data, output)

    def properties_from_csv_row(self, row, header, ignore_columns):
        props = {}
        for h, prop in enumerate(header):
            # handle a strange edge case where the length of the row is longer than the length of the header.  We do this to prevent an out of range error.
            x = h
            if x > len(row) - 1:
                x = len(row) - 1
            if row[x] == '' or prop in ignore_columns:
                continue
            else:
                props[prop] = row[x]
        return props

    def _filename_to_list(self, filename):
        item_list = []
        try:
            with open(filename, 'rbU') as item_file:
                item_list = json.load(item_file)
        except ValueError:
            with open(filename, 'rbU') as item_file:
                reader = csv.reader(item_file)
                header = reader.next()
                if 'event' in header:
                    event_name_index = header.index("event")
                    distinct_id_index = header.index("distinct_id")
                    time_index = header.index("time")
                    for row in reader:
                        props = {'token': self.token, 'distinct_id': row[distinct_id_index],
                                                'time': row[time_index],
                                                'ip': 0}
                        props.update(self.properties_from_csv_row(row, header, ['event', 'distinct_id', 'time']))
                        event = {'event': row[event_name_index], 'properties': props}
                        item_list.append(event)
                elif '$distinct_id' in header:
                    distinct_id_index = header.index("$distinct_id")
                    for row in reader:
                        props = self.properties_from_csv_row(row, header, ['$distinct_id'])
                        profile = {'$distinct_id': row[distinct_id_index], '$properties': props}
                        item_list.append(profile)
        except IOError:
            print "Error loading data from file: " + filename

        return item_list

    def _send_batch(self, endpoint, batch):
        payload = {"data": base64.b64encode(json.dumps(batch)), "verbose": 1}
        message = self.request(self.IMPORT_URL, [endpoint], payload)
        print "Sent " + str(len(batch)) + " items on " + strftime("%Y-%m-%d %H:%M:%S") + "!"
        print message
        if json.loads(message)['status'] != 1:
            raise RuntimeError('import failed')

    def export_people(self, output_file, params={}, format='json'):
        profiles = self.query_engage(params)
        self._export_data(profiles, output_file, format)

    def export_events(self, output_file, params, format='json'):
        events = self.query_export(params)
        self._export_data(events, output_file, format)

    def import_events(self, data, timezone_offset=0):
        event_list = []
        if isinstance(data, basestring):
            event_list = self._filename_to_list(data)
        elif isinstance(data, list):
            event_list = data
        else:
            print "data parameter must be a filename or a list of events"

        pool = eventlet.GreenPool(size=self.pool_size)  # increase the size if you have more RAM available
        batch = []
        for event in event_list:
            assert ("time" in event['properties']), "Must specify a backdated time"
            assert ("distinct_id" in event['properties']), "Must specify a distinct ID"
            event['properties']['time'] = str(
                int(event['properties']['time']) - (timezone_offset * 3600))  # transforms timestamp to UTC
            if "token" not in event['properties']:
                assert self.token, "Events must contain a token or one must be supplied on initialization"
                event['properties']["token"] = self.token
            batch.append(event)
            if len(batch) == 50:
                pool.spawn(self._send_batch, 'import', batch)
                batch = []
        if len(batch):
            self._send_batch('import', batch)
            print str(batch) + "\n" + "Sent remaining %d events!" % len(batch)
        pool.waitall()

    def import_people(self, data):
        profile_list = []
        if isinstance(data, basestring):
            profile_list = self._filename_to_list(data)
        elif isinstance(data, list):
            profile_list = data
        else:
            print "data parameter must be a filename or a list of events"

        pool = eventlet.GreenPool(size=self.pool_size)  # increase the size if you have more RAM available
        batch = []

        for profile in profile_list:
            params = {
                '$ignore_time': 'true',
                '$ip': 0,
                'token': self.token,
                '$distinct_id': profile['$distinct_id'],
                '$set': profile['$properties']
            }
            batch.append(params)
            if len(batch) == 50:
                pool.spawn(self._send_batch, 'engage', batch)
                batch = []
        if len(batch):
            self._send_batch('engage', batch)
            print str(batch) + "\n" + "Sent remaining %d updates!" % len(batch)
        pool.waitall()

    @staticmethod
    def write_to_csv(items, output_file):
        if '$distinct_id' in items[0]:
            props_key = '$properties'
            initial_header_value = '$distinct_id'
        else:
            props_key = 'properties'
            initial_header_value = 'event'

        subkeys = set()
        # returns a list of lists of property names from each item
        columns = [item[props_key].keys() for item in items]
        # flattens to a list of property names
        columns = list(chain.from_iterable(columns))
        subkeys.update(columns)

        # Create the header
        header = [initial_header_value]
        for key in subkeys:
            header.append(key.encode('utf-8'))

        # Create the writer and write the header
        writer = csv.writer(output_file)
        writer.writerow(header)

        for item in items:
            row = []
            try:
                row.append(item[initial_header_value])
            except KeyError:
                row.append('')

            for subkey in subkeys:
                try:
                    row.append((item[props_key][subkey]).encode('utf-8'))
                except AttributeError:
                    row.append(item[props_key][subkey])
                except KeyError:
                    row.append("")
            writer.writerow(row)
