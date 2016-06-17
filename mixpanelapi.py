import base64
import urllib  # for url encoding
import urllib2  # for sending requests
from itertools import chain
import cStringIO

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
    VERSION = '2.0'

    def __init__(self, api_secret, timeout=120):
        self.api_secret = api_secret
        self.timeout = timeout

    def request(self, base_url, methods, params):
        data = None
        request_url = '/'.join([base_url, str(self.VERSION)] + methods) + '/?' + self.unicode_urlencode(params)
        # print request_url
        headers = {'Authorization': 'Basic {encoded_secret}'.format(encoded_secret=base64.b64encode(self.api_secret))}
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

    def export_people(self, params, output_file, format='json'):
        profiles = self.query_engage(params)
        self._export_data(profiles, output_file, format)

    def export_events(self, params, output_file, format='json'):
        events = self.query_export(params)
        self._export_data(events, output_file, format)

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
