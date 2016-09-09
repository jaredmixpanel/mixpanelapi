import base64
import urllib  # for url encoding
import urllib2  # for sending requests
import cStringIO
import logging
from time import strftime
from itertools import chain
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool
from paginator import ConcurrentPaginator
from ast import literal_eval
from copy import deepcopy

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

    def __init__(self, api_secret, token=None, timeout=120, pool_size=None, max_retries=10, debug=False):
        self.api_secret = api_secret
        self.token = token
        self.timeout = timeout
        if pool_size is None:
            pool_size = cpu_count() * 2
        self.pool_size = pool_size
        self.max_retries = max_retries
        if debug:
            logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)
        else:
            logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.WARNING)

    @staticmethod
    def unicode_urlencode(params):
        if isinstance(params, dict):
            params = params.items()
        for i, param in enumerate(params):
            if isinstance(param[1], list):
                params[i] = (param[0], json.dumps(param[1]),)

        result = urllib.urlencode([(k, isinstance(v, unicode) and v.encode('utf-8') or v) for k, v in params])
        return result

    @staticmethod
    def response_handler_callback(response):
        if json.loads(response)['status'] != 1:
            logging.warning("Bad API response: " + response)
            raise RuntimeError('import failed')
        logging.debug("API Response: " + response)

    @staticmethod
    def write_items_to_csv(items, output_file):
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

    @staticmethod
    def properties_from_csv_row(row, header, ignored_columns):
        props = {}
        for h, prop in enumerate(header):
            # Handle a strange edge case where the length of the row is longer than the length of the header.
            # We do this to prevent an out of range error.
            x = h
            if x > len(row) - 1:
                x = len(row) - 1
            if row[x] == '' or prop in ignored_columns:
                continue
            else:
                try:
                    p = literal_eval(row[x])
                    props[prop] = p
                except (SyntaxError, ValueError) as e:
                    props[prop] = row[x]
        return props

    @staticmethod
    def event_object_from_csv_row(row, header, event_index=None, distinct_id_index=None, time_index=None):
        event_index = (header.index("event") if event_index is None else event_index)
        distinct_id_index = (header.index("distinct_id") if distinct_id_index is None else distinct_id_index)
        time_index = (header.index("time") if time_index is None else time_index)
        props = {'distinct_id': row[distinct_id_index], 'time': int(row[time_index])}
        props.update(Mixpanel.properties_from_csv_row(row, header, ['event', 'distinct_id', 'time']))
        event = {'event': row[event_index], 'properties': props}
        return event

    @staticmethod
    def people_object_from_csv_row(row, header, distinct_id_index=None):
        distinct_id_index = (header.index("$distinct_id") if distinct_id_index is None else distinct_id_index)
        props = Mixpanel.properties_from_csv_row(row, header, ['$distinct_id'])
        profile = {'$distinct_id': row[distinct_id_index], '$properties': props}
        return profile

    @staticmethod
    def list_from_items_filename(filename):
        item_list = []
        try:
            with open(filename, 'rbU') as item_file:
                item_list = json.load(item_file)
        except ValueError:
            with open(filename, 'rbU') as item_file:
                reader = csv.reader(item_file)
                header = reader.next()
                if 'event' in header:
                    event_index = header.index("event")
                    distinct_id_index = header.index("distinct_id")
                    time_index = header.index("time")
                    for row in reader:
                        event = Mixpanel.event_object_from_csv_row(row, header, event_index, distinct_id_index,
                                                                   time_index)
                        item_list.append(event)
                elif '$distinct_id' in header:
                    distinct_id_index = header.index("$distinct_id")
                    for row in reader:
                        profile = Mixpanel.people_object_from_csv_row(row, header, distinct_id_index)
                        item_list.append(profile)
        except IOError:
            logging.warning("Error loading data from file: " + filename)

        return item_list

    @staticmethod
    def _export_data(data, output_file, format='json'):
        with open(output_file, 'w') as output:
            if format == 'json':
                json.dump(data, output)
            elif format == 'csv':
                Mixpanel.write_items_to_csv(data, output)
            else:
                msg = "Invalid format - must be 'json' or 'csv': format = " + str(format) + '\n' \
                      + "Dumping json to " + output_file
                logging.warning(msg)
                json.dump(data, output)

    @staticmethod
    def _prep_event_for_import(event, token, timezone_offset):
        if ('time' not in event['properties']) or ('distinct_id' not in event['properties']):
            logging.warning('Event missing time or distinct_id property, dumping to invalid_events.txt!')
            with open('invalid_events.txt', 'a') as invalid:
                json.dump(event, invalid)
                invalid.write('\n')
                return
        event_copy = deepcopy(event)
        event_copy['properties']['time'] = int(event['properties']['time']) - (
            timezone_offset * 3600)  # transforms timestamp to UTC
        event_copy['properties']['token'] = token
        return event_copy

    @staticmethod
    def _prep_profile_for_import(profile, token, ignore_alias):
        params = {
            '$ignore_time': True,
            '$ignore_alias': ignore_alias,
            '$ip': 0,
            '$token': token,
            '$distinct_id': profile['$distinct_id'],
            '$set': profile['$properties']
        }
        return params

    def _get_engage_page(self, params):
        response = self.request(Mixpanel.API_URL, ['engage'], params)
        data = json.loads(response)
        if 'results' in data:
            return data
        else:
            logging.warning("Invalid response from /engage: " + response)

    def _send_batch(self, endpoint, batch, retries=0):
        payload = {"data": base64.b64encode(json.dumps(batch)), "verbose": 1}
        try:
            response = self.request(Mixpanel.IMPORT_URL, [endpoint], payload, 'POST')
            msg = "Sent " + str(len(batch)) + " items on " + strftime("%Y-%m-%d %H:%M:%S") + "!"
            logging.debug(msg)
            return response
        except urllib2.HTTPError as err:
            if err.code == 503:
                if retries < self.max_retries:
                    logging.warning("HTTP Error 503: Retry #" + str(retries + 1))
                    self._send_batch(endpoint, batch, retries + 1)
                else:
                    logging.warning("Failed to import batch, dumping to file: import_backup.txt")
                    with open('import_backup.txt', 'a') as backup:
                        json.dump(batch, backup)
                        backup.write('\n')
            else:
                raise

    def request(self, base_url, path_components, params, method='GET'):
        if method == 'POST':
            data = Mixpanel.unicode_urlencode(params)
            request_url = '/'.join([base_url] + path_components) + '/'
        else:
            data = None
            request_url = '/'.join(
                [base_url, str(Mixpanel.VERSION)] + path_components) + '/?' + Mixpanel.unicode_urlencode(params)
        logging.debug("Request URL: " + request_url)
        headers = {'Authorization': 'Basic {encoded_secret}'.format(encoded_secret=base64.b64encode(self.api_secret))}
        request = urllib2.Request(request_url, data, headers)
        response = urllib2.urlopen(request, timeout=self.timeout)
        response_data = response.read()
        return response_data

    def query_export(self, params):
        response = self.request(Mixpanel.DATA_URL, ['export'], params)
        file_like_object = cStringIO.StringIO(response)
        raw_data = file_like_object.getvalue().split('\n')
        raw_data.pop()
        events = []
        for line in raw_data:
            events.append(json.loads(line))
        return events

    def query_engage(self, params={}):
        paginator = ConcurrentPaginator(self._get_engage_page, concurrency=self.pool_size)
        return paginator.fetch_all(params)

    def export_events(self, output_file, params, format='json'):
        events = self.query_export(params)
        Mixpanel._export_data(events, output_file, format)

    def export_people(self, output_file, params={}, format='json'):
        profiles = self.query_engage(params)
        Mixpanel._export_data(profiles, output_file, format)

    def import_events(self, data, timezone_offset=0):
        self._import_data(data, 'events', timezone_offset=timezone_offset)

    def import_people(self, data, ignore_alias=False):
        self._import_data(data, 'people', ignore_alias=ignore_alias)

    def _import_data(self, data, item_type, timezone_offset=0, ignore_alias=False):
        assert self.token, "Project token required for import!"
        item_list = []
        if isinstance(data, basestring):
            item_list = Mixpanel.list_from_items_filename(data)
        elif isinstance(data, list):
            item_list = data
        else:
            logging.warning("data parameter must be a filename or a list of events")

        pool = ThreadPool(processes=self.pool_size)
        batch = []
        args = [{}, self.token]

        if item_type == 'events':
            endpoint = 'import'
            args.append(timezone_offset)
            prep_function = Mixpanel._prep_event_for_import
        elif item_type == 'people':
            endpoint = 'engage'
            args.append(ignore_alias)
            prep_function = Mixpanel._prep_profile_for_import
        else:
            logging.warning('Item type must be "events" or "people", found: ' + str(item_type))
            return

        for item in item_list:
            args[0] = item
            params = prep_function(*args)
            if params:
                batch.append(params)
            if len(batch) == 50:
                pool.apply_async(self._send_batch, args=(endpoint, batch), callback=Mixpanel.response_handler_callback)
                batch = []
        if len(batch):
            pool.apply_async(self._send_batch, args=(endpoint, batch), callback=Mixpanel.response_handler_callback)
        pool.close()
        pool.join()