import base64
import urllib  # for url encoding
import urllib2  # for sending requests
import cStringIO
import logging
import gzip
import shutil
import time
import os
import datetime
from inspect import isfunction
from itertools import chain
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool
from paginator import ConcurrentPaginator
from ast import literal_eval
from copy import deepcopy
import csv
import json

try:
    import ciso8601
except ImportError:
    pass


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
        subkeys = sorted(subkeys)

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
    def list_from_argument(arg):
        item_list = []
        if isinstance(arg, basestring):
            item_list = Mixpanel.list_from_items_filename(arg)
        elif isinstance(arg, list):
            item_list = arg
        else:
            logging.warning("data parameter must be a filename or a list of items")

        return item_list

    @staticmethod
    def list_from_items_filename(filename):
        item_list = []
        try:
            with open(filename, 'rbU') as item_file:
                item_list = json.load(item_file)
        except ValueError:
            with open(filename, 'rbU') as item_file:
                reader = csv.reader(item_file, )
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
    def gzip_file(filename):
        gzip_filename = filename + '.gz'
        with open(filename, 'rb') as f_in, gzip.open(gzip_filename, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    @staticmethod
    def _export_data(data, output_file, format='json', compress=False):
        with open(output_file, 'w+') as output:
            if format == 'json':
                json.dump(data, output)
            elif format == 'csv':
                Mixpanel.write_items_to_csv(data, output)
            else:
                msg = "Invalid format - must be 'json' or 'csv': format = " + str(format) + '\n' \
                      + "Dumping json to " + output_file
                logging.warning(msg)
                json.dump(data, output)

        if compress:
            Mixpanel.gzip_file(output_file)
            os.remove(output_file)

    @staticmethod
    def _prep_event_for_import(event, token, timezone_offset):
        if ('time' not in event['properties']) or ('distinct_id' not in event['properties']):
            logging.warning('Event missing time or distinct_id property, dumping to invalid_events.txt')
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
    def _prep_params_for_profile(profile, token, operation, value, ignore_alias, dynamic):
        if dynamic:
            op_value = value(profile)
        else:
            op_value = value

        params = {
            '$ignore_time': True,
            '$ip': 0,
            '$ignore_alias': ignore_alias,
            '$token': token,
            '$distinct_id': profile['$distinct_id'],
            operation: op_value
        }
        return params

    @staticmethod
    def dt_from_iso(profile):
        dt = datetime.datetime.min
        try:
            last_seen = profile["$properties"]["$last_seen"]
            try:
                dt = ciso8601.parse_datetime_unaware(last_seen)
            except NameError:
                dt = datetime.datetime.strptime(last_seen, "%Y-%m-%dT%H:%M:%S")
        except KeyError:
            return dt
        return dt

    @staticmethod
    def sum_transactions(profile):
        total = 0
        try:
            transactions = profile['$properties']['$transactions']
            for t in transactions:
                total = total + t['$amount']
        except KeyError:
            pass
        return {'Revenue': total}

    def _get_engage_page(self, params):
        response = self.request(Mixpanel.API_URL, ['engage'], params)
        data = json.loads(response)
        if 'results' in data:
            return data
        else:
            logging.warning("Invalid response from /engage: " + response)

    def _dispatch_batches(self, endpoint, item_list, prep_args):
        pool = ThreadPool(processes=self.pool_size)
        batch = []

        if endpoint == 'import':
            prep_function = Mixpanel._prep_event_for_import
        elif endpoint == 'engage':
            prep_function = Mixpanel._prep_params_for_profile
        else:
            logging.warning('endpoint must be "import" or "engage", found: ' + str(endpoint))
            return

        for item in item_list:
            prep_args[0] = item
            params = prep_function(*prep_args)
            if params:
                batch.append(params)
            if len(batch) == 50:
                pool.apply_async(self._send_batch, args=(endpoint, batch), callback=Mixpanel.response_handler_callback)
                batch = []
        if len(batch):
            pool.apply_async(self._send_batch, args=(endpoint, batch), callback=Mixpanel.response_handler_callback)
        pool.close()
        pool.join()

    def _send_batch(self, endpoint, batch, retries=0):
        payload = {"data": base64.b64encode(json.dumps(batch)), "verbose": 1}
        try:
            response = self.request(Mixpanel.IMPORT_URL, [endpoint], payload, 'POST')
            msg = "Sent " + str(len(batch)) + " items on " + time.strftime("%Y-%m-%d %H:%M:%S") + "!"
            logging.debug(msg)
            return response
        except urllib2.HTTPError as err:
            if err.code == 503:
                if retries < self.max_retries:
                    logging.warning("HTTP Error 503: Retry #" + str(retries + 1))
                    self._send_batch(endpoint, batch, retries + 1)
                else:
                    logging.warning("Failed to import batch, dumping to file import_backup.txt")
                    with open('import_backup.txt', 'a') as backup:
                        json.dump(batch, backup)
                        backup.write('\n')
            else:
                raise

    def request(self, base_url, path_components, params, method='GET'):
        """
        Base method for sending HTTP requests to the various Mixpanel APIs

        :param base_url: Ex: https://api.mixpanel.com
        :type base_url: str
        :param path_components: endpoint path as list of strings
        :type path_components: list
        :param params: dictionary containing the Mixpanel parameters for the API request
        :type params: dict
        :param method: GET or POST
        :type method: str
        :return: JSON data returned from API
        """
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

    def people_operation(self, operation, value, profiles=None, query_params=None, ignore_alias=False, backup=False,
                         backup_file=None):
        """
        Base method for performing any of the People analytics operations

        :param operation: a string with name of a Mixpanel People operation, like $set or $delete
        :type operation: str
        :param value: can be a static value applied to all profiles or a user-defined function (or lambda) that takes a
        profile as its only parameter and returns the value to use for the operation on the given profile
        :param profiles: can be a Python list of profiles or the name of a file containing a json array dump of profiles
        :param query_params: params to query engage with (alternative to supplying the profiles param)
        :param ignore_alias: True or False
        :type ignore_alias: bool
        :param backup: True to create backup file otherwise False (default)
        :type backup: bool
        """
        assert self.token, "Project token required for People operation!"
        if profiles is not None and query_params is not None:
            logging.warning("profiles and query_params both provided, please use one or the other")
            return

        profiles_list = []
        if profiles:
            profiles_list = Mixpanel.list_from_argument(profiles)

        if query_params is not None:
            profiles_list = self.query_engage(query_params)

        if backup:
            if backup_file is None:
                backup_file = "backup_" + str(int(time.time())) + ".json"
            self._export_data(profiles_list, backup_file)

        dynamic = isfunction(value)
        self._dispatch_batches('engage', profiles_list, [{}, self.token, operation, value, ignore_alias, dynamic])

    def people_delete(self, profiles=None, query_params=None, backup=True, backup_file=None):
        self.people_operation('$delete', '', profiles=profiles, query_params=query_params, ignore_alias=True,
                              backup=backup, backup_file=backup_file)

    def people_set(self, value, profiles=None, query_params=None, ignore_alias=True, backup=True, backup_file=None):
        self.people_operation('$set', value=value, profiles=profiles, query_params=query_params,
                              ignore_alias=ignore_alias, backup=backup, backup_file=backup_file)

    def people_set_once(self, value, profiles=None, query_params=None, ignore_alias=True, backup=False,
                        backup_file=None):
        self.people_operation('$set_once', value=value, profiles=profiles, query_params=query_params,
                              ignore_alias=ignore_alias, backup=backup, backup_file=backup_file)

    def people_unset(self, value, profiles=None, query_params=None, ignore_alias=True, backup=True, backup_file=None):
        self.people_operation('$unset', value=value, profiles=profiles, query_params=query_params,
                              ignore_alias=ignore_alias, backup=backup, backup_file=backup_file)

    def people_add(self, value, profiles=None, query_params=None, ignore_alias=True, backup=True, backup_file=None):
        self.people_operation('$add', value=value, profiles=profiles, query_params=query_params,
                              ignore_alias=ignore_alias, backup=backup, backup_file=backup_file)

    def people_append(self, value, profiles=None, query_params=None, ignore_alias=True, backup=True,
                      backup_file=None):
        self.people_operation('$append', value=value, profiles=profiles, query_params=query_params,
                              ignore_alias=ignore_alias, backup=backup, backup_file=backup_file)

    def people_union(self, value, profiles=None, query_params=None, ignore_alias=True, backup=True, backup_file=None):
        self.people_operation('$union', value=value, profiles=profiles, query_params=query_params,
                              ignore_alias=ignore_alias, backup=backup, backup_file=backup_file)

    def people_remove(self, value, profiles=None, query_params=None, ignore_alias=True, backup=True, backup_file=None):
        self.people_operation('$remove', value=value, profiles=profiles, query_params=query_params,
                              ignore_alias=ignore_alias, backup=backup, backup_file=backup_file)

    def people_change_property_name(self, old_name, new_name, profiles=None, query_params=None, ignore_alias=True,
                                    backup=True, backup_file=None, unset=True):
        if profiles is None and query_params is None:
            query_params = {'selector': '(defined (properties["' + old_name + '"]))'}
        self.people_operation('$set', lambda p: {new_name: p['$properties'][old_name]}, query_params=query_params,
                              ignore_alias=ignore_alias, backup=backup, backup_file=backup_file)
        if unset:
            self.people_operation('$unset', [old_name], profiles=profiles, query_params=query_params, backup=False)

    def people_revenue_property_from_transactions(self, profiles=None, query_params=None, ignore_alias=True,
                                                  backup=True, backup_file=None):
        if profiles is None and query_params is None:
            query_params = {'selector': '(defined (properties["$transactions"]))'}

        self.people_operation('$set', Mixpanel.sum_transactions, profiles=profiles, query_params=query_params,
                              ignore_alias=ignore_alias, backup=backup, backup_file=backup_file)

    def deduplicate_people(self, profiles=None, prop_to_match='$email', merge_props=False, case_sensitive=False):
        main_reference = {}
        update_profiles = []
        delete_profiles = []

        if profiles is not None:
            profiles_list = Mixpanel.list_from_argument(profiles)
        else:
            selector = '(boolean(properties["' + prop_to_match + '"]) == true)'
            profiles_list = self.query_engage({'where': selector})

        for profile in profiles_list:
            try:
                if case_sensitive:
                    match_prop = str(profile["$properties"][prop_to_match])
                else:
                    match_prop = str(profile["$properties"][prop_to_match]).lower()
            except KeyError:
                continue

            if not main_reference.get(match_prop):
                main_reference[match_prop] = []

            main_reference[match_prop].append(profile)

        for matching_prop, matching_profiles in main_reference.iteritems():
            if len(matching_profiles) > 1:
                matching_profiles.sort(key=lambda dupe: Mixpanel.dt_from_iso(dupe))
                # We create a $delete update for each duplicate profile and at the same time create a
                # $set_once update for the keeper profile by working through duplicates oldest to newest
                if merge_props:
                    prop_update = {"$distinct_id": matching_profiles[-1]["$distinct_id"], "$properties": {}}
                for x in xrange(len(matching_profiles) - 1):
                    delete_profiles.append({'$distinct_id': matching_profiles[x]['$distinct_id']})
                    if merge_props:
                        prop_update["$properties"].update(matching_profiles[x]["$properties"])
                if merge_props and "$last_seen" in prop_update["$properties"]:
                    del prop_update["$properties"]["$last_seen"]
                if merge_props:
                    update_profiles.append(prop_update)

        if merge_props:
            self.people_operation('$set_once', lambda p: p['$properties'], profiles=update_profiles, ignore_alias=True)

        self.people_operation('$delete', '', profiles=delete_profiles, ignore_alias=True)

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

    def export_events(self, output_file, params, format='json', compress=False):
        # Increase timeout to 15 minutes if it's still set to default
        if self.timeout == 120:
            self.timeout = 900
        events = self.query_export(params)
        Mixpanel._export_data(events, output_file, format=format, compress=compress)

    def export_people(self, output_file, params={}, format='json', compress=False):
        profiles = self.query_engage(params)
        Mixpanel._export_data(profiles, output_file, format=format, compress=compress)

    def import_events(self, data, timezone_offset=0):
        self._import_data(data, 'import', timezone_offset=timezone_offset)

    def import_people(self, data, ignore_alias=False):
        self._import_data(data, 'engage', ignore_alias=ignore_alias)

    def _import_data(self, data, endpoint, timezone_offset=0, ignore_alias=False):
        assert self.token, "Project token required for import!"
        item_list = Mixpanel.list_from_argument(data)
        args = [{}, self.token]
        if endpoint == 'import':
            args.append(timezone_offset)
        elif endpoint == 'engage':
            args.extend(['$set', lambda profile: profile['$properties'], ignore_alias, True])

        self._dispatch_batches(endpoint, item_list, args)
