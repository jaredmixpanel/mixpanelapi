from unittest import TestCase
from mixpanel_api.mixpanel_api import Mixpanel
import os
import csv
import json
import base64
import time
import random
import string
from datetime import date, timedelta
from copy import deepcopy
import uuid


class TestMixpanel(TestCase):
    def setUp(self):
        self.maxDiff = None
        # Main test project: https://mixpanel.com/report/1039339/
        self.mixpanel = Mixpanel('691bfbf163af2deff808fcc3d4c2a9e8', 'd92eb752ebbffe57556ff6ea4f8a9125')
        # Import only test project: https://mixpanel.com/report/1039477/
        self.import_project = Mixpanel('0360aa57ccea3a589d216aa6a2c59a35', '8b3b4ca883462e2d98d3879b5d259e59')

    def test_unicode_urlencode(self):
        params = {'$key 1': 'value!', 'key2': '"Hello, World"', '$list': ['a', '%', '_8']}
        encoded_params = 'key2=%22Hello%2C+World%22&%24key+1=value%21&%24list=%5B%22a%22%2C+%22%25%22%2C+%22_8%22%5D'
        self.assertEqual(self.mixpanel.unicode_urlencode(params), encoded_params)

    def test_response_handler_callback(self):
        response = '{"status": 0, "error": "missing required parameters"}'
        self.assertRaises(RuntimeError, self.mixpanel.response_handler_callback, response)

    def test_write_items_to_csv_with_events(self):
        items = [{'event': 'page view', 'properties': {'prop1': 'val1', 'prop2': 'val2'}},
                 {'event': 'login', 'properties': {'prop3': 'val3', 'prop2': 'val2'}}]

        expected_output = 'event,prop1,prop2,prop3\r\npage view,val1,val2,\r\nlogin,,val2,val3\r\n'

        with open('event_items.csv', 'a+') as f:
            try:
                self.mixpanel.write_items_to_csv(items, f)
                f.seek(0)
                test_output = f.read()
                self.assertEqual(expected_output, test_output)
            finally:
                os.remove('event_items.csv')

    def test_write_items_to_csv_with_people(self):
        items = [{'$distinct_id': 'abc123', '$properties': {'prop1': 'val1', 'prop2': 'val2'}},
                 {'$distinct_id': 'xyz456', '$properties': {'prop3': 'val3', 'prop2': 'val2'}}]

        expected_output = '$distinct_id,prop1,prop2,prop3\r\nabc123,val1,val2,\r\nxyz456,,val2,val3\r\n'

        with open('people_items.csv', 'a+') as f:
            try:
                self.mixpanel.write_items_to_csv(items, f)
                f.seek(0)
                test_output = f.read()
                self.assertEqual(expected_output, test_output)
            finally:
                os.remove('people_items.csv')

    def test_properties_from_csv_row_events(self):
        with open('events_items_gold.csv', 'rbU') as f:
            reader = csv.reader(f)
            header = reader.next()
            row = reader.next()
            ignored = ['event', 'distinct_id', 'time']

            expected_props = {'Invited User?': False, 'App Version': 3, 'Referrering Domain': 'http://duckduckgo.com',
                              '$model': 'iPad3,4', '$import': True, 'Campaign Name': 'Buy Now', '$os': 'iPhone OS',
                              'Registration Date': '2016-07-21T00:00:11', 'Campaign Source': 'Email',
                              'mp_country_code': 'US'}

            test_props = self.mixpanel.properties_from_csv_row(row, header, ignored)

            self.assertEqual(expected_props, test_props)

    def test_properties_from_csv_row_people(self):
        with open('people_items_gold.csv', 'rbU') as f:
            reader = csv.reader(f)
            header = reader.next()
            row = reader.next()
            ignored = ['$distinct_id']

            expected_props = {'Invited User?': False, 'App Version': 3, 'Referrering Domain': 'http://reddit.com',
                              '$model': 'iPhone6,1', '$region': 'Florida', '$unsubscribed': ':true',
                              '$timezone': 'America/New_York', '$email': 'shadow.ranger@hotmail.com',
                              '$last_name': 'Miller', 'Campaign Name': 'Super Sale', '$country_code': 'US',
                              '$city': 'Orange Park', '$first_name': 'Jeffery', 'Current Level': 1, '$os': 'iPhone OS',
                              'Registration Date': '2016-08-16T04:55:06', 'Campaign Source': 'Google Adwords',
                              '$predict_grade': 'A'}

            test_props = self.mixpanel.properties_from_csv_row(row, header, ignored)

            self.assertEqual(expected_props, test_props)

    def test_event_object_from_csv_row(self):
        with open('events_items_gold.csv', 'rbU') as f:
            reader = csv.reader(f)
            header = reader.next()
            row = reader.next()

            expected_object = {'event': 'Registration Complete',
                               'properties': {'Invited User?': False, 'App Version': 3,
                                              'Referrering Domain': 'http://duckduckgo.com', '$model': 'iPad3,4',
                                              '$import': True, 'Campaign Name': 'Buy Now',
                                              'distinct_id': '693888f8-235f-44dc-8987-ab4f18c20e67', 'time': 1469059211,
                                              '$os': 'iPhone OS', 'Registration Date': '2016-07-21T00:00:11',
                                              'Campaign Source': 'Email', 'mp_country_code': 'US'}}

            test_object = self.mixpanel.event_object_from_csv_row(row, header)

            self.assertEqual(expected_object, test_object)

    def test_people_object_from_csv_row(self):
        with open('people_items_gold.csv', 'rbU') as f:
            reader = csv.reader(f)
            header = reader.next()
            row = reader.next()

            expected_object = {'$distinct_id': '5c4f0859-80b1-40ab-bbc0-0b7457310138',
                               '$properties': {'Invited User?': False, 'App Version': 3,
                                               'Referrering Domain': 'http://reddit.com', '$model': 'iPhone6,1',
                                               '$region': 'Florida', '$unsubscribed': ':true',
                                               '$timezone': 'America/New_York', '$email': 'shadow.ranger@hotmail.com',
                                               '$last_name': 'Miller', 'Campaign Name': 'Super Sale',
                                               '$country_code': 'US', '$city': 'Orange Park', '$first_name': 'Jeffery',
                                               'Current Level': 1, '$os': 'iPhone OS',
                                               'Registration Date': '2016-08-16T04:55:06',
                                               'Campaign Source': 'Google Adwords', '$predict_grade': 'A'}}

            test_object = self.mixpanel.people_object_from_csv_row(row, header)

            self.assertEqual(expected_object, test_object)

    def test_list_from_items_filename_with_events_csv(self):

        expected_list = [{'event': 'Registration Complete', 'properties': {'Invited User?': False, 'App Version': 3,
                                                                           'Referrering Domain': 'http://duckduckgo.com',
                                                                           '$model': 'iPad3,4', '$import': True,
                                                                           'Campaign Name': 'Buy Now',
                                                                           'distinct_id': '693888f8-235f-44dc-8987-ab4f18c20e67',
                                                                           'time': 1469059211, '$os': 'iPhone OS',
                                                                           'Registration Date': '2016-07-21T00:00:11',
                                                                           'Campaign Source': 'Email',
                                                                           'mp_country_code': 'US'}},
                         {'event': 'Registration Complete', 'properties': {'Invited User?': False, 'App Version': 3,
                                                                           'Referrering Domain': 'http://bing.com',
                                                                           '$model': 'iPhone6,1', '$import': True,
                                                                           'Campaign Name': 'Organic',
                                                                           'distinct_id': '16bc248f-446d-418f-85cd-c76cf26ce29c',
                                                                           'time': 1469059215, '$os': 'iPhone OS',
                                                                           'Registration Date': '2016-07-21T00:00:15',
                                                                           'Campaign Source': 'Organic',
                                                                           'mp_country_code': 'US'}},
                         {'event': 'App Install',
                          'properties': {'Invited User?': False, 'App Version': 3, 'Referrering Domain': 'Organic',
                                         '$model': 'iPhone6,1', '$region': 'Michigan', '$import': True,
                                         'Campaign Name': 'Super Sale',
                                         'distinct_id': '1cb813e7-72c6-4a8f-a34a-68bf26d43652', '$city': 'Dearborn',
                                         'time': 1469059217, '$os': 'iPhone OS', 'Campaign Source': 'Twitter',
                                         'mp_country_code': 'US'}}]

        test_list = self.mixpanel.list_from_items_filename('events_items_gold.csv')
        self.assertEqual(expected_list, test_list)

    def test_list_from_items_filename_with_events_json(self):

        expected_list = [{u'event': u'Registration Complete',
                          u'properties': {u'Invited User?': u'False', u'App Version': u'3',
                                          u'Referrering Domain': u'http://duckduckgo.com', u'$model': u'iPad3,4',
                                          u'$import': True, u'Campaign Name': u'Buy Now',
                                          u'distinct_id': u'693888f8-235f-44dc-8987-ab4f18c20e67', u'time': 1469059211,
                                          u'$os': u'iPhone OS', u'Registration Date': u'2016-07-21T00:00:11',
                                          u'Campaign Source': u'Email', u'mp_country_code': u'US'}},
                         {u'event': u'Registration Complete',
                          u'properties': {u'Invited User?': u'False', u'App Version': u'3',
                                          u'Referrering Domain': u'http://bing.com', u'$model': u'iPhone6,1',
                                          u'$import': True, u'Campaign Name': u'Organic',
                                          u'distinct_id': u'16bc248f-446d-418f-85cd-c76cf26ce29c', u'time': 1469059215,
                                          u'$os': u'iPhone OS', u'Registration Date': u'2016-07-21T00:00:15',
                                          u'Campaign Source': u'Organic', u'mp_country_code': u'US'}},
                         {u'event': u'App Install', u'properties': {u'Invited User?': u'False', u'App Version': u'3',
                                                                    u'Referrering Domain': u'Organic',
                                                                    u'$model': u'iPhone6,1', u'$region': u'Michigan',
                                                                    u'$import': True, u'Campaign Name': u'Super Sale',
                                                                    u'distinct_id': u'1cb813e7-72c6-4a8f-a34a-68bf26d43652',
                                                                    u'$city': u'Dearborn', u'time': 1469059217,
                                                                    u'$os': u'iPhone OS',
                                                                    u'Campaign Source': u'Twitter',
                                                                    u'mp_country_code': u'US'}}]

        test_list = self.mixpanel.list_from_items_filename('events_items_gold.json')
        self.assertEqual(expected_list, test_list)

    def test_list_from_items_filename_with_people_csv(self):

        expected_list = [{'$distinct_id': '5c4f0859-80b1-40ab-bbc0-0b7457310138',
                          '$properties': {'Invited User?': False, 'App Version': 3,
                                          'Referrering Domain': 'http://reddit.com', '$model': 'iPhone6,1',
                                          '$region': 'Florida', '$unsubscribed': ':true',
                                          '$timezone': 'America/New_York', '$email': 'shadow.ranger@hotmail.com',
                                          '$last_name': 'Miller', 'Campaign Name': 'Super Sale', '$country_code': 'US',
                                          '$city': 'Orange Park', '$first_name': 'Jeffery', 'Current Level': 1,
                                          '$os': 'iPhone OS', 'Registration Date': '2016-08-16T04:55:06',
                                          'Campaign Source': 'Google Adwords', '$predict_grade': 'A'}},
                         {'$distinct_id': '20b54c07-6e7a-4014-a4e9-2ab9bc48378e',
                          '$properties': {'Invited User?': False, 'App Version': 3, 'Referrering Domain': 'Organic',
                                          '$model': 'GT-I9500', '$predict_grade': 'A', '$unsubscribed': ':true',
                                          '$email': 'lancer.love@yahoo.com', '$last_name': 'Peterson',
                                          'Campaign Name': 'Organic', '$first_name': 'Walter', 'Current Level': 1,
                                          '$os': 'Android', 'Registration Date': '2016-08-16T09:10:02',
                                          'Campaign Source': 'Organic'}},
                         {'$distinct_id': '58d0f6a3-d533-41dd-9e31-2d6f84d7de2e',
                          '$properties': {'Referrering Domain': 'http://baidu.com', 'Total Games Played': 2,
                                          '$city': 'Beijing', 'Current Level': 3, '$timezone': 'Asia/Shanghai',
                                          'Invited User?': False, 'Last Visit': '2016-08-16T19:42:29',
                                          '$unsubscribed': ':true', 'Registration Date': '2016-08-16T09:33:08',
                                          'Last Game Played': '2016-08-16T19:34:03', '$os': 'iPhone OS',
                                          '$email': 'trebuchet.hound@hotmail.com', 'Campaign Name': 'Organic',
                                          '$country_code': 'CN', 'Last Level Completed': '2016-08-16T19:40:13',
                                          'App Version': 3, '$model': 'iPad4,1', '$region': 'Beijing Shi',
                                          '$last_name': 'West', '$first_name': 'Ray', '$predict_grade': 'A',
                                          'Campaign Source': 'Organic'}}]

        test_list = self.mixpanel.list_from_items_filename('people_items_gold.csv')
        self.assertEqual(expected_list, test_list)

    def test_list_from_items_filename_with_people_json(self):

        expected_list = [{u'$distinct_id': u'5c4f0859-80b1-40ab-bbc0-0b7457310138',
                          u'$properties': {u'Invited User?': u'False', u'App Version': u'3', u'$country_code': u'US',
                                           u'$model': u'iPhone6,1', u'$region': u'Florida', u'$unsubscribed': u':true',
                                           u'$timezone': u'America/New_York', u'$email': u'shadow.ranger@hotmail.com',
                                           u'$last_name': u'Miller', u'$os': u'iPhone OS',
                                           u'Campaign Name': u'Super Sale', u'Referrering Domain': u'http://reddit.com',
                                           u'$city': u'Orange Park', u'$first_name': u'Jeffery', u'Current Level': 1,
                                           u'$predict_grade': u'A', u'Registration Date': u'2016-08-16T04:55:06',
                                           u'Campaign Source': u'Google Adwords'}},
                         {u'$distinct_id': u'20b54c07-6e7a-4014-a4e9-2ab9bc48378e',
                          u'$properties': {u'Invited User?': u'False', u'App Version': u'3',
                                           u'Referrering Domain': u'Organic', u'$model': u'GT-I9500',
                                           u'$unsubscribed': u':true', u'$email': u'lancer.love@yahoo.com',
                                           u'$last_name': u'Peterson', u'$os': u'Android', u'Campaign Name': u'Organic',
                                           u'$first_name': u'Walter', u'Current Level': 1, u'$predict_grade': u'A',
                                           u'Registration Date': u'2016-08-16T09:10:02',
                                           u'Campaign Source': u'Organic'}},
                         {u'$distinct_id': u'58d0f6a3-d533-41dd-9e31-2d6f84d7de2e',
                          u'$properties': {u'Referrering Domain': u'http://baidu.com', u'Total Games Played': 2,
                                           u'$city': u'Beijing', u'Current Level': 3, u'$timezone': u'Asia/Shanghai',
                                           u'Invited User?': u'False', u'Last Visit': u'2016-08-16T19:42:29',
                                           u'$unsubscribed': u':true', u'Registration Date': u'2016-08-16T09:33:08',
                                           u'Last Game Played': u'2016-08-16T19:34:03', u'$os': u'iPhone OS',
                                           u'$email': u'trebuchet.hound@hotmail.com', u'Campaign Name': u'Organic',
                                           u'$country_code': u'CN', u'Last Level Completed': u'2016-08-16T19:40:13',
                                           u'App Version': u'3', u'$model': u'iPad4,1', u'$region': u'Beijing Shi',
                                           u'$last_name': u'West', u'$first_name': u'Ray', u'$predict_grade': u'A',
                                           u'Campaign Source': u'Organic'}}]

        test_list = self.mixpanel.list_from_items_filename('people_items_gold.json')
        self.assertEqual(expected_list, test_list)

    def test__export_data_with_events(self):
        with open('events_items_gold.json', 'rbU') as gold_json_file:
            gold_json_data = json.load(gold_json_file)
            self.mixpanel._export_data(gold_json_data, 'events_data.json')
            self.mixpanel._export_data(gold_json_data, 'events_data.csv', format='csv')
            with open('events_data.json', 'rbU') as j, open('events_data.csv', 'rbU') as c, \
                    open('events_items_gold.csv', 'rbU') as g:
                try:
                    gold_csv_data = csv.reader(g)
                    test_csv_data = csv.reader(c)
                    test_json_data = json.load(j)
                    self.assertItemsEqual(gold_json_data, test_json_data,
                                          msg="Exported JSON events data doesn't match.")
                    self.assertItemsEqual(gold_csv_data, test_csv_data, msg="Exported CSV events data doesn't match.")
                finally:
                    os.remove('events_data.json')
                    os.remove('events_data.csv')

    def test__export_data_with_people(self):
        with open('people_items_gold.json', 'rbU') as gold_json_file:
            gold_json_data = json.load(gold_json_file)
            self.mixpanel._export_data(gold_json_data, 'people_data.json')
            self.mixpanel._export_data(gold_json_data, 'people_data.csv', format='csv')
            with open('people_data.json', 'rbU') as j, open('people_data.csv', 'rbU') as c, \
                    open('people_items_gold.csv', 'rbU') as g:
                try:
                    gold_csv_data = csv.reader(g)
                    test_csv_data = csv.reader(c)
                    test_json_data = json.load(j)
                    self.assertItemsEqual(gold_json_data, test_json_data,
                                          msg="Exported JSON People data doesn't match.")
                    self.assertItemsEqual(gold_csv_data, test_csv_data, msg="Exported CSV People data doesn't match.")
                finally:
                    os.remove('people_data.json')
                    os.remove('people_data.csv')

    def test__prep_event_for_import(self):
        input_event = {'event': 'page view',
                       'properties': {'distinct_id': 12345, 'prop1': 'val1', 'prop2': 'val2', 'time': 1471503600}}
        gold_event = {'event': 'page view',
                      'properties': {'token': '123', 'distinct_id': 12345, 'prop1': 'val1', 'prop2': 'val2',
                                     'time': 1471528800}}
        no_time = {'event': 'page view',
                   'properties': {'distinct_id': 12345, 'prop1': 'val1', 'prop2': 'val2'}}
        no_distinct_id = {'event': 'page view',
                          'properties': {'prop1': 'val1', 'prop2': 'val2', 'time': 1471503600}}
        test_event = self.mixpanel._prep_event_for_import(input_event, '123', -7)
        self.assertEqual(gold_event, test_event)
        self.assertRaises(AssertionError, self.mixpanel._prep_event_for_import, no_time, '123', -7)
        self.assertRaises(AssertionError, self.mixpanel._prep_event_for_import, no_distinct_id, '123', -7)

    def test__prep_profile_for_import(self):
        input_profile = {'$distinct_id': 'abc123', '$properties': {'prop1': 'val1', 'prop2': 'val2'}}
        gold_profile = {'$ignore_time': True, '$ignore_alias': False, '$set': {'prop1': 'val1', 'prop2': 'val2'},
                        '$token': '123', '$distinct_id': 'abc123', '$ip': 0}
        test_profile = self.mixpanel._prep_profile_for_import(input_profile, '123', False)
        self.assertEqual(gold_profile, test_profile)

    def test__get_engage_page(self):
        params = {'where': '(("Kelly" in properties["$first_name"]) and (defined (properties["$first_name"])))'}
        gold_data = {u'status': u'ok', u'results': [{u'$distinct_id': u'77d57cbd-ec0b-4e02-93c0-c738a9ed59d8',
                                                     u'$properties': {u'Invited User?': False, u'App Version': 3,
                                                                      u'$country_code': u'US', u'$model': u'iPhone5,2',
                                                                      u'$predict_grade': u'A',
                                                                      u'$unsubscribed': u':true',
                                                                      u'Registration Date': u'2016-08-17T01:30:20',
                                                                      u'$email': u'dance.troll@yahoo.com',
                                                                      u'$region': u'California',
                                                                      u'$last_name': u'Cooper',
                                                                      u'Experiment Group': u'Group B',
                                                                      u'Campaign Name': u'Huge Discounts!',
                                                                      u'Referrering Domain': u'http://bing.com',
                                                                      u'$city': u'Salinas', u'$first_name': u'Kelly',
                                                                      u'$os': u'iPhone OS',
                                                                      u'$timezone': u'America/Los_Angeles',
                                                                      u'Campaign Source': u'Facebook'}},
                                                    {u'$distinct_id': u'34dcb1f3-f6a6-433b-b402-436717ceaa82',
                                                     u'$properties': {u'Invited User?': False, u'App Version': 3,
                                                                      u'$country_code': u'US', u'$model': u'iPad2,5',
                                                                      u'$predict_grade': u'A',
                                                                      u'$unsubscribed': u':true',
                                                                      u'Registration Date': u'2016-08-16T18:23:01',
                                                                      u'$email': u'hacker.lancer@hotmail.com',
                                                                      u'$region': u'Pennsylvania',
                                                                      u'$last_name': u'Burton',
                                                                      u'Campaign Name': u'Super Sale',
                                                                      u'Referrering Domain': u'http://facebook.com',
                                                                      u'$city': u'Hershey', u'$first_name': u'Kelly',
                                                                      u'$os': u'iPhone OS',
                                                                      u'$timezone': u'America/New_York',
                                                                      u'Campaign Source': u'Facebook'}},
                                                    {u'$distinct_id': u'55c86fe3-1f7e-4842-a276-3b6e7ae4456b',
                                                     u'$properties': {u'Invited User?': True, u'App Version': 3,
                                                                      u'$country_code': u'ID', u'$model': u'iPhone5,2',
                                                                      u'$predict_grade': u'A',
                                                                      u'$unsubscribed': u':true',
                                                                      u'$email': u'giant.coward@aol.com',
                                                                      u'$last_name': u'Fernandez',
                                                                      u'Campaign Name': u'Huge Discounts!',
                                                                      u'Referrering Domain': u'http://facebook.com',
                                                                      u'$first_name': u'Kelly', u'$os': u'iPhone OS',
                                                                      u'Registration Date': u'2016-08-16T23:57:46',
                                                                      u'Campaign Source': u'Facebook'}}],
                     u'session_id': u'1472215655-vkhHzc', u'page_size': 1000, u'total': 3, u'page': 0}

        test_data = self.mixpanel._get_engage_page(params)
        for item in gold_data['results']:
            self.assertIn(item, test_data['results'])

    def test__send_batch(self):
        event_batch = []
        event_batch_with_token = []
        people_batch = []
        people_batch_with_token = []
        random_name = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))
        from_date = (date.today() - timedelta(days=6)).strftime('%Y-%m-%d')
        to_date = date.today().strftime('%Y-%m-%d')

        for x in range(0, 50):
            event = {'event': random_name,
                     'properties': {'distinct_id': 'abc123', 'prop1': 'val1',
                                    '$import': True, 'time': (int(time.time()) - 5 * 24 * 60 * 60)}}
            event_batch.append(event)

            event_with_token = deepcopy(event)
            event_with_token['properties']['token'] = self.import_project.token
            event_batch_with_token.append(event_with_token)

            profile = {'$distinct_id': str(uuid.uuid4()), '$properties': {'prop 1': 'val1', '$name': random_name}}
            people_batch.append(profile)

            profile_with_token = deepcopy(profile)
            profile_with_token['$set'] = profile_with_token.pop('$properties')
            profile_with_token.update({'$token': self.import_project.token, '$ignore_time': True, '$ip': 0})

            people_batch_with_token.append(profile_with_token)

        self.import_project._send_batch('import', event_batch_with_token)
        self.import_project._send_batch('engage', people_batch_with_token)
        # Delay to make sure items are ready for export
        time.sleep(10)
        events_params = {'from_date': from_date, 'to_date': to_date, 'event': [random_name]}
        test_event_data = self.import_project.query_export(events_params)
        people_params = {'where': '((properties["$name"] == "' + random_name + '"))'}
        test_people_data = self.import_project.query_engage(people_params)

        self.assertItemsEqual(event_batch, test_event_data)
        self.assertItemsEqual(people_batch, test_people_data)

    def test_request(self):
        seg_params = {'from_date': '2016-07-20', 'to_date': '2016-07-21', 'event': 'App Install'}
        seg_gold_data = '{"legend_size": 1, "data": {"series": ["2016-07-20", "2016-07-21"], "values": {"App Install": {"2016-07-20": 867, "2016-07-21": 2118}}}}'
        seg_test_data = self.mixpanel.request(Mixpanel.API_URL, ['segmentation'], seg_params)
        self.assertEqual(seg_gold_data, seg_test_data, msg='/segmentation data does not match')

        import_data = [{'event': 'page view', 'properties': {'prop1': 'val1', 'prop2': 'val2', 'distinct_id': 'abc123',
                                                             'token': '8b3b4ca883462e2d98d3879b5d259e59'}},
                       {'event': 'login', 'properties': {'prop3': 'val3', 'prop2': 'val2', 'distinct_id': 'xyz456',
                                                         'token': '8b3b4ca883462e2d98d3879b5d259e59'}}]
        payload = {"data": base64.b64encode(json.dumps(import_data)), "verbose": 1}
        import_response = self.import_project.request(Mixpanel.IMPORT_URL, ['import'], payload, method='POST')
        self.assertEqual('{"status": 1, "error": null}', import_response, msg='/import error in response')

    def test_query_export(self):
        with open('events_export_gold.json', 'rbU') as f:
            gold_data = json.load(f)
            params = {'from_date': '2016-07-20', 'to_date': '2016-07-21',
                      'event': ['App Install', 'Registration Complete']}
            test_data = self.mixpanel.query_export(params)
            self.assertItemsEqual(gold_data, test_data)

    def test_query_engage(self):
        params = {'where': '(datetime(1472129993) > properties["Registration Date"])'}

        with open('people_export_gold.json', 'rbU') as f:
            gold_data = json.load(f)
            test_data = self.mixpanel.query_engage(params)
            self.assertItemsEqual(gold_data, test_data)

    def test_export_events_to_json(self):
        params = {'from_date': '2016-07-20', 'to_date': '2016-07-21', 'event': ['App Install', 'Registration Complete']}
        self.mixpanel.export_events('events_export.json', params)

        with open('events_export_gold.json', 'rbU') as gold_file, \
                open('events_export.json', 'rbU') as test_file:
            try:
                gold_file.seek(0)
                test_file.seek(0)
                gold_data = json.load(gold_file)
                test_data = json.load(test_file)
                self.assertItemsEqual(gold_data, test_data)
            finally:
                os.remove('events_export.json')

    def test_export_events_to_csv(self):
        params = {'from_date': '2016-07-20', 'to_date': '2016-07-21', 'event': ['App Install', 'Registration Complete']}
        self.mixpanel.export_events('events_export.csv', params, format='csv')

        with open('events_export_gold.csv', 'rbU') as gold_file, \
                open('events_export.csv', 'rbU') as test_file:
            try:
                gold_file.seek(0)
                test_file.seek(0)
                gold_data = csv.reader(gold_file)
                test_data = csv.reader(test_file)
                self.assertItemsEqual(gold_data, test_data)
            finally:
                os.remove('events_export.csv')

    def test_export_people_to_json(self):
        params = {'where': '(datetime(1472129993) > properties["Registration Date"])'}
        self.mixpanel.export_people('people_export.json', params)

        with open('people_export_gold.json', 'rbU') as gold_file, open('people_export.json', 'rbU') as test_file:
            gold_data = json.load(gold_file)
            test_data = json.load(test_file)
            self.assertItemsEqual(gold_data, test_data)

        os.remove('people_export.json')

    def test_export_people_to_csv(self):
        params = {'where': '(datetime(1472129993) > properties["Registration Date"])'}
        self.mixpanel.export_people('people_export.csv', params, format='csv')

        with open('people_export_gold.csv', 'rbU') as gold_file, open('people_export.csv', 'rbU') as test_file:
            try:
                gold_file.seek(0)
                test_file.seek(0)
                gold_data = csv.reader(gold_file)
                test_data = csv.reader(test_file)
                self.assertItemsEqual(gold_data, test_data)
            finally:
                os.remove('people_export.csv')

    # THE TESTS BELOW REQUIRE MANUALLY RESETTING THE import_mixpanelapi PROJECT - RESET NOW
    # https://mixpanel.com/report/1039391/

    def test_import_events_json(self):
        with open('events_export_gold.json', 'rbU') as gold_json_file:
            gold_json_data = json.load(gold_json_file)
            self.import_project.import_events('events_export_gold.json')
            # Add a delay to ensure all imported events are ready for export
            time.sleep(10)
            params = {'from_date': '2016-07-20', 'to_date': '2016-07-21',
                      'event': ['App Install', 'Registration Complete']}
            test_json_data = self.import_project.query_export(params)
            self.assertItemsEqual(gold_json_data, test_json_data)

    def test_import_people_json(self):
        with open('people_export_gold.json', 'rbU') as gold_json_file:
            gold_json_data = json.load(gold_json_file)
            self.import_project.import_people('people_export_gold.json')
            # Add a delay to ensure all imported profiles are ready for export
            time.sleep(10)
            params = {'where': '(datetime(1472129993) > properties["Registration Date"])'}
            test_json_data = self.import_project.query_engage(params)
            self.assertItemsEqual(gold_json_data, test_json_data)

    # ANOTHER MANUAL RESET IS REQUIRED HERE

    def test_import_events_csv(self):
        with open('events_export_gold.json', 'rbU') as gold_json_file:
            gold_json_data = json.load(gold_json_file)
            self.import_project.import_events('events_export_gold.csv')
            # Add a delay to ensure all imported events are ready for export
            time.sleep(10)
            params = {'from_date': '2016-07-20', 'to_date': '2016-07-21',
                      'event': ['App Install', 'Registration Complete']}
            test_json_data = self.import_project.query_export(params)
            self.assertItemsEqual(gold_json_data, test_json_data)

    def test_import_people_csv(self):
        with open('people_export_gold.json', 'rbU') as gold_json_file:
            gold_json_data = json.load(gold_json_file)
            self.import_project.import_people('people_export_gold.csv')
            # Add a delay to ensure all imported profiles are ready for export
            time.sleep(10)
            params = {'where': '(datetime(1472129993) > properties["Registration Date"])'}
            test_json_data = self.import_project.query_engage(params)
            self.assertItemsEqual(gold_json_data, test_json_data)

    # def test__import_data(self):
    #     self.fail()
