import datetime
import influxdb
import uuid

from junit2influx import xunitparser


def prepare_test_point(time, context, test_data, measurement):
    final_data = {
        'measurement': measurement,
        'time': time.isoformat(),
        'fields': context.get('fields', {}).copy(),
        'tags': context.get('tags', {}).copy(),
    }
    # this is a hack because in influxdb there is no built-in way
    # to count a number of rows, only values themselves.
    # so we add a dummy field with 1 as the values, so we can sum on it
    final_data['fields']['_quantity'] = 1
    # final_data['fields']['uuid'] =  str(uuid.uuid4())
    final_data['fields']['duration'] = test_data['time']
    final_data['fields']['name'] = test_data['name']
    final_data['fields']['classname'] = test_data['classname']
    final_data['fields']['message'] = test_data['message']
    # final_data['fields']['tests'] = 1
    final_data['tags']['result'] = test_data['result']
    final_data['tags']['feature'] = test_data['feature']
    final_data['tags']['uuid'] = str(uuid.uuid4())

    if test_data['result'] == "success":
        final_data['fields']['pass'] = 1
        final_data['fields']['fail'] = 0
    else:
        final_data['fields']['pass'] = 0
        final_data['fields']['fail'] = 1
    print(final_data)
    return final_data


def prepare_build_point(time, context, tests_data, measurement):
    final_data = {
        'measurement': measurement,
        'time': time.isoformat(),
        'fields': context.get('fields', {}).copy(),
        'tags': context.get('tags', {}).copy(),
    }

    # this is a hack because in influxdb there is no built-in way
    # to count a number of rows, only values themselves.
    # so we add a dummy field with 1 as the values, so we can sum on it
    # print (tests_data)
    
    final_data['fields']['_quantity'] = 1
    final_data['fields']['duration'] = sum([
        t['time'] for t in tests_data if t['time']])
    final_data['fields']['tests'] = len(tests_data)

    results = {'success': 0, 'error': 0, 'failure': 0, 'skipped': 0}
    for t in tests_data:
        results[t['result']] += 1
    
    for key, value in results.items():
        final_data['fields']['{}_count'.format(key)] = value
    final_data['fields']['failure_and_error_count'] = (
        results['error'] + results['failure'] )    

    if results['error'] > 0 or results['failure'] > 0:
        final_data['tags']['result'] = 'failure'
    else:
        final_data['tags']['result'] = 'success'
    return final_data


def push(junit_file, context, influxdb_url, time=None):
    now = time or datetime.datetime.now()
    ts, tr = xunitparser.parse(junit_file)
    test_points = [
        prepare_test_point(now, context, p, 'tests')
        for p in ts
    ]
    build_point = prepare_build_point(now, context, ts, 'builds')
    client = influxdb.InfluxDBClient.from_dsn(influxdb_url, timeout=60)
    client.write_points([build_point])
    client.write_points(test_points)
