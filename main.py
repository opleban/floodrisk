import requests
import yaml
import csv
from pprint import pprint
from ratelimit import limits, sleep_and_retry
import time 

RATE_PERIOD = 60
RATE_LIMIT = 60

class FirstStreetRequest:
    def __init__(self):
        config = self.load_config()
        self.api_key = config['api_key']
        self.base_url = config['base_url']
        if "input_file" in config:
            self.input_file = config['input_file']

    def load_config(self):
        _config = {}
        try:
            with open('config.yml', 'r') as ymlfile:
                _config = yaml.load(ymlfile, Loader=yaml.FullLoader)
        except Exception as e:
            print("Missing configuration file")
            print(e)
            exit()
        return _config

    def read_input(self):
        return (line for line in csv.DictReader(open(self.input_file)))

        # with open(self['input_file'], mode='r') as csv_file:
        #     csv_reader = csv.DictReader(csv_file)
        #     for row in csv_reader:
        #         yield row
        

    def auth_headers(self):
        return { "Authorization": f"Bearer {self.api_key}" }

    def check_api(config):
        res = self.make_request(f"{base_url}/apikey/v0.1/check")
        pprint(res.text)

    @sleep_and_retry
    @limits(calls=RATE_LIMIT, period=RATE_PERIOD)
    def make_request(self, url, params=None):
        print(f"Making request: {url} with params: {params}")
        headers = self.auth_headers()
        if params:
            res = requests.request("GET", url, headers=headers, params=params)
        else:
            res = requests.request("GET", url, headers=headers)
        return res

    def get_data_for_city_base(self, service, city, state):
        # construct our end point for market value
        service_url = f"{self.base_url}/data/v0.1/{service}/city"
        # create our query parameters passing in city and state
        query_params = {"address": f"{city}, {state}"}
        res = self.make_request(service_url, query_params)
        # if res.status_code != 200:
        #     raise Exception('API response: {}'.format(res.status_code))
        # pprint(res.json())
        return res

    def get_risk_summary_for_city(self, city, state):
        return self.get_data_for_city_base("summary", city, state)

    def get_market_value_for_city(self, city, state):
        return self.get_data_for_city_base("market-value-impact", city, state)

    def get_tidal_value_for_city(self, city, state):
        return self.get_data_for_city_base("tidal", city, state)

    def get_tidal_value_for_city(self, city, state):
        return self.get_data_for_city_base("hurrican", city, state)

    def get_data_for_city(self, city, state):
        return self.get_risk_summary_for_city(city, state)
    # get_market_value_for_city(config, city, state)


def run_main():
    first_street_requester = FirstStreetRequest()

    request_count = 0
    success = 0
    fail = 0
    for input_row in first_street_requester.read_input():
        city = input_row['city']
        state = input_row['state']
        print(f"{city}, {state}")
        res = first_street_requester.get_data_for_city(city, state)
        print(res.status_code)
        if res.status_code == 200:
            success += 1
        else:
            fail += 1
        request_count += 1

    print(f"Total Request Count: {request_count}")
    print(f"Success Count: {success}")
    print(f"Fail Count: {fail}")

t0 = time.time()
run_main()
t1 = time.time()

print(f"Runtime in (secs): {t1}-{t0}")
    
# call flood risk api
    # by State or by Zip

# parse/transform the data

# write the data to csv
    # store data either by state or by zip or all together
