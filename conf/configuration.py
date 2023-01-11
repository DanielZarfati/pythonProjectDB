import requests
from urllib3.exceptions import InsecureRequestWarning

conn_values = {}


def initialize():
    filename = 'conf/configuration.conf'
    with open(filename) as file:
        for line in file:
            parameter, value = line.strip().split('=', 1)
            conn_values[parameter] = value


def param_initialize(filename):

    with open(filename) as file:
        for line in file:
            parameter, value = line.strip().split('=', 1)