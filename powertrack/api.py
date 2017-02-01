import ConfigParser
import json
import requests
import urlparse

from powertrack.historical_api import JobManager as HistoricalAPIJobManager
from powertrack.search_api import JobManager as SearchAPIJobManager


HISTORICAL_API = "historical"
SEARCH_API = "search"

config = ConfigParser.RawConfigParser()
config.read("powertrack.conf")


class PowerTrack(object):
    def __init__(self, api=HISTORICAL_API):
        self.api = api

        self.account_name = config.get('credentials', 'account_name')
        self.username = config.get('credentials', 'username')
        self.password = config.get('credentials', 'password')
        self.label = config.get('credentials', 'label')
        self.folder = config.get('output', 'folder')
        self.num_threads = config.get('output', 'num_threads')

        if self.api == HISTORICAL_API:
            self.powertrack_root_url = "https://gnip-api.gnip.com/"
            self.jobs = HistoricalAPIJobManager(self)
        elif self.api == SEARCH_API:
            self.powertrack_root_url = "https://gnip-api.twitter.com/"
            self.jobs = SearchAPIJobManager(self)

    def build_url(self, path):
        """
        Get full URL from relative path
        :param path: Relative path
        :return:
        """
        return urlparse.urljoin(self.powertrack_root_url, path)

    def get(self, path):
        """
        GET request to a relative path with basic authentication
        :param path: Relative path
        :return: Response
        """
        url = self.build_url(path)
        return requests.get(url, auth=(self.username, self.password))

    def post(self, path, data):
        """
        POST request to a relative path with basic authentication
        :param path: Relative path
        :param data: POST body data
        :return: Response
        """
        url = self.build_url(path)
        return requests.post(url, data=json.dumps(data), auth=(self.username, self.password), headers={'Content-Type': 'application/json'})

    def put(self, path, data):
        """
        PUT request to a full URL with basic authentication
        :param path: Relative path
        :param data: PUT body data
        :return: Response
        """
        url = self.build_url(path)
        return requests.put(url, data=json.dumps(data), auth=(self.username, self.password), headers={'Content-Type': 'application/json'})
