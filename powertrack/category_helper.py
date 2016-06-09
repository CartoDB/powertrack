import os
from datetime import datetime

from powertrack.api import SEARCH_API, PowerTrack


SEARCH_API_MAX_POSITIVE_CLAUSES = 28  # GNIP's max is 30 and we need 2 to enforce geo availability


class Category(object):
    def __init__(self, name, terms=None):
        self.name = name
        self.terms = terms

    @property
    def query(self):
        return " OR ".join(self.terms)


class Job(object):
    def __init__(self, name):
        self.name = name
        self.categories = []
        self.category_numbers = {}

    def add_category(self, category):
        self.categories.append(category)
        self.category_numbers[category.name] = len(self.categories)

    def create_category(self, name, terms=None):
        new_category = Category(name, terms)
        self.categories.append(new_category)
        self.category_numbers[new_category.name] = len(self.categories)
        return new_category

    def get_queryset(self, api):
        queryset = []

        if api == SEARCH_API:
            clauses_in_current_query = 0
            query = ''
            for category in self.categories:
                if (clauses_in_current_query + len(category.terms)) > SEARCH_API_MAX_POSITIVE_CLAUSES:
                    queryset.append(query[4:])
                    clauses_in_current_query = 0
                    query = ''
                query += " OR " + category.query
                clauses_in_current_query += len(category.terms)
            queryset.append(query[4:])

        return queryset

    def find_category(self, csv_tweet):
        for category in self.categories:
            for term in category.terms:
                if term.lower() in csv_tweet.lower():
                    return str(self.category_numbers[category.name]), category.name

        return '', ''

    def run(self, start, end=None, title=None, geo_enrichment=False, columns=None, api=SEARCH_API):
        title = title or self.name

        if end is None:
            end = datetime.utcnow()

        pt = PowerTrack(api=api)

        for i, query in enumerate(self.get_queryset(api)):
            new_job = pt.jobs.create(start, end, title + "_tmp", query, geo_enrichment, columns)
            new_job.export_tweets(append=False if i == 0 else True)

        with open(os.path.join(pt.folder, title + "_tmp.csv")) as tmp_file:
            with open(os.path.join(pt.folder, title + ".csv"), "w") as final_file:
                for i, csv_tweet in enumerate(tmp_file):
                    if i == 0:
                        csv_tweet = csv_tweet.rstrip() + ",category_number,category_name\n"
                    else:
                        category_number, category_name = self.find_category(csv_tweet)
                        csv_tweet = csv_tweet.rstrip() + ",{category_number},{category_name}\n".format(category_number=category_number,
                                                                                                       category_name=category_name)
                    final_file.write(csv_tweet)
