import os
import warnings
from datetime import datetime

from powertrack.api import SEARCH_API, PowerTrack


SEARCH_API_MAX_CLAUSE_LENGTH = 128
SEARCH_API_MAX_RULE_LENGTH = 2048


class CategoryException(Exception):
    pass


class Category(object):
    def __init__(self, name, terms=None):
        """
        Category constructor
        :param name: Category name
        :param terms: Array with all the terms that fit the category, such as ["#lakers", "@lakers"]. Each term will be translated into a
                      positive Powertrack clause, and will be OR'ed with the rest
        :return:
        """
        self.name = name
        self.terms = terms

        for term in self.terms:
            if len(self.term) > SEARCH_API_MAX_CLAUSE_LENGTH:
                warnings.warn("{term} exceeds Search API length for a single positive clause".format(term=term))

        if len("({value}) (has:geo)".format(query=self.rule)) > SEARCH_API_MAX_RULE_LENGTH:
            warnings.warn("Category definition exceeds the length of a single Search API request")

    @property
    def rule(self):
        """
        Get a Powertrack-compatible rule for the category
        :return: OR'ed string of terms
        """
        return " OR ".join(self.terms)


class Job(object):
    def __init__(self, name):
        """
        Job constructor
        :param name: Job name
        :return:
        """
        self.name = name
        self.categories = []
        self.category_numbers = {}

    def add_category(self, category):
        """
        Add a category object to the internal category list
        :param category: Category to be added
        :return:
        """
        self.categories.append(category)
        self.category_numbers[category.name] = len(self.categories)

    def create_category(self, name, terms=None):
        """
        Create a new category object from name and terms and add it to the internal category list
        :param name: Category name
        :param terms: Array with all the terms that fit the category, such as ["#lakers", "@lakers"]. Each term will be translated into a
                      positive Powertrack clause, and will be OR'ed with the rest
        :return: Created category
        """
        new_category = Category(name, terms)
        self.categories.append(new_category)
        self.category_numbers[new_category.name] = len(self.categories)
        return new_category

    def get_ruleset(self, api):
        """
        Get the set of rules that will be sent to Powertrack as individual requests.
        Because of the limitation on positive clauses, each rule in the ruleset will keep combining individual category rules as long as the
        positive clause limit is not exceeded.
        :param api: Either SEARCH_API or HISTORICAL_API
        :return: Ruleset as an array of rules, each rule being the combination of one or more individual category rules
        """
        # TODO: add support for the historical API (limits are more relaxed there)
        ruleset = []

        if api == SEARCH_API:
            clauses_in_current_rule = 0
            rule = ''
            for category in self.categories:
                rule += " OR " + category.rule
                clauses_in_current_rule += len(category.terms)
            ruleset.append(rule[4:])  # Remove first " OR "

        if len("({value}) (has:geo)".format(query=ruleset)) > SEARCH_API_MAX_RULE_LENGTH:
            warnings.warn("Category set definition exceeds the length of a single Search API request")

        return ruleset

    def find_category(self, csv_tweet):
        """
        Find which category a tweet belong to
        :param csv_tweet: Tweet to be scanned
        :return: Tuple with category number and name. Number is internal to each Job object, not stored in the category itself
        """
        # TODO: use only the tweet's body for scanning purposes, instead of the whole tweet
        for category in self.categories:
            for term in category.terms:
                if term.lower() in csv_tweet.lower():
                    return str(self.category_numbers[category.name]), category.name

        return '', ''

    def run(self, start, end=None, title=None, columns=None, api=SEARCH_API):
        """
        Run the Powertrack job to fetch the tweets, scan the resulting file so that categories can be assigned and create the final tweet file
        :param start: Start timestamp
        :param end: End timestamp, defaults to now
        :param title: Title to be used as the file name (defaults to Job's name)
        :param columns: Array of columns to be created in CartoDB's table. None for all columns.
        :param api: Either SEARCH_API or HISTORICAL_API
        :return:
        """
        # TODO: test against the historical API
        title = title or self.name

        if end is None:
            end = datetime.utcnow()

        pt = PowerTrack(api=api)

        for i, rule in enumerate(self.get_ruleset(api)):
            new_job = pt.jobs.create(start, end, title + "_tmp", rule, columns)
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
