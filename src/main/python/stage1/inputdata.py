from smv import *
from smv.dqm import *

class Employment(SmvCsvFile):
    def path(self):
        return "employment/CB1200CZ11.csv"

    def failAtParsingError(self):
        return False

    def dqm(self):
        """An example DQM policy"""
        return SmvDQM().add(FailParserCountPolicy(10))
