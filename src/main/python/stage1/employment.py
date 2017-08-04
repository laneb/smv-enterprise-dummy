###---GLOBAL_IMPORTS_START---###
from smv import *
from pyspark.sql.functions import *
###---GLOBAL_IMPORTS_END---###
###---EmploymentByState_IMPORTS_START---###
import stage1.inputdata


###---EmploymentByState_IMPORTS_END---###
from smv import *
from pyspark.sql.functions import col, sum, lit

from stage1 import inputdata

__all__ = ['EmploymentByState']

class EmploymentByState(SmvModule, SmvOutput):
    """
    Python ETL Example: employ by state
    """

    def requiresDS(self):
        return [stage1.inputdata.Employment]

    def run(self, i):
        df = i[inputdata.Employment]
        return df.groupBy(col("ST")).agg(sum(col("EMP")).alias("mnop"))
    def isEphemeral(self):
        return False
