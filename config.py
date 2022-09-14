import os
import datetime
import great_expectations as ge


# get data context from yaml file
context = ge.data_context.DataContext()

# Location for storing data files
# Base directory to be obtained from the command line argument later
base_dir_path = r"..\landing"

# Setting logging configurations
LOGDIR = r"./logs/"
LOGFILENAME_PREFIX = 'GE-LogFile'

x = datetime.datetime.now()
dt_str = f"""{x.strftime("%Y")}{x.strftime("%m")}{x.strftime("%d")}"""
LOGFILENAME = f"""{LOGDIR}{LOGFILENAME_PREFIX}-{dt_str}.log"""

LOG_LEVEL = os.environ['GE_LOGLEVEL']
