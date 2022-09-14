import logging.handlers

from geBatchRequests import *
from geUtils import *
from geSetDataSource import *
from geSetCheckpoint import *
from config import *

# set data context
# configure datasource
# create expectation suite
# create checkpoint
# validate new batch of data
# customize deployment


# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    logging.info(f"Starting GE process")

    # datasource name to be obtained from the command line argument later
    datasource_name = 'gepoc_pandas_datasource'

    # data asset name to be obtained from the command line argument later
    data_asset_name = 'School.csv'

    # expectation suite name to be obtained from the command line argument later
    exp_suite_name = 'school_expectations-1'

    # checkpoint name to be obtained from the command line argument later
    checkpoint_name = 'gepoc_checkpoint'

    # Create or update the datasource
    set_datasource('Pandas', datasource_name, data_asset_name, base_dir_path)
    set_datasource('Sql', 'my_postgres_datasource', 'StudentData', base_dir_path)

    # create or update inferred checkpoint for Pandas Datasource
    ibr_pandas = inf_batch_request_pandas(datasource_name, data_asset_name)
    set_checkpoint(checkpoint_name, ibr_pandas, exp_suite_name)

    # create or update inferred checkpoint for PG-SQL Datasource
    ibr_sql = inf_batch_request_sql('my_postgres_datasource', 'public.StudentData')
    set_checkpoint('db_checkpoint', ibr_sql , 'StudentDataExp')

    # run checkpoints
    logging.info('Running the 1st checkpoint - Pandas')
    validation_results = context.run_checkpoint(checkpoint_name=checkpoint_name)
    parse_val_results(validation_results)

    logging.info('Running the 2nd checkpoint - Sql')
    validation_results = context.run_checkpoint(checkpoint_name='db_checkpoint')
    parse_val_results(validation_results)

    # Create runtime batch data dataframe or pass the datafile path obtained from command line
    data_file =  "./landing/School.csv"

    # Create Validator and invoke validate function
    logging.info('Running the 3rd checkpoint - Runtime - Pandas')
    rbr_pandas = rt_batch_request_pandas(datasource_name, 'SchoolData', data_file)
    validator = context.get_validator(batch_request=rbr_pandas, expectation_suite_name=exp_suite_name)
    validation_results_rt = validator.validate(expectation_suite=None)
    # Parse validation results
    parse_val_results(validation_results_rt)

    # Create Validator and invoke validate function
    logging.info('Running the 4th checkpoint - Runtime - Sql')
    qry_str = r'''Select * from public."StudentData"'''
    rbr_sql = rt_batch_request_sql('my_postgres_datasource', 'StudentData', qry_str)
    validator = context.get_validator(batch_request=rbr_sql, expectation_suite_name='StudentDataExp')
    validation_results_rt = validator.validate(expectation_suite=None)
    # Print validation results
    parse_val_results(validation_results_rt)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
