from ruamel import yaml
import logging
from config import *

logging.basicConfig(filename=f'{LOGFILENAME}',
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    level=LOG_LEVEL,
                    )


def parse_val_summary(vr):
    logging.info("Batch Request: Validation Result Summary: ")
    # logging.info(f"Evaluated Expectations: {vr.get('statistics')}")
    logging.info(f"\tValidation Result: {vr.get('success')}")
    logging.info(f"\tEvaluated Expectations: {vr['statistics'].get('evaluated_expectations')}")
    logging.info(f"\tSuccessful Expectations: {vr['statistics'].get('successful_expectations')}")
    logging.info(f"\tUnsuccessful Expectations: {vr['statistics'].get('unsuccessful_expectations')}")
    logging.info(f"\tSuccess %: {vr['statistics'].get('success_percent')}")


def parse_val_details(vr):
    logging.info("Batch Request: Validation Result Details: ")
    # Iterate through each result of each expectation
    for i in range(len(vr['results'])):
        # Log expectation number & name
        logging.info(f"Rule No: {(i + 1)} :  Expectation: {vr['results'][i]['expectation_config']['_expectation_type']}")
        # logging.info(f" Expectation Args: {vr['results'][4]['expectation_config']['_kwargs']}")

        # Log expectation result - Success / Failure
        logging.info(f"    Success: {vr['results'][i]['success']} ")

        # Iterate further through the keys of result of each expectation & log the result
        for j in vr['results'][i]['result'].keys():
            logging.info(f"     {j}: {vr['results'][i]['result'][j]}")


def parse_val_results(vr):

    # logging.info("Batch Request: Validation Summary: ")
    # logging.info(f"Validation Result: {vr.get('success')}")

    # Parse the validation result string differently for checkpoint and validator method
    if not vr.get('statistics') is None:
        # Checkpoint result string cane directly parsed
        parse_val_summary(vr)
        parse_val_details(vr)
    else:
        # Validator result string needs to be iterated through various nested keys
        for key in vr:
            # print(f"{i}: {key}")
            # 1st level key-values - check for value of '_run_results' key
            if key == '_run_results':
                # print(f"""in {key} :""")
                # 2nd level key-values
                for jkey, jval in vr[key].items():
                    # print(f"HELLO {jkey} IS: {jval}")
                    # 3rd level key-Values -  - get value of 'validation_result' key
                    for kkey, kval in jval.items():
                        # print(f"HELLO {kkey} IS: {kval}")
                        if kkey == 'validation_result':
                            # print('\n\t\t\tIn Stats Key\n')
                            # Invoke parsing functions to parse overall statistics & detailed results for each expectation
                            parse_val_summary(kval)
                            parse_val_details(kval)
                            return

