from ruamel import yaml
import logging
from config import *

def set_checkpoint(cp_name, batch_request, es_name):
    # setting checkpoint for inferred data connector

    yaml_config = f"""
    name: {cp_name}
    config_version: 1.0
    class_name: SimpleCheckpoint
    run_name_template: "%Y%m%d-%H%M%S-my-run-name-template"
    validations: 
        - batch_request: {batch_request}
          expectation_suite_name: {es_name}
          action_list:
          - name: store_validation_result
            action:
              class_name: StoreValidationResultAction
          - name: update_data_docs
            action:
              class_name: UpdateDataDocsAction
    """

    logging.debug('Printing yaml configuration for setting checkpoint')
    logging.debug(yaml_config)

    # Debug code - Testing Yaml config
    # print_debug('Testing checkpoint yaml configuration')
    # my_checkpoint = context.test_yaml_config(yaml_config=yaml_config)

    # Debug code - Printing Checkpoint config
    # logging.debug('Printing checkpoint')
    # logging.debug(my_checkpoint.get_config(mode="yaml"))

    # Add checkpoint to data context
    logging.info('Adding checkpoint to context')
    context.add_checkpoint(**yaml.safe_load(yaml_config))


