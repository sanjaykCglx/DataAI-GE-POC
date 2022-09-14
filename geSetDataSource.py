from ruamel import yaml
import logging
from config import *

def set_datasource(ds_type, ds_name, da_name, bd_path):
    if ds_type == 'Pandas':
        logging.info('Pandas engine')
        ds_yaml = f"""
        name: {ds_name}
        module_name: great_expectations.datasource
        class_name: Datasource
        execution_engine:
          class_name: PandasExecutionEngine
          module_name: great_expectations.execution_engine
        data_connectors:
          default_inferred_data_connector_name:
            module_name: great_expectations.datasource.data_connector
            class_name: InferredAssetFilesystemDataConnector
            default_regex:
              group_names:
                 - data_asset_name
              pattern: (.*)
            base_directory: {bd_path}
          default_runtime_data_connector_name:
            module_name: great_expectations.datasource.data_connector
            class_name: RuntimeDataConnector
            batch_identifiers:
                - default_identifier_name
        """
    elif ds_type == 'Sql':
        logging.info('SQL engine')

        # Getting config variables through context
        c_drivername = context.config_variables['my_postgres_db_yaml_creds'].get('drivername')
        c_host = context.config_variables['my_postgres_db_yaml_creds'].get('host')
        c_port = context.config_variables['my_postgres_db_yaml_creds'].get('port')
        c_username = context.config_variables['my_postgres_db_yaml_creds'].get('username')
        c_password = context.config_variables['my_postgres_db_yaml_creds'].get('password')
        c_database = context.config_variables['my_postgres_db_yaml_creds'].get('database')

        # Forming connection string
        conn_str = f"""{c_drivername}+psycopg2://{c_username}:{c_password}@{c_host}:{c_port}/{c_database}"""
        logging.debug(conn_str)

        ds_yaml = f"""
        name: my_postgres_datasource
        class_name: Datasource
        execution_engine:
          class_name: SqlAlchemyExecutionEngine
          connection_string: {conn_str}
        data_connectors:
           default_runtime_data_connector_name:
               class_name: RuntimeDataConnector
               batch_identifiers:
                   - default_identifier_name
           default_inferred_data_connector_name:
               class_name: InferredAssetSqlDataConnector
               include_schema_name: true
        """
        # connection_string: postgresql+psycopg2://<USERNAME>:<PASSWORD>@<HOST>:<PORT>/<DATABASE>
        # connection_string: postgresql + psycopg2: // postgres: admin @ localhost:5432 / postgres
        # credentials: {my_postgres_db_yaml_creds}

    elif ds_type == 'Spark':
        # to be added
        logging.info('Spark engine')
    else:
        # Return invalid ds_type
        logging.info('Invalid engine')

    logging.debug('Printing DataSource Yaml')
    logging.debug(ds_yaml)

    # Debug code - Testing Yaml config
    logging.debug('Testing New DataSource Yaml')
    # context.test_yaml_config(yaml_config=ds_yaml)

    logging.info('Adding new datasource')
    ds_dict = yaml.safe_load(ds_yaml)
    new_ds = context.add_datasource(**ds_dict)
