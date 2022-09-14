from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.core.batch import BatchRequest


# def inf_batch_request_sql(ds_name, da_name, query_str):
def inf_batch_request_sql(ds_name, da_name):
    # setting SQL batch request for inferred data connector
    ibr_sql = BatchRequest(
        datasource_name=ds_name,
        data_connector_name="default_inferred_data_connector_name",
        data_asset_name=da_name
        # data_connector_query={"query": query_str}
    )

    return ibr_sql


def rt_batch_request_sql(ds_name, da_name, qry_str):
    # setting SQL batch request for runtime data connector
    rbr_sql = RuntimeBatchRequest(
        datasource_name=ds_name,
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name=da_name,
        runtime_parameters={"query": qry_str},
        batch_identifiers={"default_identifier_name": "default_identifier"},
    )

    return rbr_sql


def inf_batch_request_pandas(ds_name, da_name):
    # setting Pandas batch request for inferred data connector
    ibr_pandas = BatchRequest(
        datasource_name=ds_name,
        data_connector_name="default_inferred_data_connector_name",
        data_asset_name=da_name,
        data_connector_query={"index": -1}
    )

    return ibr_pandas


# create batch for runtime environment
def rt_batch_request_pandas(ds_name, da_name, data_file):
    rbr_pandas = RuntimeBatchRequest(
        datasource_name=ds_name,
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name=da_name,
        runtime_parameters={
            # Choosing either path or dataframe option
            "path": data_file
            # "batch_data": df_school
        },
        batch_identifiers={
            "default_identifier_name": "default_identifier"
        },
    )
    return rbr_pandas


