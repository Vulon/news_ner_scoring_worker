from google.cloud import bigquery
import typing


def get_distinct_hashes(bq_client: bigquery.Client, full_output_table_name: str) -> typing.Set[typing.Tuple[int, int]]:
    query_job = bq_client.query(
        f"""
        SELECT distinct 
        title_hash, description_hash
        FROM `{full_output_table_name}`"""
    )
    distinct_hashes = query_job.result()
    distinct_hashes = { (row.title_hash, row.description_hash) for row in distinct_hashes }
    return distinct_hashes


def get_input_entries(bq_client: bigquery.Client, full_input_table_name: str, input_rows_limit: int) -> typing.List[bigquery.table.Row]:
    input_query = bq_client.query(
        f"""
        SELECT distinct *
        FROM {full_input_table_name}
        WHERE processed_flag = false
        LIMIT {input_rows_limit}
        """
    )
    input_entries = input_query.result()
    return [item for item in input_entries]


def get_unprocessed_entries_count(bq_client, full_table_name) -> int:
    unprocessed_count_query = bq_client.query(
        f"""
        SELECT count(*) as unprocessed_count
        FROM {full_table_name}
        WHERE processed_flag = false
        """
    )
    unprocessed_count = unprocessed_count_query.result()
    unprocessed_count = [row.unprocessed_count for row in unprocessed_count][0]
    return unprocessed_count


def mark_entries_processed(bq_client: bigquery.Client, full_table_name: str, title_hashes: list, description_hashes: list, distinct_hashes: typing.Set[typing.Tuple[int, int]]):
    results = bq_client.query(
        f"""
        UPDATE {full_table_name}
        SET processed_flag = true
        WHERE 
        title_hash in ({ ",".join(map(str, title_hashes)) })
        AND
        description_hash in ({ ",".join(map(str, description_hashes)) })
        """
    ).result()
    results = [item for item in results]
    for title_hash, description_hash in zip(title_hashes, description_hashes):
        distinct_hashes.add( (title_hash, description_hash) )
    return results, distinct_hashes