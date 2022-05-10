from google.cloud import bigquery
import google.cloud.logging
import requests
from config import load_config
from utils.text_utils import clean_text
import nltk
import time
import json
import os
from exceptions.backend_exceptions import NerBackendAccessException, NerBackendInternalException
from utils.cli_utils import parse_cli_arguments
from google.cloud import pubsub_v1
import typing
from big_querry.sql import get_distinct_hashes, get_unprocessed_entries_count, mark_entries_processed, get_input_entries


nltk.download('punkt')


def send_ner_request(url: str, line: str):
    data = {
        "text" : line
    }
    data = json.dumps(data)
    results = requests.post(url, data=data)
    if results.status_code >= 500:
        raise NerBackendInternalException(f"NER Backend {results.status_code} : {str(results.content)} ")
    elif results.status_code >= 400:
        raise NerBackendAccessException(f"NER Backend {results.status_code} : {str(results.content)} ")
    results = results.json()

    return results["prediction"]


def send_batch_ner_request(url: str, lines: typing.List[str], logging) -> typing.List[typing.Dict[str, str]]:
    data = {
        "batch": lines
    }
    data = json.dumps(data)
    results = requests.post(url, data=data)
    if results.status_code >= 500:
        raise NerBackendInternalException(f"NER Backend {results.status_code} : {str(results.content)} ")
    elif results.status_code >= 400:
        raise NerBackendAccessException(f"NER Backend {results.status_code} : {str(results.content)} ")
    results = results.json()

    return results['prediction']


def is_entry_already_processed(entry, hashes_list: typing.Set[typing.Tuple[int, int]]):
    title_hash = entry['title_hash']
    description_hash = entry['description_hash']
    if (title_hash, description_hash) in hashes_list:
        return True
    else:
        return False


def process_prediction( raw_prediction) -> typing.Dict[str, typing.List[str]]:
    ner_tags = {
        "org": [],
        "geo": [],
        "gpe": [],
        "per": [],
    }
    prediction = [item for item in raw_prediction if item['Tag'] != 'O']

    accumulated_token = []
    previous_token_type = ""
    for item in prediction:
        item: dict
        token_tag: str = item['Tag']
        sequence_type = token_tag.split('-')[0]
        token_type = token_tag.split('-')[-1]
        if sequence_type == 'B' and len(accumulated_token) > 0:
            if previous_token_type in ner_tags.keys():
                ner_tags[previous_token_type].append(" ".join(accumulated_token))
            accumulated_token = []
        elif sequence_type == 'I' and token_type != previous_token_type:
            if previous_token_type in ner_tags.keys():
                ner_tags[previous_token_type].append(" ".join(accumulated_token))
            accumulated_token = []

        previous_token_type = token_type
        accumulated_token.append(item['Token'].replace(";", " "))
    if previous_token_type in ner_tags.keys():
        ner_tags[previous_token_type].append(" ".join(accumulated_token))
    return ner_tags


def construct_new_entry(input_entry: bigquery.table.Row, ner_tags: typing.Dict[str, list]) -> dict:
    json_entry = dict([item for item in input_entry.items()])

    json_entry['date'] = input_entry['date'].strftime('%Y-%m-%d %H:%M:%S.%f')
    json_entry['processed_flag'] = False
    json_entry['ner_org_list'] = json.dumps(ner_tags['org'])
    json_entry['ner_geo_list'] = json.dumps(ner_tags['geo'])
    json_entry['ner_gpe_list'] = json.dumps(ner_tags['gpe'])
    json_entry['ner_per_list'] = json.dumps(ner_tags['per'])
    return json_entry


def insert_big_query_new_rows(bq_client: bigquery.Client, full_output_table_name: str, entries_to_insert: typing.List[ typing.Dict[str, list] ], logging ) -> typing.List[typing.Dict[str, list]]:
    errors = bq_client.insert_rows_json(full_output_table_name, entries_to_insert)
    logging.info(f"Writing {len(entries_to_insert)} rows to table")
    error_indices = [e['index'] for e in errors]
    for e in errors:
        logging.error(e)
    inserted_entries = [item for j, item in enumerate(entries_to_insert) if j not in error_indices]
    return inserted_entries


def mark_inserted_entries_as_processed(bq_client: bigquery.Client, full_input_table_name: str,
                                       inserted_entries: typing.List[typing.Dict[str, list]], logging, distinct_hashes):
    title_hashes = [item['title_hash'] for item in inserted_entries]
    description_hashes = [item['description_hash'] for item in inserted_entries]
    logging.info(f"Inserted {len(inserted_entries)} rows")
    errors, distinct_hashes = mark_entries_processed(bq_client, full_input_table_name, title_hashes, description_hashes, distinct_hashes)
    if len(errors) > 0:
        logging.error(f"Could not update {len(errors)} rows in the input table")
    for e in errors:
        logging.error(e)
    return distinct_hashes



def main(*args, **kwargs):
    import os
    start = time.time()
    nltk.download('punkt')
    gc_logger = google.cloud.logging.Client()
    gc_logger.setup_logging()
    import logging
    bq_client = bigquery.Client()
    cfg = load_config()
    cli_args = parse_cli_arguments(cfg)

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(cli_args.project_id, cli_args.publish_topic)

    full_input_table_name = "{}.{}.{}".format(cli_args.project_id, cli_args.dataset_id, cli_args.input_table_id)
    full_output_table_name = "{}.{}.{}".format(cli_args.project_id, cli_args.dataset_id, cli_args.output_table_id)
    distinct_hashes = get_distinct_hashes(bq_client, full_output_table_name)
    unprocessed_count = get_unprocessed_entries_count(bq_client, full_input_table_name)

    logging.info(f"Function start. unprocessed entries count: {unprocessed_count}")

    retries = 0
    while unprocessed_count > 0:
        if time.time() - start > cli_args.execution_timeout:
            logging.warning(f"Execution stopped after {time.time() - start} seconds. Unprocessed count: {unprocessed_count}")
            break
        input_entries = get_input_entries(bq_client, full_input_table_name, cli_args.input_rows_limit)

        entries_text_list = []
        entries_indices_list = []

        for i, entry in enumerate(input_entries):
            entry: bigquery.table.Row
            try:
                if is_entry_already_processed(entry, distinct_hashes):
                    mark_entries_processed(bq_client, full_input_table_name, [entry['title_hash']], [entry['description_hash']], distinct_hashes)
                    continue
                title = clean_text(entry['title'])
                description = clean_text(entry['description'])
                text = title + " . " + description
                entries_text_list.append(text)
                entries_indices_list.append(i)
            except Exception as e:
                logging.exception(e)

        entries_to_insert = []
        if len(entries_text_list) > 0:
            try:
                batch_results = send_batch_ner_request(cli_args.ner_service_url, entries_text_list, logging)

                for index, prediction in zip(entries_indices_list, batch_results):
                    ner_tags = process_prediction(prediction)
                    json_entry = construct_new_entry(input_entries[index], ner_tags)
                    entries_to_insert.append(json_entry)
            except Exception as e:
                logging.exception(e)

            if len(entries_to_insert) < 1:
                logging.warning("No entries to insert")
            else:
                inserted_entries = insert_big_query_new_rows(bq_client, full_output_table_name, entries_to_insert, logging)
                distinct_hashes = mark_inserted_entries_as_processed(bq_client, full_input_table_name, inserted_entries, logging, distinct_hashes)

        new_unprocessed_count = get_unprocessed_entries_count(bq_client, full_input_table_name)
        if new_unprocessed_count == unprocessed_count:
            retries += 1
            logging.warning(f"Unprocessed entries count did not decrease retries: {retries}")
        else:
            retries = 0
        unprocessed_count = new_unprocessed_count
        if retries > 2:
            logging.error("Retries count exceeded the limit. Stopping")
            unprocessed_count = 0

    future = publisher.publish(topic_path, b'NER enriched')
    result = future.result()



if __name__ == "__main__":
    import os
    config = load_config()
    project_root = os.path.dirname(os.path.abspath(__file__))
    credentials_path = os.path.join(project_root, config.google_api_key_path)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
    main()
