from google.cloud import bigquery
import google.cloud.logging
import requests
from config import load_config
from utils.text_utils import clean_text
import nltk
import json
from utils.bigquery_utils import get_unprocessed_entries_count, mark_entries_processed
from utils.cli_utils import parse_cli_arguments


def send_ner_request(url: str, line: str):
    data = {
        "text" : line
    }
    data = json.dumps(data)
    results = requests.post(url, data=data).json()
    return results["prediction"]


def main(*args, **kwargs):
    gc_logger = google.cloud.logging.Client()
    gc_logger.setup_logging()
    import logging
    bq_client = bigquery.Client()
    cfg = load_config()
    cli_args = parse_cli_arguments(cfg)
    full_input_table_name = "{}.{}.{}".format(cli_args.project_id, cli_args.dataset_id, cli_args.input_table_id)
    full_output_table_name = "{}.{}.{}".format(cli_args.project_id, cli_args.dataset_id, cli_args.output_table_id)

    query_job = bq_client.query(
        f"""
        SELECT distinct 
        title_hash, description_hash
        FROM `{full_output_table_name}`"""
    )
    distinct_hashes = query_job.result()
    distinct_hashes = { (row.title_hash, row.description_hash) for row in distinct_hashes }

    unprocessed_count = get_unprocessed_entries_count(bq_client, full_input_table_name)
    logging.info(f"Function start. unprocessed entries count: {unprocessed_count}")

    retries = 0
    while unprocessed_count > 0:
        input_query = bq_client.query(
            f"""
            SELECT distinct *
            FROM {full_input_table_name}
            WHERE processed_flag = false
            LIMIT {cli_args.input_rows_limit}
            """
        )
        input_entries = input_query.result()

        entries_to_insert = []
        for i, entry in enumerate(input_entries):
            entry: bigquery.table.Row
            try:
                title = entry['title']
                description = entry['description']
                title_hash = entry['title_hash']
                description_hash = entry['description_hash']
                if (title_hash, description_hash) in distinct_hashes:
                    mark_entries_processed(bq_client, full_input_table_name, [title_hash], [description_hash])
                    continue
                title = clean_text(title)
                description = clean_text(description)
                title_lines = nltk.sent_tokenize(title)
                description_lines = nltk.sent_tokenize(description)
                ner_tags = {
                    "org" : [],
                    "geo" : [],
                    "gpe" : [],
                    "per" : [],
                }
                lines = title_lines + description_lines
                for line in lines:
                    prediction = send_ner_request(cli_args.ner_service_url, line)
                    prediction = [ item for item in prediction if item['Tag'] != 'O' ]
                    accumulated_token = []
                    previous_token_type = ""
                    for item in prediction:
                        item: dict
                        token_tag: str = item['Tag']
                        sequence_type = token_tag.split('-')[0]
                        token_type = token_tag.split('-')[-1]
                        if sequence_type == 'B' and len(accumulated_token) > 0:
                            if previous_token_type in ner_tags.keys():
                                ner_tags[previous_token_type].append( " ".join(accumulated_token) )
                            accumulated_token = []
                        elif sequence_type == 'I' and token_type != previous_token_type:
                            if previous_token_type in ner_tags.keys():
                                ner_tags[previous_token_type].append(" ".join(accumulated_token))
                            accumulated_token = []

                        previous_token_type = token_type
                        accumulated_token.append(item['Token'].replace(";", " "))
                    if previous_token_type in ner_tags.keys():
                        ner_tags[previous_token_type].append(" ".join(accumulated_token))
                json_entry = dict([item for item in entry.items()])
                json_entry['date'] = entry['date'].strftime('%Y-%m-%d %H:%M:%S.%f')
                json_entry['processed_flag'] = False
                json_entry['ner_org_list'] = json.dumps(ner_tags['org'])
                json_entry['ner_geo_list'] = json.dumps(ner_tags['geo'])
                json_entry['ner_gpe_list'] = json.dumps(ner_tags['gpe'])
                json_entry['ner_per_list'] = json.dumps(ner_tags['per'])

                entries_to_insert.append(json_entry)
            except Exception as e:
                logging.exception(e)

        if len(entries_to_insert) < 1:
            logging.warning("No entries to insert")
        else:
            errors = bq_client.insert_rows_json(full_output_table_name, entries_to_insert)
            logging.info(f"Writing {len(entries_to_insert)} rows to table")
            error_indices = [e['index'] for e in errors]
            for e in errors:
                logging.error(e)
            inserted_entries = [ item for j, item in enumerate(entries_to_insert) if j not in error_indices ]
            title_hashes = [item['title_hash'] for item in inserted_entries]
            description_hashes = [item['description_hash'] for item in inserted_entries]
            logging.info(f"Inserted { len(inserted_entries) } rows")
            errors = mark_entries_processed(bq_client, full_input_table_name, title_hashes, description_hashes)
            if len(errors) > 0:
                logging.error(f"Could not update { len(errors) } rows in the input table")
            for e in errors:
                logging.error(e)

        new_unprocessed_count = get_unprocessed_entries_count(bq_client, full_input_table_name)
        if new_unprocessed_count == unprocessed_count:
            retries += 1
            logging.warning(f"Unprocessed entries count did not decrease retries: {retries}")
        else:
            retries = 0
        unprocessed_count = new_unprocessed_count
        if retries > 3:
            logging.error("Retries count exceeded the limit. Stopping")
            unprocessed_count = 0

    # publish message


if __name__ == "__main__":
    import os
    config = load_config()
    project_root = os.path.dirname(os.path.abspath(__file__))
    credentials_path = os.path.join(project_root, config.google_api_key_path)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
    main()
