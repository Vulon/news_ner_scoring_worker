import argparse


def parse_cli_arguments(cfg):
    parser = argparse.ArgumentParser(description="Scheduled mediastack news donwloader")

    parser.add_argument('-pi', '--project_id', type=str, help='A project id for BigQuery table', default=cfg.project_id)
    parser.add_argument('-di', '--dataset_id', type=str, help='A dataset id for BigQuery table', default=cfg.dataset_id)
    parser.add_argument('-iti', '--input_table_id', type=str, help='An input table name for BigQuery', default=cfg.input_table_id)
    parser.add_argument('-oti', '--output_table_id', type=str, help='An input table name for BigQuery', default=cfg.output_table_id)
    parser.add_argument('-pt', '--publish_topic', type=str, help='A name of the topic to publish to', default=cfg.output_topic)
    parser.add_argument('-irl', '--input_rows_limit', type=str, help='A name of the topic to publish to', default=cfg.input_rows_limit)
    parser.add_argument('-nsu', '--ner_service_url', type=str, help='URL for the NER service backend', default=cfg.ner_service_url)
    parser.add_argument('-et', '--execution_timeout', type=float, help='Maximum execution time limit.', default=cfg.execution_timeout)

    cli_args = parser.parse_args()
    return cli_args