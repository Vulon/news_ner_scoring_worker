from marshmallow import Schema, fields, post_load


class BaseConfig(Schema):
    def __getattr__(self, item):
        return self.data.get(item)
    project_id = fields.Str(required = True)
    dataset_id = fields.Str(required = True)
    input_table_id = fields.Str(required = True)
    output_table_id = fields.Str(required = True)
    output_topic = fields.Str(required = True)
    input_rows_limit = fields.Int(required = True)
    ner_service_url = fields.Str(required=True)
    google_api_key_path = fields.Str(required=True)
    sub_topic = fields.Str(required=True)
    pub_topic = fields.Str(required=True)
    execution_timeout = fields.Float(required=True)

    @post_load
    def make_config(self, data, **kwargs):
        self.data = data
        return self

def load_config(config_path= "config.json") -> BaseConfig:
    with open( config_path, 'r' ) as file:
        config_str = file.read()
        config = BaseConfig().loads(config_str)

        return config