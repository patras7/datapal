import re
import json

"""
Defines all the util functions needed to create the Infor DD tables
Raw data can be ingested in JSON format from the Infor Data Catalog Rest API
{{INFOR_ION_URL}}/datacatalog/v1/object/fetch /datacatalog/v1/object/fetch
"""


def get_column_description(column_json, key):
    # This is needed due to special formatting of table LN_Enum
    if "description" in dict(column_json).keys():
        return column_json["description"]
    else:
        return key


def get_column_type(column_json):
    """
    Return the column type
    - For most of the columns, the type is given by the "type"
    - For date and timestamp type, the "format" key exists and it specifies the actual type
    """
    if "format" in dict(column_json).keys():
        return column_json["format"]
    else:
        return column_json["type"]


def create_table_object(table_entry):
    d = dict(table_entry["schema"]["properties"])
    table_properties = [
        {
            "column_name": key,
            "column_desc": get_column_description(d[key], key),
            "column_type": get_column_type(d[key]),
        }
        for key in d.keys()
    ]

    pks = [
        re.findall(r"\w+", pk_col)[0]
        for pk_col in table_entry["properties"]["IdentifierPaths"]
    ]

    table_descr = table_entry["schema"]["description"].split(", ")[0]
    regex = re.compile(
        r"(?P<src>[A-Z]+)[\s.](?P<raw_name>\w+)[\s.](?P<full_name>[\s\S]+)"
    )
    table_desc = regex.match(table_descr).groupdict()["full_name"]

    return {
        "object_name": table_entry["name"].lower(),
        "description": table_desc,
        "primary_key_raw_fields": pks,
        "object_properties": table_properties,
    }


def create_infor_tables_df(ctx, raw):
    all_tables = []
    fs = raw.filesystem()
    for filestatus in raw.filesystem().ls():
        with fs.open(filestatus.path) as f:
            tables = [create_table_object(obj) for obj in json.load(f)["objects"]]
            all_tables.extend(tables)

    return ctx.spark_session.createDataFrame(all_tables)
