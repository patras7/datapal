from enum import Enum


class OracleEbsDataDictionaryTableName(str, Enum):
    """
    Oracle Data Dictionary Tables
    """

    FND_TABLES = "FND_TABLES"
    FND_COLUMNS = "FND_COLUMNS"
    FND_PRIMARY_KEYS = "FND_PRIMARY_KEYS"
    FND_PRIMARY_KEY_COLUMNS = "FND_PRIMARY_KEY_COLUMNS"
    FND_FOREIGN_KEYS = "FND_FOREIGN_KEYS"
    FND_FOREIGN_KEY_COLUMNS = "FND_FOREIGN_KEY_COLUMNS"
    ALL_TABLES = "ALL_TABLES"


class OracleEbsFieldNameAlias(str, Enum):
    """
    Enumerate Oracle Field Alias
    """

    application_id = "APPLICATION_ID"
    column_name = "COLUMN_NAME"
    column_id = "COLUMN_ID"
    table_name = "TABLE_NAME"
    table_id = "TABLE_ID"
    data_type = "DATA_TYPE"
    description = "DESCRIPTION"
    constraint_name = "CONSTRAINT_NAME"
    r_constraint_name = "R_CONSTRAINT_NAME"
    owner = "OWNER"
    position = "POSITION"
    constraint_type = "CONSTRAINT_TYPE"
    column_type = "COLUMN_TYPE"
    position_of_the_field_in_the_table = "COLUMN_ID"
    primary_key_table_id = "PRIMARY_KEY_TABLE_ID"
    primary_key_id = "PRIMARY_KEY_ID"
    foreign_key_id = "FOREIGN_KEY_ID"
    audit_key_flag = "AUDIT_KEY_FLAG"
    primary_key_type = "PRIMARY_KEY_TYPE"


class OracleEbsObjectAlias(str, Enum):
    """
    Enumerate Preprocessor Dataset Field Aliases
    """

    owner = "owner"
    object_name = "object_name"
    domain_name = "domain_name"
    primary_key_id = "primary_key_id"
    primary_column_id = "primary_column_id"
    primary_column_name = "primary_column_name"
    primary_object_raw_fields = "primary_object_raw_fields"
    primary_key_raw_fields = "primary_key_raw_fields"
    foreign_object_name = "foreign_object_name"
    foreign_key_id = "foreign_key_id"
    foreign_column_id = "foreign_column_id"
    field_description = "field_description"
    description = "description"
    text_table_names = "text_table_names"
    foreign_object_raw_fields = "foreign_object_raw_fields"
    link_description = "link_description"
    field_name = "field_name"
    raw_field_name = "raw_field_name"
    field_type = "field_type"
    field_literal = "field_literal"
    foreign_column_name = "foreign_column_name"
    primary_object_name = "primary_object_name"
