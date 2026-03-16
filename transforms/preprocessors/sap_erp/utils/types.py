from enum import Enum


class SapDataDictionaryTableName(str, Enum):
    DD02L = "DD02L"
    DD02T = "DD02T"
    DD03L = "DD03L"
    DD04T = "DD04T"
    DD08L = "DD08L"
    DD05S = "DD05S"


class SpecialSapFieldName(str, Enum):
    MANDT = "MANDT"  # SAP client key
    SPRAS = "SPRAS"  # SAP language key


class ConnectorFieldPrefixes(str, Enum):
    SAP_PALANTIR = "/PALANTIR/"  # Connector timestamp field


class SapFieldNameAlias(str, Enum):
    check_table_name = "check_table_name"
    column_name = "column_name"
    data_type_in_abap_dictionary = "data_type_in_abap_dictionary"
    domain_name = "domain_name"
    field_data_element = "field_data_element"
    field_literal = "field_literal"
    field_name = "field_name"
    identifies_a_key_field_of_a_table = "identifies_a_key_field_of_a_table"
    language_key = "language_key"
    long_field_label = "long_field_label"
    metadata = "metadata"
    new_meta_element = "new_meta_element"
    next_position = "next_position"
    position = "position"
    position_of_the_field_in_the_table = "position_of_the_field_in_the_table"
    segment_by = "segment_by"
    short_field_label = "short_field_label"
    table_name = "table_name"
