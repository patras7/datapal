from pyspark.sql import DataFrame
from pyspark.sql import functions as F, types as T
from transforms.verbs import columns as C


# TODO: this doesn't cover the entire set of MSSQL <-> pyspark type mapping
FIELD_TYPE_MAPPINGS = {
    "int": "integer",
    "decimal": "decimal",
    "varchar": "string",
    "image": "string",
    "datetimeoffset": "timestamp",
    "datetime": "timestamp",
    "numeric": "decimal",
    "uniqueidentifier": "string",
    "float": "double",
    "char": "string",
    "nvarchar": "string",
}


def generate_fields_metadata(
    tables: DataFrame,
    columns: DataFrame,
    types: DataFrame,
    default_constraints: DataFrame,
    index_columns: DataFrame,
    indexes: DataFrame,
    foreign_keys: DataFrame,
    foreign_key_columns: DataFrame,
    check_constraints: DataFrame,
    extended_properties: DataFrame,
    computed_columns: DataFrame,
) -> DataFrame:
    tables = tables.select(
        F.col("name").alias("table_name"),
        F.col("object_id").alias("table_object_id"),
    )

    columns = columns.select(
        F.col("name").alias("field_name"),
        F.col("object_id").alias("column_object_id"),
        F.col("user_type_id").alias("column_user_type_id"),
        F.col("column_id").alias("column_column_id"),
        F.col("default_object_id").alias("column_default_object_id"),
    )

    types = types.select(
        F.col("name").alias("field_type"),
        F.col("user_type_id").alias("type_user_type_id"),
    )

    default_constraints = default_constraints.select(
        F.col("object_id").alias("def_object_id"),
    )

    index_columns = index_columns.select(
        F.col("object_id").alias("index_object_id"),
        F.col("column_id").alias("index_column_id"),
        F.col("index_id").alias("index_index_id"),
    )

    indexes = indexes.select(
        F.col("object_id").alias("index_object_id"),
        F.col("index_id").alias("index_index_id"),
        F.col("is_primary_key").alias("index_is_primary_key"),
        F.col("is_unique_constraint").alias("index_is_unique_constraint"),
    )

    foreign_keys = foreign_keys.select(
        F.col("object_id").alias("fk_object_id"),
    )

    foreign_key_columns = foreign_key_columns.select(
        F.col("parent_object_id").alias("fkc_parent_object_id"),
        F.col("parent_column_id").alias("fkc_parent_column_id"),
        F.col("constraint_object_id").alias("fkc_constraint_object_id"),
    )

    extended_properties = extended_properties.select(
        F.col("major_id").alias("ep_major_id"),
        F.col("minor_id").alias("ep_minor_id"),
        F.col("name").alias("ep_name"),
        F.col("class_desc").alias("ep_class_desc"),
    ).filter(
        (F.col("ep_name") == F.lit("MS_Description"))
        & (F.col("ep_class_desc") == F.lit("OBJECT_OR_COLUMN"))
    )

    computed_columns = computed_columns.select(
        F.col("object_id").alias("cc_object_id"),
        F.col("column_id").alias("cc_column_id"),
    )

    indexes_pk = (
        index_columns.join(
            indexes, on=["index_object_id", "index_index_id"], how="inner"
        )
        .filter(F.col("index_is_primary_key"))
        .select(
            F.col("index_object_id").alias("index_pk_object_id"),
            F.col("index_column_id").alias("index_pk_column_id"),
        )
    )

    foreign_key = (
        foreign_keys.join(
            foreign_key_columns,
            on=[
                (
                    foreign_keys["fk_object_id"]
                    == foreign_key_columns["fkc_constraint_object_id"]
                )
            ],
            how="inner",
        )
        # TODO: in SQL, this is done via
        # GROUP BY fc.parent_column_id,fc.parent_object_id
        .drop("fk_object_id", "fkc_constraint_object_id").distinct()
    )

    check_constraints = (
        check_constraints.select(
            F.col("parent_object_id").alias("constraint_parent_object_id"),
            F.col("parent_column_id").alias("constraint_parent_column_id"),
        )
        # TODO: in SQL, this is done via
        # GROUP  BY c.parent_column_id, c.parent_object_id
        .distinct()
    )

    unique_key = (
        index_columns.join(
            indexes, on=["index_object_id", "index_index_id"], how="inner"
        )
        .filter(F.col("index_is_unique_constraint"))
        # TODO: in SQL, this is done via
        # GROUP  BY GROUP BY index_columns.object_id, index_columns.column_id)
        .select(
            F.col("index_object_id"),
            F.col("index_column_id"),
        )
        .distinct()
    )

    fields = tables.join(
        columns,
        on=[(tables["table_object_id"] == columns["column_object_id"])],
        how="left",
    )
    fields = (
        fields.join(
            types,
            on=[(fields["column_user_type_id"] == types["type_user_type_id"])],
            how="left",
        )
        .join(
            default_constraints,
            on=[
                (
                    fields["column_default_object_id"]
                    == default_constraints["def_object_id"]
                )
            ],
            how="left",
        )
        .join(
            indexes_pk,
            on=[
                (fields["column_object_id"] == indexes_pk["index_pk_object_id"])
                & (fields["column_column_id"] == indexes_pk["index_pk_column_id"])
            ],
            how="left",
        )
        .join(
            foreign_key,
            on=[
                (fields["column_object_id"] == foreign_key["fkc_parent_object_id"])
                & (fields["column_column_id"] == foreign_key["fkc_parent_column_id"])
            ],
            how="left",
        )
        .join(
            check_constraints,
            on=[
                (
                    fields["column_column_id"]
                    == check_constraints["constraint_parent_column_id"]
                )
                & (
                    fields["column_object_id"]
                    == check_constraints["constraint_parent_object_id"]
                )
            ],
            how="left",
        )
        .join(
            unique_key,
            on=[
                (fields["column_column_id"] == unique_key["index_column_id"])
                & (fields["column_object_id"] == unique_key["index_object_id"])
            ],
            how="left",
        )
        .join(
            extended_properties,
            on=[
                (fields["table_object_id"] == extended_properties["ep_major_id"])
                & (fields["column_column_id"] == extended_properties["ep_minor_id"])
            ],
            how="left",
        )
        .join(
            computed_columns,
            on=[
                (fields["table_object_id"] == computed_columns["cc_object_id"])
                & (fields["column_column_id"] == computed_columns["cc_column_id"])
            ],
            how="left",
        )
    )

    @F.udf(returnType=T.StringType())
    def map_field_type(field_type):
        return FIELD_TYPE_MAPPINGS.get(field_type)

    fields = (
        fields.withColumn("raw_field_name", F.col("field_name"))
        .withColumn(
            "field_name",
            F.regexp_replace(C.camel_to_snake_case(F.col("raw_field_name")), "__", "_"),
        )
        .withColumn("field_description", F.lit(None).cast("string"))
        .withColumn(
            "object_name",
            F.regexp_replace(C.camel_to_snake_case(F.col("table_name")), "__", "_"),
        )
        .withColumn("field_type", map_field_type(F.col("field_type")))
    )

    return fields.select(
        "object_name",
        "field_name",
        "raw_field_name",
        "field_type",
        "field_description",
        F.lit(None).astype("string").alias("domain_name"),
        F.lit(None).astype("string").alias("field_literal"),
        F.lit(None).astype("integer").alias("field_order"),
    )
