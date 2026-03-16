from pyspark.sql import Column, DataFrame, functions as F, types as T
from pyspark.sql.window import Window

from software_defined_integrations.transforms.preprocessors.sap_erp.utils.types import (
    SapFieldNameAlias,
    SpecialSapFieldName,
)


def generate_links_metadata(
    full_table_relations: DataFrame,
    table_fields_metadata: DataFrame,
):
    pkey_table_fields_metadata = _agg_pkey_table_fields_metadata(table_fields_metadata)
    all_fields = _agg_table_fields_metadata(table_fields_metadata)
    all_fields = all_fields.join(
        pkey_table_fields_metadata, SapFieldNameAlias.table_name
    ).hint("broadcast")
    relations = _agg_full_table_relations(
        _clean_full_table_relations(full_table_relations, table_fields_metadata)
    )
    relations = relations.join(
        pkey_table_fields_metadata.withColumnRenamed(
            SapFieldNameAlias.table_name, SapFieldNameAlias.check_table_name
        ),
        SapFieldNameAlias.check_table_name,
    )

    primary_all_fields = all_fields.select(
        [F.col(c).alias("primary_" + c) for c in all_fields.columns]
    )
    foreign_all_fields = all_fields.select(
        [F.col(c).alias("foreign_" + c) for c in all_fields.columns]
    )

    primary_all_fields = primary_all_fields.withColumn(
        "primary_mandt",
        _get_mandt_field(F.col(SapFieldNameAlias.table_name), table_fields_metadata),
    )

    field_data_element_sublist_links = primary_all_fields.join(
        foreign_all_fields,
        on=(
            # Rule 1: foreign_pkey_field_data_elements is a subset of primary_field_data_elements
            _is_subset(
                F.col("foreign_pkey_field_data_elements"),
                F.col("primary_field_data_elements"),
            )
        ),
    )
    # meta_element = either field_data_element or domain_name
    field_data_element_sublist_links = field_data_element_sublist_links.withColumn(
        "primary_foreign_key_list",
        _key_list(
            F.col("primary_field_names"),
            F.col("primary_field_data_elements"),
            F.col("foreign_pkey_field_data_elements"),
        ),
    )

    domain_name_sublist_links = primary_all_fields.join(
        foreign_all_fields,
        on=(
            # Rule 2: foreign_pkey_domain_names is a subset of primary_domain_names
            _is_subset(
                F.col("foreign_pkey_domain_names"), F.col("primary_domain_names")
            )
        ),
    )
    domain_name_sublist_links = domain_name_sublist_links.withColumn(
        "primary_foreign_key_list",
        _key_list(
            F.col("primary_field_names"),
            F.col("primary_domain_names"),
            F.col("foreign_pkey_domain_names"),
        ),
    )

    # Rule 1.B
    # SPRAS not in primary_field_data_elements
    primary_all_fields_fde_spras = primary_all_fields.filter(
        ~F.array_contains(
            F.col("primary_field_data_elements"), SpecialSapFieldName.SPRAS
        )
    )
    # And SPRAS in foreign_pkey_field_names
    foreign_all_fields_fde_spras = foreign_all_fields.filter(
        F.array_contains(F.col("foreign_pkey_field_names"), SpecialSapFieldName.SPRAS)
    )
    field_data_element_spras_links = primary_all_fields_fde_spras.join(
        foreign_all_fields_fde_spras,
        on=(
            # And foreign_pkey_field_data_elements is a subset of [*primary_field_data_elements, SPRAS]
            _is_subset(
                F.col("foreign_pkey_field_data_elements"),
                F.array_union(
                    F.col("primary_field_data_elements"),
                    F.array(F.lit(SpecialSapFieldName.SPRAS)),
                ),
            )
        ),
    )
    field_data_element_spras_links = field_data_element_spras_links.withColumn(
        "primary_foreign_key_list",
        _key_list(
            F.col("primary_field_names"),
            F.col("primary_field_data_elements"),
            F.col("foreign_pkey_field_data_elements"),
        ),
    )

    # Rule 2.B
    # SPRAS not in primary_domain_names
    primary_all_fields_dn_spras = primary_all_fields.filter(
        ~F.array_contains(F.col("primary_domain_names"), SpecialSapFieldName.SPRAS)
    )
    # And SPRAS in foreign_pkey_field_names
    foreign_all_fields_dn_spras = foreign_all_fields.filter(
        F.array_contains(F.col("foreign_pkey_field_names"), SpecialSapFieldName.SPRAS)
    )
    domain_name_spras_links = primary_all_fields_dn_spras.join(
        foreign_all_fields_dn_spras,
        on=(
            # And foreign_pkey_domain_names is a subset of [*primary_domain_names, SPRAS]
            _is_subset(
                F.col("foreign_pkey_domain_names"),
                F.array_union(
                    F.col("primary_domain_names"),
                    F.array(F.lit(SpecialSapFieldName.SPRAS)),
                ),
            )
        ),
    )
    domain_name_spras_links = domain_name_spras_links.withColumn(
        "primary_foreign_key_list",
        _key_list(
            F.col("primary_field_names"),
            F.col("primary_domain_names"),
            F.col("foreign_pkey_domain_names"),
        ),
    )

    metadata_links = (
        field_data_element_sublist_links.unionByName(domain_name_sublist_links)
        .unionByName(field_data_element_spras_links)
        .unionByName(domain_name_spras_links)
        .filter(
            # Remove relations that are only [MANDT-like] or [SPRAS]
            (F.col("primary_foreign_key_list") != F.array("primary_mandt"))
        )
        .select(
            F.col("primary_table_name").alias("primary_object_name"),
            F.col("primary_foreign_key_list").alias("primary_object_raw_fields"),
            F.col("foreign_table_name").alias("foreign_object_name"),
            F.col("foreign_pkey_field_names").alias("foreign_object_raw_fields"),
            F.lit(None).cast(T.StringType()).alias("link_description"),
        )
    )

    relations_links = relations.select(
        F.col(SapFieldNameAlias.table_name).alias("primary_object_name"),
        F.col("column_names").alias("primary_object_raw_fields"),
        F.col(SapFieldNameAlias.check_table_name).alias("foreign_object_name"),
        F.col("pkey_field_names").alias("foreign_object_raw_fields"),
        F.lit(None).cast(T.StringType()).alias("link_description"),
    )

    return (
        metadata_links.unionByName(relations_links)
        .filter(
            # TODO (sbalanovich): ensure this filter is not needed. Remove relations that have 0 lengths
            (F.size("primary_object_raw_fields") > 0)
            &
            # TODO (sbalanovich): ensure this filter is not needed. Remove relations that have different lengths
            (F.size("primary_object_raw_fields") == F.size("foreign_object_raw_fields"))
            &
            # TODO (sbalanovich): ensure this filter is not needed. Remove relations that have different lengths
            (F.col("primary_object_name") != F.col("foreign_object_name"))
            # Remove relations that are only [MANDT] or [SPRAS]
            & (
                F.col("primary_object_raw_fields")
                != F.array(F.lit(SpecialSapFieldName.MANDT))
            )
            & (
                F.col("primary_object_raw_fields")
                != F.array(F.lit(SpecialSapFieldName.SPRAS))
            )
        )
        .dropDuplicates()
    )


def _is_subset(
    small_list: Column,
    large_list: Column,
):
    return F.size(F.array_intersect(large_list, small_list)) == F.size(
        F.array_distinct(small_list)
    )


@F.udf(returnType=T.ArrayType(T.StringType()))
def _key_list(
    primary_field_names: Column,
    primary_meta_elements: Column,
    foreign_pkey_meta_elements: Column,
):
    return [
        primary_field_names[index]
        for index in [
            primary_meta_elements.index(meta_element)
            if meta_element != SpecialSapFieldName.SPRAS
            else primary_field_names.index(SpecialSapFieldName.SPRAS)
            for meta_element in foreign_pkey_meta_elements
            if meta_element in primary_meta_elements
        ]
    ]


def _agg_pkey_table_fields_metadata(
    table_fields_metadata: DataFrame,
) -> DataFrame:
    partitions = [SapFieldNameAlias.table_name]
    order = [
        SapFieldNameAlias.table_name,
        SapFieldNameAlias.position_of_the_field_in_the_table,
    ]
    w = Window.partitionBy(*partitions).orderBy(*order)
    return (
        table_fields_metadata.filter(
            F.col(SapFieldNameAlias.identifies_a_key_field_of_a_table) == "X"
        )
        .withColumn(
            "pkey_field_names",
            F.collect_list(F.col(SapFieldNameAlias.field_name)).over(w),
        )
        .withColumn(
            "pkey_field_data_elements",
            F.collect_list(F.col(SapFieldNameAlias.field_data_element)).over(w),
        )
        .withColumn(
            "pkey_domain_names",
            F.collect_list(F.col(SapFieldNameAlias.domain_name)).over(w),
        )
        .groupBy(*partitions)
        .agg(
            F.max("pkey_field_names").alias("pkey_field_names"),
            F.max("pkey_field_data_elements").alias("pkey_field_data_elements"),
            F.max("pkey_domain_names").alias("pkey_domain_names"),
        )
        .select(
            SapFieldNameAlias.table_name,
            "pkey_field_names",
            "pkey_field_data_elements",
            "pkey_domain_names",
        )
    )


# Produces an aggregated dataset of all table names
def _agg_table_fields_metadata(table_fields_metadata: DataFrame) -> DataFrame:
    partitions = [SapFieldNameAlias.table_name]
    order = [
        SapFieldNameAlias.table_name,
        SapFieldNameAlias.position_of_the_field_in_the_table,
    ]
    w = Window.partitionBy(*partitions).orderBy(*order)
    return (
        table_fields_metadata.withColumn(
            "field_names", F.collect_list(F.col(SapFieldNameAlias.field_name)).over(w)
        )
        .withColumn(
            "field_data_elements",
            F.collect_list(F.col(SapFieldNameAlias.field_data_element)).over(w),
        )
        .withColumn(
            "domain_names", F.collect_list(F.col(SapFieldNameAlias.domain_name)).over(w)
        )
        .groupBy(*partitions)
        .agg(
            F.max("field_names").alias("field_names"),
            F.max("field_data_elements").alias("field_data_elements"),
            F.max("domain_names").alias("domain_names"),
        )
        .select(
            SapFieldNameAlias.table_name,
            "field_names",
            "field_data_elements",
            "domain_names",
        )
    )


# Removed fields from relations that don't exist in the data
def _clean_full_table_relations(
    full_table_relations: DataFrame,
    table_fields_metadata: DataFrame,
) -> DataFrame:
    return full_table_relations.join(
        table_fields_metadata.select(
            F.col(SapFieldNameAlias.table_name),
            F.col(SapFieldNameAlias.field_name).alias(SapFieldNameAlias.column_name),
        ),
        [SapFieldNameAlias.table_name, SapFieldNameAlias.column_name],
        "inner",
    )


# Produces an aggregated dataset of all table names
def _agg_full_table_relations(full_table_relations: DataFrame) -> DataFrame:
    partitions = [
        SapFieldNameAlias.table_name,
        SapFieldNameAlias.check_table_name,
        SapFieldNameAlias.segment_by,
    ]
    order = [
        SapFieldNameAlias.table_name,
        SapFieldNameAlias.check_table_name,
        SapFieldNameAlias.position,
    ]
    w = Window.partitionBy(*partitions).orderBy(*order)
    return (
        full_table_relations.withColumn(
            "column_names", F.collect_list(F.col(SapFieldNameAlias.column_name)).over(w)
        )
        .groupBy(*partitions)
        .agg(F.max("column_names").alias("column_names"))
        .select(
            SapFieldNameAlias.table_name,
            SapFieldNameAlias.check_table_name,
            "column_names",
        )
    )


# In SAP, the MANDT field is the client and every table should have it
# It is sometimes named something else, like CLIENT
# Extract the correct MANDT field by finding the MANDT data element instead
def _get_mandt_field(table: Column, fields_metadata: DataFrame) -> str:
    mandt_field = (
        fields_metadata.filter(
            (F.col(SapFieldNameAlias.table_name) == table)
            & (F.col(SapFieldNameAlias.field_data_element) == SpecialSapFieldName.MANDT)
        )
        .select(SapFieldNameAlias.field_name)
        .collect()
    )
    if mandt_field:
        return F.lit(mandt_field[0].asDict()[SapFieldNameAlias.field_name])
    return F.lit(None).cast(T.StringType())
