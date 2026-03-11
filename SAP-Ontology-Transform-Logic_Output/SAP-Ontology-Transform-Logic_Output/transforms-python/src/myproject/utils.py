# =============================================================
# Converted from Palantir Transforms -> Databricks PySpark
# Generated on 2026-02-25T13:47:18.752088400Z (UTC)
# =============================================================

# Original file had no Palantir-specific code
# File copied as-is

from pyspark.sql import functions as F, types as T
from pyspark.sql.window import Window
from itertools import chain
from pyspark.sql import DataFrame
from typing import Dict
import re
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, DecimalType
from foundry_lifetimes import apply_lifetimes_model, predict_clv, LifetimesPredictionType


_METRIC_COLS = [
    "count_of_billing_document_items",
    "count_of_mandt_kunag_|_foreign_key_KNA1",
    "count_of_mandt_kunrg_|_foreign_key_KNA1",
    "count_of_mandt_kunrg_vkorg_vtweg_spart_|_foreign_key_KNVV",
    "sum_of_billed_quantity_|_fkimg",
    "sum_of_sales_value",
    "avg_unit_price",
    'size_of_cumulative_set_of_mandt_matnr_|_foreign_key_MARA',
    'lagged_size_of_cumulative_set_of_mandt_matnr_|_foreign_key_MARA'
]


def drop_cols_where_all_values_are_the_same(df):
    # map cols and their approx distinct count
    col_counts = df.agg(*(F.approx_count_distinct(F.col(c)).alias(c) for c in df.columns)).collect()[0].asDict()

    # create array of cols where approx distinct count <= 1
    cols_to_drop = [col for col in df.columns if col_counts[col] <= 1]

    return df.drop(*cols_to_drop)


def add_time_series_metric_id_cols(df, primary_key_col, group_type_id, metrics=_METRIC_COLS, filter_ids=['all']):
    if len(filter_ids) == 0:
        filter_ids = ['all']
    for metric in metrics:
        for filter_id in filter_ids:
            col_name = '_|_'.join([group_type_id, filter_id, metric, "time_series_id"])
            df = df.withColumn(col_name, F.concat_ws('_||_', F.lit(group_type_id), F.lit(filter_id), primary_key_col, F.lit(metric)))
    return df


_OBSERVATION_DURATIONS = [7, 14, 30, 60, 90, 182, 365]


def enrich_with_forecasting_metrics(
    df, freq_model, monetary_model, grouping_name, since_date_alias='2022_01_01', observation_durations=_OBSERVATION_DURATIONS
):
    frequency_col = f'{grouping_name}_order_header_count_since_{since_date_alias}'
    recency_col = f'{grouping_name}_recency_in_days_since_{since_date_alias}'
    T_col = f'{grouping_name}_age_in_days_since_{since_date_alias}'
    monetary_col = f'{grouping_name}_avg_order_value_per_order_header_since_{since_date_alias}'

    enrichments = (
        df
        .where(
            (F.col(f"{grouping_name}_has_billing_documents_since_{since_date_alias}") == "true")
            & (F.col(monetary_col) > 0)
            & (F.col(recency_col) < F.col(T_col))
        )
        .select(
            "primary_key",
            frequency_col,
            recency_col,
            T_col,
            monetary_col
        )
        .withColumn(
            f"{grouping_name}_alive_probability_forecast",
            apply_lifetimes_model(
                freq_model,
                frequency_column_name=frequency_col,
                recency_column_name=recency_col,
                T_column_name=T_col,
                prediction_type=LifetimesPredictionType.CONDITIONAL_PROBABILITY_ALIVE
            )
        )
        .withColumn(
            f"{grouping_name}_total_lifetime_value_forecast",
            predict_clv(
                freq_model,
                monetary_model,
                frequency_column_name=frequency_col,
                recency_column_name=recency_col,
                T_column_name=T_col,
                monetary_value_column_name=monetary_col
            )
        )
    )

    for obs_duration in observation_durations:
        enrichments = (
            enrichments
            .withColumn(
                "obs_t",
                F.lit(obs_duration)
            )
            .withColumn(
                f"{grouping_name}_{obs_duration}_day_frequency_forecast",
                apply_lifetimes_model(
                    freq_model,
                    observation_duration_column_name='obs_t',
                    frequency_column_name=frequency_col,
                    recency_column_name=recency_col,
                    T_column_name=T_col,
                    prediction_type=LifetimesPredictionType.CONDITIONAL_EXPECTED_NUMBER_OF_PURCHASES
                )
            )
            .withColumn(
                f"{grouping_name}_{obs_duration}_day_average_monetary_value_forecast",
                apply_lifetimes_model(
                    monetary_model,
                    frequency_column_name=frequency_col,
                    monetary_value_column_name=monetary_col
                )
            )
            .withColumn(
                f"{grouping_name}_{obs_duration}_day_total_value_forecast",
                F.col(f"{grouping_name}_alive_probability_forecast")
                * F.col(f"{grouping_name}_{obs_duration}_day_frequency_forecast")
                * F.col(f"{grouping_name}_{obs_duration}_day_average_monetary_value_forecast")
            )
        )

    enrichments = enrichments.drop(
        "obs_t",
        frequency_col,
        recency_col,
        T_col,
        monetary_col
    )
    df_enriched = df.join(enrichments, on='primary_key', how='left')

    return df_enriched


def enrich_with_billing_document_metrics(df, bdi, grouping_cols, grouping_name, enrich_with_ranks_pcntgs_and_cumsums=True):
    month_lags = list(range(1, 7)) + [12]  # last 6 months and YTD
    metric_date_cutoffs = [datetime.today() - relativedelta(months=x) for x in month_lags]
    metric_date_cutoffs_str = [d.strftime('%Y-%m-%d') for d in metric_date_cutoffs]
    metric_date_cutoffs_aliases_str = [f'{x}_months_ago_to_date' for x in month_lags]

    rfm1 = get_recency_frequency_and_monetary_value_by_group(
        bdi,
        grouping_cols=grouping_cols,
        grouping_name=grouping_name,
        since_date='2022-01-01',
        enrich_with_ranks_pcntgs_and_cumsums=enrich_with_ranks_pcntgs_and_cumsums
    )

    rfm2 = get_recency_frequency_and_monetary_value_by_group(
        bdi,
        grouping_cols=grouping_cols,
        grouping_name=grouping_name,
        since_date='2023-01-01',
        enrich_with_ranks_pcntgs_and_cumsums=enrich_with_ranks_pcntgs_and_cumsums
    )

    rfm = rfm1.join(rfm2, on='_group_id', how='outer')

    for date_str, date_alias in zip(metric_date_cutoffs_str, metric_date_cutoffs_aliases_str):  # noqa
        rfm = (
            rfm
            .join(
                get_recency_frequency_and_monetary_value_by_group(
                    bdi,
                    grouping_cols=grouping_cols,
                    grouping_name=grouping_name,
                    since_date=date_str,
                    since_date_alias=date_alias,
                    enrich_with_ranks_pcntgs_and_cumsums=enrich_with_ranks_pcntgs_and_cumsums
                ),
                on='_group_id',
                how='outer'
            )
        )
    rfm = rfm.withColumnRenamed('_group_id', 'primary_key')

    # add financial calculations to each entity (if entity has associated billing documents)
    df_enriched = df.join(rfm, on='primary_key', how='left')
    return df_enriched


def enrich_with_billing_document_features(df, bdi, grouping_cols):
    preprocess = (
        bdi
        .withColumn(
            "_group_id",
            F.concat_ws('_||_', *grouping_cols)
        )
        .withColumn(                                    # Map SPD labels
            "spd_label",
            F.when(
                F.col("spd_flag"),
                F.lit("SPD")
            ).otherwise(
                F.lit("NON-SPD")
            )
        )
        .withColumn(                                    # Map SOURCE labels
            "source_label",
            F.when(
                F.col("source_flag"),
                F.lit("SOURCE")
            ).otherwise(
                F.lit("NON-SOURCE")
            )
        )
    )

    agg = (
        preprocess
        .groupBy("_group_id")
        .agg(
            F.collect_set("spd_label").alias("spd_labels"),  # Collect all unique SPD labels
            F.collect_set("source_label").alias("source_labels")  # Collect all unique SOURCE labels
        )
    )

    df_enriched = (
        df
        .join(
            agg.withColumnRenamed("_group_id", "primary_key"),
            on="primary_key",
            how="left"
        )
    )

    return df_enriched


def decimal_to_double_helper(dtype_str):
    return re.sub(r'decimal\(\d+,\d+\)', 'double', dtype_str)


def cast_decimal_cols_to_double(df):
    return df.select(*[
            F.col(d[0]).astype(decimal_to_double_helper(d[1]))
            if 'decimal' in d[1] else F.col(d[0])
            for d in df.dtypes
        ])


def remove_null_cols(df, but_keep_these=[]):
    """Drops DataFrame columns that are fully null
    (i.e. the maximum value is null)

    Arguments:
        df {spark DataFrame} -- spark dataframe
        but_keep_these {list} -- list of columns to keep without checking for nulls

    Returns:
        spark DataFrame -- dataframe with fully null columns removed
    """

    # skip checking some columns
    cols_to_check = [col for col in df.columns if col not in but_keep_these]
    if len(cols_to_check) > 0:
        # drop columns for which the max is None
        rows_with_data = df.select(*cols_to_check).groupby().agg(*[F.max(c).alias(c) for c in cols_to_check]).take(1)[0]  # noqa
        cols_to_drop = [c for c, const in rows_with_data.asDict().items() if const == None]
        new_df = df.drop(*cols_to_drop)
        return new_df
    else:
        return df


def get_cols_sorted_by_null_count(df):
    null_count_dict = {col: df.filter(df[col].isNull()).repartition(1).count() for col in df.columns}
    sorted_cols = [col for col, null_count in sorted(null_count_dict.items(), key=lambda x: f'{x[1]}_{x[0]}')]
    return sorted_cols


def map_column_values(df: DataFrame, map_dict: Dict, column: str, new_column: str = "") -> DataFrame:
    """Handy method for mapping column values from one value to another

    Args:
        df (DataFrame): Dataframe to operate on 
        map_dict (Dict): Dictionary containing the values to map from and to
        column (str): The column containing the values to be mapped
        new_column (str, optional): The name of the column to store the mapped values in. 
                                    If not specified the values will be stored in the original column

    Returns:
        DataFrame
    """
    spark_map = F.create_map([F.lit(x) for x in chain(*map_dict.items())])
    return df.withColumn(new_column or column, spark_map[df[column]])


def to_snake_case(name):
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    name = re.sub('__([A-Z])', r'_\1', name)
    name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', name)
    return name.lower()


def diff_month(d1, d2):
    return (d1.year - d2.year) * 12 + d1.month - d2.month


def get_recency_frequency_and_monetary_value_by_group(bdi, grouping_cols, grouping_name, since_date='2022-09-01', since_date_alias=None, enrich_with_ranks_pcntgs_and_cumsums=True):
    if since_date_alias:
        since_date_alias_w_underscores = since_date_alias
    else:
        since_date_alias_w_underscores = since_date.replace('-', '_')

    metrics_df = (
        bdi
        .withColumn('_group_id', F.concat_ws('_||_', *grouping_cols))
        .filter(
            (F.col('document_currency_|_waerk') == 'USD')
            & (F.col('billed_quantity_|_fkimg') > 0)  # note that this means our rfm metrics are ignoring returns
            & (F.col('net_value_|_netwr') > 0)  # TODO: confirm if this is a valid filter to impose
            & (F.col('billing_date_|_fkdat') >= since_date)
        )
        .groupby('_group_id')
        .agg(
            F.datediff(
                F.current_date(),
                F.min(F.col('billing_date_|_fkdat'))
            )
            .alias(f'{grouping_name}_age_in_days_since_{since_date_alias_w_underscores}'),

            F.datediff(
                F.max(F.col('billing_date_|_fkdat')),
                F.min(F.col('billing_date_|_fkdat'))
            ).alias(f'{grouping_name}_recency_in_days_since_{since_date_alias_w_underscores}'),

            F.datediff(
                F.current_date(),
                F.max(F.col('billing_date_|_fkdat'))
            ).alias(f'{grouping_name}_days_since_last_order_since_{since_date_alias_w_underscores}'),

            F.approx_count_distinct(
                F.col('primary_key')
            ).alias(f'{grouping_name}_order_count_since_{since_date_alias_w_underscores}'),

            F.approx_count_distinct(
                F.col('mandt_vbeln_|_foreign_key_VBRK')
            ).alias(f'{grouping_name}_order_header_count_since_{since_date_alias_w_underscores}'),

            F.approx_count_distinct(
                F.col('billing_date_|_fkdat')
            ).alias(f'{grouping_name}_unique_order_date_count_since_{since_date_alias_w_underscores}'),

            F.approx_count_distinct(
                F.col("mandt_matnr_|_foreign_key_MARA")
            ).alias(f'{grouping_name}_unique_cin_count_since_{since_date_alias_w_underscores}'),

            F.approx_count_distinct(
                F.col("mandt_kunag_|_foreign_key_KNA1")
            ).alias(f'{grouping_name}_unique_sold_to_customer_count_since_{since_date_alias_w_underscores}'),

            F.sum(
                F.col("sales_value")
            ).alias(f'{grouping_name}_total_order_value_since_{since_date_alias_w_underscores}'),
            
              F.sum(
            F.when(F.col("spd_flag") == True, F.col("sales_value")).otherwise(0)
        ).alias(f'{grouping_name}_total_order_value_spd_only_since_{since_date_alias_w_underscores}'),
        
            F.sum(
                F.col("billed_quantity_|_fkimg")
            ).alias(f'{grouping_name}_total_units_billed_since_{since_date_alias_w_underscores}')
        )
        .withColumn(
            f'{grouping_name}_avg_order_value_since_{since_date_alias_w_underscores}',
            F.col(f'{grouping_name}_total_order_value_since_{since_date_alias_w_underscores}') / F.col(f'{grouping_name}_order_count_since_{since_date_alias_w_underscores}')
        )
        .withColumn(
            f'{grouping_name}_total_order_value_per_day_since_{since_date_alias_w_underscores}',
            F.col(f'{grouping_name}_total_order_value_since_{since_date_alias_w_underscores}') / F.col(f'{grouping_name}_age_in_days_since_{since_date_alias_w_underscores}')
        )
        .withColumn(
            f'{grouping_name}_avg_order_value_per_order_date_since_{since_date_alias_w_underscores}',
            F.col(f'{grouping_name}_total_order_value_since_{since_date_alias_w_underscores}') / F.col(f'{grouping_name}_unique_order_date_count_since_{since_date_alias_w_underscores}')
        )
        .withColumn(
            f'{grouping_name}_avg_order_value_per_order_header_since_{since_date_alias_w_underscores}',
            F.col(f'{grouping_name}_total_order_value_since_{since_date_alias_w_underscores}') / F.col(f'{grouping_name}_order_header_count_since_{since_date_alias_w_underscores}')
        )
        # add billing documents flag:
        .withColumn(
            f"{grouping_name}_has_billing_documents_since_{since_date_alias_w_underscores}",
            F.when(
                F.isnull(F.col(f"{grouping_name}_age_in_days_since_{since_date_alias_w_underscores}")),
                False
            ).when(
                # ensure first purchase was at least 5 days ago
                F.col(f"{grouping_name}_age_in_days_since_{since_date_alias_w_underscores}") <= 5,
                False
            ).otherwise(True)
        )
    )

    if enrich_with_ranks_pcntgs_and_cumsums:
        metrics_df = enrich_metric_with_ranks_pcntgs_and_cumsums(
            metrics_df, f'{grouping_name}_order_count_since_{since_date_alias_w_underscores}'
        )

        metrics_df = enrich_metric_with_ranks_pcntgs_and_cumsums(
            metrics_df, f'{grouping_name}_total_order_value_since_{since_date_alias_w_underscores}'
        )

        metrics_df = enrich_metric_with_ranks_pcntgs_and_cumsums(
            metrics_df, f'{grouping_name}_total_order_value_per_day_since_{since_date_alias_w_underscores}'
        )
    return metrics_df


def enrich_metric_with_ranks_pcntgs_and_cumsums(df, metric_name):
    df = (
        df
        .withColumn(
            f'rank_of_{metric_name}',
            F.row_number().over(
                Window.orderBy(F.col(metric_name).desc())
            )
        )
        .withColumn(
            f'cumulative_sum_of_{metric_name}',
            F.sum(metric_name).over(
                Window
                .orderBy(F.col(metric_name).desc())
                .rangeBetween(Window.unboundedPreceding, 0)
            )
        )
        .join(
            (
                df.groupby().agg(
                    F.sum(F.col(metric_name))
                    .alias(f'total_{metric_name}')
                )
            ),
            how='cross'
        )
        .withColumn(
            f'pcntg_of_total_{metric_name}',
            F.col(metric_name) / F.col(f'total_{metric_name}')
        )
        .withColumn(
            f'cumulative_sum_of_pcntg_of_total_{metric_name}',
            F.col(f'cumulative_sum_of_{metric_name}') / F.col(f'total_{metric_name}')
        )
    )
    return df


def get_metric_agg_with_abc_classification(
    df,
    agg_fn_name='sum',
    metric_col='sales_value',
    metric_name='sales_in_usd',
    grouping_scheme='product',
    grouping_cols=[],
    partition_scheme='dataset',
    partition_cols=[],
    a_class_max_pcntg=0.8,
    b_class_max_pcntg=0.95
):
    # metric_agg_params = {
    #     'grouping_scheme': grouping_scheme,
    #     'grouping_cols': grouping_cols,
    #     'partition_scheme': partition_scheme,
    #     'partition_cols': partition_cols,
    #     'metric_col': metric_col,
    #     'metric_name': metric_name,
    #     'agg_fn_name': agg_fn_name,
    #     'a_class_max_pcntg': a_class_max_pcntg,
    #     'b_class_max_pcntg': b_class_max_pcntg
    # }
    NAME_TO_AGG_FN_MAP = {
        'sum': F.sum,
        'avg': F.avg
    }

    agg_fn = NAME_TO_AGG_FN_MAP.get(agg_fn_name)

    # col names:
    col_name__agg_fn_of_metric_for_dataset = f'{agg_fn_name}_of_{metric_name}_for_dataset'
    col_name__agg_fn_of_metric_for_partition_block = f'{agg_fn_name}_of_{metric_name}_for_{partition_scheme}_blocks'
    col_name__agg_fn_of_metric_for_group_within_partition_block = f'{agg_fn_name}_of_{metric_name}_for_{grouping_scheme}_within_{partition_scheme}'

    col_name__rank_of_metric_for_partition_block = f'rank_of_{metric_name}_for_{partition_scheme}_block'
    col_name__rank_of_metric_for_group_within_partition_block = f'rank_of_{metric_name}_for_{grouping_scheme}_within_{partition_scheme}'

    col_name__cumulative_agg_fn_of_metric_across_desc_ordered_partition_blocks = f'cumulative_{agg_fn_name}_of_{metric_name}_across_desc_ordered_{partition_scheme}s'
    col_name__cumulative_agg_fn_of_metric_across_desc_ordered_groups_within_partition_block = f'cumulative_{agg_fn_name}_of_{metric_name}_across_desc_ordered_{grouping_scheme}s_within_{partition_scheme}'

    col_name__pcntg_of_agg_fn_of_metric_for_dataset_represented_by_partition_block = f'pcntg_of_{agg_fn_name}_of_{metric_name}_represented_by_{partition_scheme}'
    col_name__pcntg_of_agg_fn_of_metric_for_partition_block_represented_by_group = f'pcntg_of_{agg_fn_name}_of_{metric_name}_for_{partition_scheme}_represented_by_{grouping_scheme}'

    col_name__cumulative_pcntg_of_agg_fn_of_metric_across_desc_ordered_partition_blocks = f'cumulative_pcntg_of_{agg_fn_name}_of_{metric_name}_across_desc_ordered_{partition_scheme}s'
    col_name__cumulative_pcntg_of_agg_fn_of_metric_across_desc_ordered_groups_within_partition_block = f'cumulative_pcntg_of_{agg_fn_name}_of_{metric_name}_across_desc_ordered_{grouping_scheme}s_within_{partition_scheme}'

    col_name__abc_category_of_agg_fn_of_metric_for_partition_block = f'abc_category_of_{agg_fn_name}_of_{metric_name}_for_{partition_scheme}'
    col_name__abc_category_of_agg_fn_of_metric_for_group_within_partition_block = f'abc_category_of_{agg_fn_name}_of_{metric_name}_for_{grouping_scheme}_within_{partition_scheme}'

    window_for_partition_df = (
        Window.orderBy(F.col(col_name__agg_fn_of_metric_for_partition_block).desc())
    )

    partition_df = (
        df
        .groupby(*partition_cols)
        .agg(
            agg_fn(F.col(metric_col))
            .alias(col_name__agg_fn_of_metric_for_partition_block)
        )
        .withColumn(
            col_name__rank_of_metric_for_partition_block,
            F.row_number().over(window_for_partition_df)
        )
        .withColumn(
            col_name__cumulative_agg_fn_of_metric_across_desc_ordered_partition_blocks,
            agg_fn(col_name__agg_fn_of_metric_for_partition_block).over(
                window_for_partition_df.rangeBetween(Window.unboundedPreceding, 0)
            )
        )
        .join(
            df.groupby().agg(agg_fn(F.col(metric_col)).alias(col_name__agg_fn_of_metric_for_dataset)),
            how='cross'
        )
        .withColumn(
            col_name__pcntg_of_agg_fn_of_metric_for_dataset_represented_by_partition_block,
            F.col(col_name__agg_fn_of_metric_for_partition_block) / F.col(col_name__agg_fn_of_metric_for_dataset)
        )
        .withColumn(
            col_name__cumulative_pcntg_of_agg_fn_of_metric_across_desc_ordered_partition_blocks,
            F.col(col_name__cumulative_agg_fn_of_metric_across_desc_ordered_partition_blocks) / F.col(col_name__agg_fn_of_metric_for_dataset)
        )
        .withColumn(
            col_name__abc_category_of_agg_fn_of_metric_for_partition_block,
            (
                F.when(
                    (
                        (
                            (F.col(col_name__cumulative_pcntg_of_agg_fn_of_metric_across_desc_ordered_partition_blocks) <= a_class_max_pcntg)
                            & (F.col(col_name__agg_fn_of_metric_for_partition_block) > 0)
                        ) | (F.col(col_name__pcntg_of_agg_fn_of_metric_for_dataset_represented_by_partition_block) >= a_class_max_pcntg)
                    ),
                    F.lit('A'))
                .when(
                    (
                        (
                            (F.col(col_name__cumulative_pcntg_of_agg_fn_of_metric_across_desc_ordered_partition_blocks) > a_class_max_pcntg)
                            & (F.col(col_name__cumulative_pcntg_of_agg_fn_of_metric_across_desc_ordered_partition_blocks) <= b_class_max_pcntg)
                        )
                    ),
                    F.lit('B')
                )
                .otherwise(
                    F.lit('C')
                )
            )
        )
        .withColumn(
            'partition_cols',
            F.array(*map(F.lit, partition_cols)) if partition_cols else F.array().cast("array<string>")
        )
        .withColumn(
            'partition_block_vals',
            F.array(*map(F.col, partition_cols)) if partition_cols else F.array().cast("array<string>")
        )
        .withColumn(
            'partition_block_map',
            F.map_from_arrays(F.col('partition_cols'), F.col('partition_block_vals')) if partition_cols else F.create_map().cast("map<string,string>")
        )
        .withColumn(
            'partition_block_id',
            F.to_json(F.col('partition_block_map')) if partition_cols else F.lit(None).cast('string')
        )
    )

    window_for_agg_df = (
        Window.partitionBy(*partition_cols).orderBy(F.col(col_name__agg_fn_of_metric_for_group_within_partition_block).desc())
    )

    agg_df = (
        df
        .groupby(
            *partition_cols,
            *grouping_cols
        )
        .agg(
            agg_fn(F.col(metric_col)).alias(col_name__agg_fn_of_metric_for_group_within_partition_block)
        )
        .withColumn(
            col_name__rank_of_metric_for_group_within_partition_block,
            F.row_number().over(window_for_agg_df)
        )
        .withColumn(
            col_name__cumulative_agg_fn_of_metric_across_desc_ordered_groups_within_partition_block,
            agg_fn(col_name__agg_fn_of_metric_for_group_within_partition_block).over(window_for_agg_df.rangeBetween(Window.unboundedPreceding, 0))
        )
        .join(
            partition_df,
            on=partition_cols if partition_cols else None,
            how='left' if partition_cols else 'cross'
        )
        .withColumn(
            col_name__pcntg_of_agg_fn_of_metric_for_partition_block_represented_by_group,
            F.col(col_name__agg_fn_of_metric_for_group_within_partition_block) / F.col(col_name__agg_fn_of_metric_for_partition_block)
        )
        .withColumn(
            col_name__cumulative_pcntg_of_agg_fn_of_metric_across_desc_ordered_groups_within_partition_block,
            F.col(col_name__cumulative_agg_fn_of_metric_across_desc_ordered_groups_within_partition_block) / F.col(col_name__agg_fn_of_metric_for_partition_block)
        )
        .withColumn(
            col_name__abc_category_of_agg_fn_of_metric_for_group_within_partition_block,
            (
                F.when(
                    (
                        (
                            (F.col(col_name__cumulative_pcntg_of_agg_fn_of_metric_across_desc_ordered_groups_within_partition_block) <= a_class_max_pcntg)
                            & (F.col(col_name__agg_fn_of_metric_for_group_within_partition_block) > 0)
                        ) | (F.col(col_name__pcntg_of_agg_fn_of_metric_for_partition_block_represented_by_group) >= a_class_max_pcntg)
                    ),
                    F.lit('A'))
                .when(
                    (
                        (
                            (F.col(col_name__cumulative_pcntg_of_agg_fn_of_metric_across_desc_ordered_groups_within_partition_block) > a_class_max_pcntg)
                            & (F.col(col_name__cumulative_pcntg_of_agg_fn_of_metric_across_desc_ordered_groups_within_partition_block) <= b_class_max_pcntg)
                        )
                    ),
                    F.lit('B')
                )
                .otherwise(
                    F.lit('C')
                )
            )
        )
        .withColumn(
            'grouping_cols',
            F.array(*map(F.lit, grouping_cols)) if grouping_cols else F.array()
        )
        .withColumn(
            'group_vals',
            F.array(*map(F.col, grouping_cols)) if grouping_cols else F.array()
        )
        .withColumn(
            'group_map',
            F.map_from_arrays(F.col('grouping_cols'), F.col('group_vals')) if grouping_cols else F.create_map().cast("map<string,string>")
        )
        .withColumn(
            'group_id',
            F.to_json(F.col('group_map')) if grouping_cols else F.lit(None)
        )
        .withColumn(
            'metric_col', F.lit(metric_col)
        )
        .withColumn(
            'metric_name', F.lit(metric_name)
        )
        .withColumn(
            'agg_fn_name', F.lit(agg_fn_name)
        )
        .withColumn(
            'a_class_max_pcntg', F.lit(a_class_max_pcntg)
        )
        .withColumn(
            'b_class_max_pcntg', F.lit(b_class_max_pcntg)
        )
    )
    return agg_df


def schema_as_dataframe(df):
    # Create the schema for the output DataFrame
    output_schema = StructType([
        StructField("column_name", StringType(), True),
        StructField("data_type", StringType(), True)
    ])

    # Extract schema information from the input DataFrame
    schema_info = [(field.name, str(field.dataType)) for field in df.schema.fields]

    # Create a new DataFrame with the schema information
    spark = SparkSession.builder.getOrCreate()
    schema_df = spark.createDataFrame(schema_info, schema=output_schema)
    return schema_df


def schema_with_statistics(df):
    # Create the schema for the output DataFrame
    output_schema = StructType([
        StructField("column_name", StringType(), True),
        StructField("data_type", StringType(), True),
        StructField("count", StringType(), True),
        StructField("null_count", LongType(), True),
        StructField("unique_values", LongType(), True),
        StructField("mean", StringType(), True),
        StructField("stddev", StringType(), True),
        StructField("min", StringType(), True),
        StructField("max", StringType(), True)
    ])
  
    # Get the summary statistics DataFrame
    summary_df = df.describe()

    # Extract summary information, null count, and unique value count for each column
    stats_info = []
    for field in df.schema.fields:
        column_name = field.name
        data_type = str(field.dataType)
        
        # Calculate the null count for the column
        null_count = df.filter(F.col(column_name).isNull()).count()
        
        # Calculate the unique value count for the column
        unique_values_count = df.select(column_name).distinct().count()
        
        # Check if the column has a numeric data type
        if isinstance(field.dataType, (DoubleType, LongType, DecimalType)):
            # Extract the statistics for the numeric column
            count, mean, stddev, min_value, max_value = summary_df.select(column_name).rdd.flatMap(list).collect()
        else:
            # Set the statistics to None for non-numeric columns
            count, mean, stddev, min_value, max_value = None, None, None, None, None
        
        stats_info.append((column_name, data_type, count, null_count, unique_values_count, mean, stddev, min_value, max_value))
    
    # Create a new DataFrame with the schema information, statistics, null counts, and unique value counts
    spark = SparkSession.builder.getOrCreate()
    stats_df = spark.createDataFrame(stats_info, schema=output_schema)

    return stats_df


# def create_sales_trend_percentage_drop_alerts(df):
#     win = (
#         Window
#         .partitionBy(*grouping_cols)
#         .orderBy(F.col(date_col).cast("timestamp").cast("long"))
#     )
#     df_with_alert_ids = (
#         df
#         .withColumn(
#             "percentage_diff_threshold_check_lag",
#             F.lag("percentage_diff_threshold_check", offset=1, default=0).over(win)
#         )
#         .withColumn(
#             "percentage_diff_threshold_check_ne",
#             (F.col("percentage_diff_threshold_check") != F.col("percentage_diff_threshold_check_lag")).cast("int")
#         )
#         .withColumn(
#             "percentage_diff_threshold_check_ne_cumsum",
#             F.sum("percentage_diff_threshold_check_ne").over(win)
#         )
#         .groupby(
#             *grouping_cols, "percentage_diff_threshold_check_ne_cumsum"
#         )
#         .agg(
#             F.min(F.col(date_col)).alias('alert_interval_start'),
#             F.max(F.col(date_col)).alias('alert_interval_end'),
#             F.last('percentage_diff_of_30_vs_90_day_rolling_avg_of_total_sales_value_of_past_30_days')
#         )
#     )
