from pyspark.sql import functions as F
from transforms.api import transform, Input, Output, Check, configure
# from transforms.api import incremental
from transforms import expectations as E
from pyspark.sql import Window

_SPARK_OPTS = [
    'DRIVER_MEMORY_LARGE',
    'EXECUTOR_MEMORY_LARGE',
    'EXECUTOR_MEMORY_OVERHEAD_LARGE',
    'NUM_EXECUTORS_64'
]

# metric_cols = [
#     "count_of_billing_document_items",
#     "count_of_mandt_kunag_|_foreign_key_KNA1",
#     "count_of_mandt_kunrg_|_foreign_key_KNA1",
#     "count_of_mandt_kunrg_vkorg_vtweg_spart_|_foreign_key_KNVV",
#     "sum_of_billed_quantity_|_fkimg",
#     "sum_of_sales_value",
#     "avg_unit_price"
# ]


@configure(profile=_SPARK_OPTS)
# @incremental(
#     require_incremental=_REQUIRE_INCREMENTAL,
#     semantic_version=_SEMANTIC_VERSION,
# )
@transform(
    df=Input("ri.foundry.main.dataset.d1c9e450-487f-4c26-8717-379311b4bbe3"),
    customer_account_material_pair_ts_metrics_df=Output("ri.foundry.main.dataset.195cfffd-a789-4cd9-b671-2d2c78587f44"),
    customer_material_pair_ts_metrics_df=Output("ri.foundry.main.dataset.c6470791-b7e5-483b-b010-8d4e79458f19"),
    material_ts_metrics_df=Output("ri.foundry.main.dataset.23199d7d-2dad-41b6-aa13-f40e884bb16d"),
    customer_account_ts_metrics_df=Output("ri.foundry.main.dataset.e44f3037-9e35-40bc-8ead-dd5cabd81d3d"),
    customer_account_ts_pd_metrics_df=Output("ri.foundry.main.dataset.a536ec0b-96f9-478b-ac94-fc9214cd2a98"),
    customer_account_ts_spd_metrics_df=Output("ri.foundry.main.dataset.258da961-33eb-4c78-ac0d-f0a6b0e0d117"),
    #customer_ts_metrics_df=Output("ri.foundry.main.dataset.1d537706-f74a-42c1-935d-97ac9dcd13d0"),
    #customer_ts_pd_metrics_df=Output("ri.foundry.main.dataset.679c800e-de6f-4738-810d-82d2d466e6d0"),
    #customer_ts_spd_metrics_df=Output("ri.foundry.main.dataset.7ccf7a42-5e26-456a-b674-9e4c66599a28"),
    #customer_shipto_ts_metrics_df=Output("ri.foundry.main.dataset.5f2f799d-1a2b-4da3-a982-87916b3660c1"),
    #customer_shipto_ts_pd_metrics_df=Output("ri.foundry.main.dataset.29cc56e0-40e8-4cc1-a8a7-9a9bf8435805"),
    #customer_shipto_ts_spd_metrics_df=Output("ri.foundry.main.dataset.8722c0fc-e735-41c3-bbdf-2ee2fe6cd2c4"),
    manufacturer_ts_metrics_df=Output("ri.foundry.main.dataset.a4b83191-02ef-4468-98c5-fb098a03cab8"),
    manufacturer_ts_pd_metrics_df=Output("ri.foundry.main.dataset.f8b47cfb-447c-4ae5-b78b-6dc383c4367e"),
    manufacturer_ts_spd_metrics_df=Output("ri.foundry.main.dataset.f1ac3cdb-8210-4ca9-b89c-a04db0a47627")
)
def compute(
    df,
    customer_account_material_pair_ts_metrics_df,
    customer_material_pair_ts_metrics_df,
    material_ts_metrics_df,
    customer_account_ts_metrics_df,
    customer_account_ts_pd_metrics_df,
    customer_account_ts_spd_metrics_df,
    #customer_ts_metrics_df,
    #customer_ts_pd_metrics_df,
    #customer_ts_spd_metrics_df,
    #customer_shipto_ts_metrics_df,
    #customer_shipto_ts_pd_metrics_df,
    #customer_shipto_ts_spd_metrics_df,
    manufacturer_ts_metrics_df,
    manufacturer_ts_pd_metrics_df,
    manufacturer_ts_spd_metrics_df
):
    date_col = 'billing_date_|_fkdat'
    df = df.dataframe()

    # customer_account_material_pair:
    customer_account_material_pair_ts_metrics_df.write_dataframe(agg_customer_account_and_material(df, date_col))

    # customer_material_pair:
    customer_material_pair_ts_metrics_df.write_dataframe(agg_customer_and_material(df, date_col))

    # material:
    material_ts_metrics_df.write_dataframe(agg_material(df, date_col))

    # customer_account:
    customer_account_ts_metrics_df.write_dataframe(agg_customer_account(df, date_col))
    customer_account_ts_pd_metrics_df.write_dataframe(
        agg_customer_account(df, date_col, [(F.col("spd_flag") == F.lit(False))], filter_id='pd_only')
    )
    customer_account_ts_spd_metrics_df.write_dataframe(
        agg_customer_account(df, date_col, [(F.col("spd_flag") == F.lit(True))], filter_id='spd_only')
    )

    # customer:
    '''customer_ts_metrics_df.write_dataframe(agg_customer(df, date_col))
    customer_ts_pd_metrics_df.write_dataframe(
        agg_customer(df, date_col, [(F.col("spd_flag") == F.lit(False))], filter_id='pd_only')
    )
    customer_ts_spd_metrics_df.write_dataframe(
        agg_customer(df, date_col, [(F.col("spd_flag") == F.lit(True))], filter_id='spd_only')
    )

    # customer_shipto:
    customer_shipto_ts_metrics_df.write_dataframe(agg_customer_shipto(df, date_col))
    customer_shipto_ts_pd_metrics_df.write_dataframe(
        agg_customer_shipto(df, date_col, [(F.col("spd_flag") == F.lit(False))], filter_id='pd_only')
    )
    customer_shipto_ts_spd_metrics_df.write_dataframe(
        agg_customer_shipto(df, date_col, [(F.col("spd_flag") == F.lit(True))], filter_id='spd_only')
    )'''

    # manufacturer:
    manufacturer_ts_metrics_df.write_dataframe(agg_manufacturer(df, date_col))
    manufacturer_ts_pd_metrics_df.write_dataframe(
        agg_manufacturer(df, date_col, [(F.col("spd_flag") == F.lit(False))], filter_id='pd_only')
    )
    manufacturer_ts_spd_metrics_df.write_dataframe(
        agg_manufacturer(df, date_col, [(F.col("spd_flag") == F.lit(True))], filter_id='spd_only')
    )


def agg_customer_account_and_material(df, date_col, df_filters=[], filter_id='all'):
    # AGGREGATION: customer_account + material
    group_type_id = 'customer_account_material_pair'
    grouping_cols = [
        F.col("mandt_kunrg_vkorg_vtweg_spart_|_foreign_key_KNVV"),
        F.col('mandt_matnr_|_foreign_key_MARA')
    ]
    def days(i):
        return i * 86400  # amount seconds in number of days

    cumulative_window = (
        Window
        .partitionBy(*grouping_cols)
        .orderBy(F.col(date_col).cast("timestamp").cast("long"))
    )

    moving_10_day_window = (
        Window
        .partitionBy(*grouping_cols)
        .orderBy(F.col(date_col).cast("timestamp").cast("long"))
        .rangeBetween(-days(10), Window.currentRow)
    )

    moving_30_day_window = (
        Window
        .partitionBy(*grouping_cols)
        .orderBy(F.col(date_col).cast("timestamp").cast("long"))
        .rangeBetween(-days(30), Window.currentRow)
    )

    moving_90_day_window = (
        Window
        .partitionBy(*grouping_cols)
        .orderBy(F.col(date_col).cast("timestamp").cast("long"))
        .rangeBetween(-days(90), Window.currentRow)
    )

    for df_filter in df_filters:
        df = df.filter(df_filter)

    df2 = (
        df
        .groupby(date_col, *grouping_cols)
        .agg(
            F.sum('count_of_billing_document_items').alias('count_of_billing_document_items'),
            F.sum('sum_of_billed_quantity_|_fkimg').alias('sum_of_billed_quantity_|_fkimg'),
            F.sum('sum_of_sales_value').alias('sum_of_sales_value'),
            F.array_distinct(
                F.flatten(F.collect_set(F.col('set_of_mandt_kunag_|_foreign_key_KNA1')))
            ).alias('set_of_mandt_kunag_|_foreign_key_KNA1'),
            F.array_distinct(
                F.flatten(F.collect_set(F.col('set_of_mandt_kunrg_|_foreign_key_KNA1')))
            ).alias('set_of_mandt_kunrg_|_foreign_key_KNA1'),
            # F.array_distinct(
            #     F.flatten(F.collect_set(F.col('set_of_mandt_kunrg_vkorg_vtweg_spart_|_foreign_key_KNVV')))
            # ).alias('set_of_mandt_kunrg_vkorg_vtweg_spart_|_foreign_key_KNVV'),
        )
        .withColumn(date_col, F.col(date_col).cast("timestamp"))
        .withColumn('_group_id', F.concat_ws('_||_', *grouping_cols))
        .withColumn('_group_type_id', F.lit(group_type_id))
        .withColumn('_filter_id', F.lit(filter_id))
        .withColumn('_time_series_group_id', F.concat_ws('_||_', F.col('_group_type_id'), F.col('_filter_id'), F.col('_group_id')))
        .withColumn('primary_key', F.concat_ws('_||_', F.col(date_col).cast('string'), F.col('_time_series_group_id')))
        .withColumn(
            'count_of_mandt_kunag_|_foreign_key_KNA1',
            F.size('set_of_mandt_kunag_|_foreign_key_KNA1')
        )
        .withColumn(
            'count_of_mandt_kunrg_|_foreign_key_KNA1',
            F.size('set_of_mandt_kunrg_|_foreign_key_KNA1')
        )
        # .withColumn(
        #     'count_of_mandt_kunrg_vkorg_vtweg_spart_|_foreign_key_KNVV',
        #     F.size('set_of_mandt_kunrg_vkorg_vtweg_spart_|_foreign_key_KNVV')
        # )
        .withColumn(
            'avg_unit_price',
            F.col('sum_of_sales_value') / F.col('sum_of_billed_quantity_|_fkimg')
        )
        .withColumn(
            'total_sales_value_of_past_30_days',
            F.sum('sum_of_sales_value').over(moving_30_day_window)
        )
        .withColumn(
            'rolling_30_day_avg_of_total_sales_value_of_past_30_days',
            F.avg('total_sales_value_of_past_30_days').over(moving_30_day_window)
        )
        .withColumn(
            'rolling_90_day_avg_of_total_sales_value_of_past_30_days',
            F.avg('total_sales_value_of_past_30_days').over(moving_90_day_window)
        )
        .withColumn(
            'rolling_30_day_avg_of_daily_sales_value',
            F.avg('sum_of_sales_value').over(moving_30_day_window)
        )
        .withColumn(
            'rolling_90_day_avg_of_daily_sales_value',
            F.avg('sum_of_sales_value').over(moving_90_day_window)
        )
        .withColumn(
            'absolute_diff_of_30_vs_90_day_rolling_avg_of_total_sales_value_of_past_30_days',
            F.col('rolling_30_day_avg_of_total_sales_value_of_past_30_days') - F.col('rolling_90_day_avg_of_total_sales_value_of_past_30_days')
        )
        .withColumn(
            'percentage_diff_of_30_vs_90_day_rolling_avg_of_total_sales_value_of_past_30_days',
            F.col('absolute_diff_of_30_vs_90_day_rolling_avg_of_total_sales_value_of_past_30_days') / F.abs(F.col('rolling_90_day_avg_of_total_sales_value_of_past_30_days'))
        )
    )
    return df2


def agg_customer_and_material(df, date_col, df_filters=[], filter_id='all'):
    # AGGREGATION: customer + material
    group_type_id = 'customer_material_pair'
    grouping_cols = [
        F.col("mandt_kunag_|_foreign_key_KNA1"),
        F.col('mandt_matnr_|_foreign_key_MARA')
    ]
    def days(i):
        return i * 86400  # amount seconds in number of days

    cumulative_window = (
        Window
        .partitionBy(*grouping_cols)
        .orderBy(F.col(date_col).cast("timestamp").cast("long"))
    )

    moving_10_day_window = (
        Window
        .partitionBy(*grouping_cols)
        .orderBy(F.col(date_col).cast("timestamp").cast("long"))
        .rangeBetween(-days(10), Window.currentRow)
    )

    moving_30_day_window = (
        Window
        .partitionBy(*grouping_cols)
        .orderBy(F.col(date_col).cast("timestamp").cast("long"))
        .rangeBetween(-days(30), Window.currentRow)
    )

    moving_90_day_window = (
        Window
        .partitionBy(*grouping_cols)
        .orderBy(F.col(date_col).cast("timestamp").cast("long"))
        .rangeBetween(-days(90), Window.currentRow)
    )

    for df_filter in df_filters:
        df = df.filter(df_filter)

    df2 = (
        df
        .groupby(date_col, *grouping_cols)
        .agg(
            F.sum('count_of_billing_document_items').alias('count_of_billing_document_items'),
            F.sum('sum_of_billed_quantity_|_fkimg').alias('sum_of_billed_quantity_|_fkimg'),
            F.sum('sum_of_sales_value').alias('sum_of_sales_value'),
            # F.array_distinct(
            #     F.flatten(F.collect_set(F.col('set_of_mandt_kunag_|_foreign_key_KNA1')))
            # ).alias('set_of_mandt_kunag_|_foreign_key_KNA1'),
            F.array_distinct(
                F.flatten(F.collect_set(F.col('set_of_mandt_kunrg_|_foreign_key_KNA1')))
            ).alias('set_of_mandt_kunrg_|_foreign_key_KNA1'),
            F.array_distinct(
                F.flatten(F.collect_set(F.col('set_of_mandt_kunrg_vkorg_vtweg_spart_|_foreign_key_KNVV')))
            ).alias('set_of_mandt_kunrg_vkorg_vtweg_spart_|_foreign_key_KNVV'),
        )
        .withColumn(date_col, F.col(date_col).cast("timestamp"))
        .withColumn('_group_id', F.concat_ws('_||_', *grouping_cols))
        .withColumn('_group_type_id', F.lit(group_type_id))
        .withColumn('_filter_id', F.lit(filter_id))
        .withColumn('_time_series_group_id', F.concat_ws('_||_', F.col('_group_type_id'), F.col('_filter_id'), F.col('_group_id')))
        .withColumn('primary_key', F.concat_ws('_||_', F.col(date_col).cast('string'), F.col('_time_series_group_id')))
        # .withColumn(
        #     'count_of_mandt_kunag_|_foreign_key_KNA1',
        #     F.size('set_of_mandt_kunag_|_foreign_key_KNA1')
        # )
        .withColumn(
            'count_of_mandt_kunrg_|_foreign_key_KNA1',
            F.size('set_of_mandt_kunrg_|_foreign_key_KNA1')
        )
        .withColumn(
            'count_of_mandt_kunrg_vkorg_vtweg_spart_|_foreign_key_KNVV',
            F.size('set_of_mandt_kunrg_vkorg_vtweg_spart_|_foreign_key_KNVV')
        )
        .withColumn(
            'avg_unit_price',
            F.col('sum_of_sales_value') / F.col('sum_of_billed_quantity_|_fkimg')
        )
        .withColumn(
            'total_sales_value_of_past_30_days',
            F.sum('sum_of_sales_value').over(moving_30_day_window)
        )
        .withColumn(
            'rolling_30_day_avg_of_total_sales_value_of_past_30_days',
            F.avg('total_sales_value_of_past_30_days').over(moving_30_day_window)
        )
        .withColumn(
            'rolling_90_day_avg_of_total_sales_value_of_past_30_days',
            F.avg('total_sales_value_of_past_30_days').over(moving_90_day_window)
        )
        .withColumn(
            'rolling_30_day_avg_of_daily_sales_value',
            F.avg('sum_of_sales_value').over(moving_30_day_window)
        )
        .withColumn(
            'rolling_90_day_avg_of_daily_sales_value',
            F.avg('sum_of_sales_value').over(moving_90_day_window)
        )
        .withColumn(
            'absolute_diff_of_30_vs_90_day_rolling_avg_of_total_sales_value_of_past_30_days',
            F.col('rolling_30_day_avg_of_total_sales_value_of_past_30_days') - F.col('rolling_90_day_avg_of_total_sales_value_of_past_30_days')
        )
        .withColumn(
            'percentage_diff_of_30_vs_90_day_rolling_avg_of_total_sales_value_of_past_30_days',
            F.col('absolute_diff_of_30_vs_90_day_rolling_avg_of_total_sales_value_of_past_30_days') / F.abs(F.col('rolling_90_day_avg_of_total_sales_value_of_past_30_days'))
        )
    )
    return df2


def agg_material(df, date_col,  df_filters=[], filter_id='all'):
    # AGGREGATION: material
    group_type_id = 'material'
    grouping_cols = [
        F.col('mandt_matnr_|_foreign_key_MARA')
    ]

    def days(i):
        return i * 86400  # amount seconds in number of days

    cumulative_window = (
        Window
        .partitionBy(*grouping_cols)
        .orderBy(F.col(date_col).cast("timestamp").cast("long"))
    )

    moving_10_day_window = (
        Window
        .partitionBy(*grouping_cols)
        .orderBy(F.col(date_col).cast("timestamp").cast("long"))
        .rangeBetween(-days(10), Window.currentRow)
    )

    moving_30_day_window = (
        Window
        .partitionBy(*grouping_cols)
        .orderBy(F.col(date_col).cast("timestamp").cast("long"))
        .rangeBetween(-days(30), Window.currentRow)
    )

    moving_90_day_window = (
        Window
        .partitionBy(*grouping_cols)
        .orderBy(F.col(date_col).cast("timestamp").cast("long"))
        .rangeBetween(-days(90), Window.currentRow)
    )

    for df_filter in df_filters:
        df = df.filter(df_filter)

    df2 = (
        df
        .groupby(date_col, *grouping_cols)
        .agg(
            F.sum('count_of_billing_document_items').alias('count_of_billing_document_items'),
            F.sum('sum_of_billed_quantity_|_fkimg').alias('sum_of_billed_quantity_|_fkimg'),
            F.sum('sum_of_sales_value').alias('sum_of_sales_value'),
            F.array_distinct(
                F.flatten(F.collect_set(F.col('set_of_mandt_kunag_|_foreign_key_KNA1')))
            ).alias('set_of_mandt_kunag_|_foreign_key_KNA1'),
            F.array_distinct(
                F.flatten(F.collect_set(F.col('set_of_mandt_kunrg_|_foreign_key_KNA1')))
            ).alias('set_of_mandt_kunrg_|_foreign_key_KNA1'),
            F.array_distinct(
                F.flatten(F.collect_set(F.col('set_of_mandt_kunrg_vkorg_vtweg_spart_|_foreign_key_KNVV')))
            ).alias('set_of_mandt_kunrg_vkorg_vtweg_spart_|_foreign_key_KNVV'),
        )
        .withColumn(date_col, F.col(date_col).cast("timestamp"))
        .withColumn('_group_id', F.concat_ws('_||_', *grouping_cols))
        .withColumn('_group_type_id', F.lit(group_type_id))
        .withColumn('_filter_id', F.lit(filter_id))
        .withColumn('_time_series_group_id', F.concat_ws('_||_', F.col('_group_type_id'), F.col('_filter_id'), F.col('_group_id')))
        .withColumn('primary_key', F.concat_ws('_||_', F.col(date_col).cast('string'), F.col('_time_series_group_id')))
        .withColumn(
            'count_of_mandt_kunag_|_foreign_key_KNA1',
            F.size('set_of_mandt_kunag_|_foreign_key_KNA1')
        )
        .withColumn(
            'count_of_mandt_kunrg_|_foreign_key_KNA1',
            F.size('set_of_mandt_kunrg_|_foreign_key_KNA1')
        )
        .withColumn(
            'count_of_mandt_kunrg_vkorg_vtweg_spart_|_foreign_key_KNVV',
            F.size('set_of_mandt_kunrg_vkorg_vtweg_spart_|_foreign_key_KNVV')
        )
        .withColumn(
            'avg_unit_price',
            F.col('sum_of_sales_value') / F.col('sum_of_billed_quantity_|_fkimg')
        )
        .withColumn(
            'cumulative_set_of_mandt_kunag_|_foreign_key_KNA1',
            F.array_distinct(
                F.flatten(
                    F.collect_set(
                        F.col('set_of_mandt_kunag_|_foreign_key_KNA1')
                    ).over(cumulative_window)
                )
            ),
        )
        .withColumn(
            'size_of_cumulative_set_of_mandt_kunag_|_foreign_key_KNA1',
            F.size('cumulative_set_of_mandt_kunag_|_foreign_key_KNA1')
        )
        .withColumn(
            'lagged_size_of_cumulative_set_of_mandt_kunag_|_foreign_key_KNA1',
            F.lag('size_of_cumulative_set_of_mandt_kunag_|_foreign_key_KNA1').over(cumulative_window)
        )
        .withColumn(
            'cumulative_set_of_mandt_kunrg_|_foreign_key_KNA1',
            F.array_distinct(
                F.flatten(
                    F.collect_set(
                        F.col('set_of_mandt_kunrg_|_foreign_key_KNA1')
                    ).over(cumulative_window)
                )
            ),
        )
        .withColumn(
            'size_of_cumulative_set_of_mandt_kunrg_|_foreign_key_KNA1',
            F.size('cumulative_set_of_mandt_kunrg_|_foreign_key_KNA1')
        )
        .withColumn(
            'lagged_size_of_cumulative_set_of_mandt_kunrg_|_foreign_key_KNA1',
            F.lag('size_of_cumulative_set_of_mandt_kunrg_|_foreign_key_KNA1').over(cumulative_window)
        )
        .withColumn(
            'cumulative_set_of_mandt_kunrg_vkorg_vtweg_spart_|_foreign_key_KNVV',
            F.array_distinct(
                F.flatten(
                    F.collect_set(
                        F.col('set_of_mandt_kunrg_vkorg_vtweg_spart_|_foreign_key_KNVV')
                    ).over(cumulative_window)
                )
            ),
        )
        .withColumn(
            'size_of_cumulative_set_of_mandt_kunrg_vkorg_vtweg_spart_|_foreign_key_KNVV',
            F.size('cumulative_set_of_mandt_kunrg_vkorg_vtweg_spart_|_foreign_key_KNVV')
        )
        .withColumn(
            'lagged_size_of_cumulative_set_of_mandt_kunrg_vkorg_vtweg_spart_|_foreign_key_KNVV',
            F.lag('size_of_cumulative_set_of_mandt_kunrg_vkorg_vtweg_spart_|_foreign_key_KNVV').over(cumulative_window)
        )
        .withColumn(
            'total_sales_value_of_past_30_days',
            F.sum('sum_of_sales_value').over(moving_30_day_window)
        )
        .withColumn(
            'rolling_30_day_avg_of_total_sales_value_of_past_30_days',
            F.avg('total_sales_value_of_past_30_days').over(moving_30_day_window)
        )
        .withColumn(
            'rolling_90_day_avg_of_total_sales_value_of_past_30_days',
            F.avg('total_sales_value_of_past_30_days').over(moving_90_day_window)
        )
        .withColumn(
            'rolling_30_day_avg_of_daily_sales_value',
            F.avg('sum_of_sales_value').over(moving_30_day_window)
        )
        .withColumn(
            'rolling_90_day_avg_of_daily_sales_value',
            F.avg('sum_of_sales_value').over(moving_90_day_window)
        )
        .withColumn(
            'absolute_diff_of_30_vs_90_day_rolling_avg_of_total_sales_value_of_past_30_days',
            F.col('rolling_30_day_avg_of_total_sales_value_of_past_30_days') - F.col('rolling_90_day_avg_of_total_sales_value_of_past_30_days')
        )
        .withColumn(
            'percentage_diff_of_30_vs_90_day_rolling_avg_of_total_sales_value_of_past_30_days',
            F.col('absolute_diff_of_30_vs_90_day_rolling_avg_of_total_sales_value_of_past_30_days') / F.abs(F.col('rolling_90_day_avg_of_total_sales_value_of_past_30_days'))
        )
    )
    return df2


def agg_customer_account(df, date_col, df_filters=[], filter_id='all'):
    # AGGREGATION: customer_account
    group_type_id = 'customer_account'
    grouping_cols = [
        F.col("mandt_kunrg_vkorg_vtweg_spart_|_foreign_key_KNVV")
    ]

    def days(i):
        return i * 86400  # amount seconds in number of days

    cumulative_window = (
        Window
        .partitionBy(*grouping_cols)
        .orderBy(F.col(date_col).cast("timestamp").cast("long"))
    )

    moving_10_day_window = (
        Window
        .partitionBy(*grouping_cols)
        .orderBy(F.col(date_col).cast("timestamp").cast("long"))
        .rangeBetween(-days(10), Window.currentRow)
    )

    moving_30_day_window = (
        Window
        .partitionBy(*grouping_cols)
        .orderBy(F.col(date_col).cast("timestamp").cast("long"))
        .rangeBetween(-days(30), Window.currentRow)
    )

    moving_90_day_window = (
        Window
        .partitionBy(*grouping_cols)
        .orderBy(F.col(date_col).cast("timestamp").cast("long"))
        .rangeBetween(-days(90), Window.currentRow)
    )

    for df_filter in df_filters:
        df = df.filter(df_filter)

    df2 = (
        df
        .groupby(date_col, *grouping_cols)
        .agg(
            F.sum('count_of_billing_document_items').alias('count_of_billing_document_items'),
            F.sum('sum_of_billed_quantity_|_fkimg').alias('sum_of_billed_quantity_|_fkimg'),
            F.sum('sum_of_sales_value').alias('sum_of_sales_value'),
            F.array_distinct(
                F.flatten(F.collect_set(F.col('set_of_mandt_matnr_|_foreign_key_MARA')))
            ).alias('set_of_mandt_matnr_|_foreign_key_MARA'),
            F.array_distinct(
                F.flatten(F.collect_set(F.col('set_of_mandt_kunag_|_foreign_key_KNA1')))
            ).alias('set_of_mandt_kunag_|_foreign_key_KNA1'),
            F.array_distinct(
                F.flatten(F.collect_set(F.col('set_of_mandt_kunrg_|_foreign_key_KNA1')))
            ).alias('set_of_mandt_kunrg_|_foreign_key_KNA1'),
            # F.array_distinct(
            #     F.flatten(F.collect_set(F.col('set_of_mandt_kunrg_vkorg_vtweg_spart_|_foreign_key_KNVV')))
            # ).alias('set_of_mandt_kunrg_vkorg_vtweg_spart_|_foreign_key_KNVV'),
        )
        .withColumn(date_col, F.col(date_col).cast("timestamp"))
        .withColumn('_group_id', F.concat_ws('_||_', *grouping_cols))
        .withColumn('_group_type_id', F.lit(group_type_id))
        .withColumn('_filter_id', F.lit(filter_id))
        .withColumn('_time_series_group_id', F.concat_ws('_||_', F.col('_group_type_id'), F.col('_filter_id'), F.col('_group_id')))
        .withColumn('primary_key', F.concat_ws('_||_', F.col(date_col).cast('string'), F.col('_time_series_group_id')))
        .withColumn(
            'count_of_mandt_matnr_|_foreign_key_MARA',
            F.size('set_of_mandt_matnr_|_foreign_key_MARA')
        )
        .withColumn(
            'count_of_mandt_kunag_|_foreign_key_KNA1',
            F.size('set_of_mandt_kunag_|_foreign_key_KNA1')
        )
        .withColumn(
            'count_of_mandt_kunrg_|_foreign_key_KNA1',
            F.size('set_of_mandt_kunrg_|_foreign_key_KNA1')
        )
        # .withColumn(
        #     'count_of_mandt_kunrg_vkorg_vtweg_spart_|_foreign_key_KNVV',
        #     F.size('set_of_mandt_kunrg_vkorg_vtweg_spart_|_foreign_key_KNVV')
        # )
        .withColumn(
            'avg_unit_price',
            F.col('sum_of_sales_value') / F.col('sum_of_billed_quantity_|_fkimg')
        )
        .withColumn(
            'cumulative_set_of_mandt_matnr_|_foreign_key_MARA',
            F.array_distinct(
                F.flatten(
                    F.collect_set(
                        F.col('set_of_mandt_matnr_|_foreign_key_MARA')
                    ).over(cumulative_window)
                )
            ),
        )
        .withColumn(
            'size_of_cumulative_set_of_mandt_matnr_|_foreign_key_MARA',
            F.size('cumulative_set_of_mandt_matnr_|_foreign_key_MARA')
        )
        .withColumn(
            'lagged_size_of_cumulative_set_of_mandt_matnr_|_foreign_key_MARA',
            F.lag('size_of_cumulative_set_of_mandt_matnr_|_foreign_key_MARA').over(cumulative_window)
        )
        .withColumn(
            'total_sales_value_of_past_30_days',
            F.sum('sum_of_sales_value').over(moving_30_day_window)
        )
        .withColumn(
            'rolling_30_day_avg_of_total_sales_value_of_past_30_days',
            F.avg('total_sales_value_of_past_30_days').over(moving_30_day_window)
        )
        .withColumn(
            'rolling_90_day_avg_of_total_sales_value_of_past_30_days',
            F.avg('total_sales_value_of_past_30_days').over(moving_90_day_window)
        )
        .withColumn(
            'rolling_30_day_avg_of_daily_sales_value',
            F.avg('sum_of_sales_value').over(moving_30_day_window)
        )
        .withColumn(
            'rolling_90_day_avg_of_daily_sales_value',
            F.avg('sum_of_sales_value').over(moving_90_day_window)
        )
        .withColumn(
            'absolute_diff_of_30_vs_90_day_rolling_avg_of_total_sales_value_of_past_30_days',
            F.col('rolling_30_day_avg_of_total_sales_value_of_past_30_days') - F.col('rolling_90_day_avg_of_total_sales_value_of_past_30_days')
        )
        .withColumn(
            'percentage_diff_of_30_vs_90_day_rolling_avg_of_total_sales_value_of_past_30_days',
            F.col('absolute_diff_of_30_vs_90_day_rolling_avg_of_total_sales_value_of_past_30_days') / F.abs(F.col('rolling_90_day_avg_of_total_sales_value_of_past_30_days'))
        )
    )
    return df2


'''def agg_customer(df, date_col, df_filters=[], filter_id='all'):
    # AGGREGATION: customer
    group_type_id = 'customer'
    grouping_cols = [
        F.col('mandt_kunag_|_foreign_key_KNA1')
    ]

    def days(i):
        return i * 86400  # amount seconds in number of days

    cumulative_window = (
        Window
        .partitionBy(*grouping_cols)
        .orderBy(F.col(date_col).cast("timestamp").cast("long"))
    )

    moving_10_day_window = (
        Window
        .partitionBy(*grouping_cols)
        .orderBy(F.col(date_col).cast("timestamp").cast("long"))
        .rangeBetween(-days(10), Window.currentRow)
    )

    moving_30_day_window = (
        Window
        .partitionBy(*grouping_cols)
        .orderBy(F.col(date_col).cast("timestamp").cast("long"))
        .rangeBetween(-days(30), Window.currentRow)
    )

    moving_90_day_window = (
        Window
        .partitionBy(*grouping_cols)
        .orderBy(F.col(date_col).cast("timestamp").cast("long"))
        .rangeBetween(-days(90), Window.currentRow)
    )

    for df_filter in df_filters:
        df = df.filter(df_filter)

    df2 = (
        df
        .groupby(date_col, *grouping_cols)
        .agg(
            F.sum('count_of_billing_document_items').alias('count_of_billing_document_items'),
            F.sum('sum_of_billed_quantity_|_fkimg').alias('sum_of_billed_quantity_|_fkimg'),
            F.sum('sum_of_sales_value').alias('sum_of_sales_value'),
            F.array_distinct(
                F.flatten(F.collect_set(F.col('set_of_mandt_matnr_|_foreign_key_MARA')))
            ).alias('set_of_mandt_matnr_|_foreign_key_MARA'),
            F.array_distinct(
                F.flatten(F.collect_set(F.col('set_of_mandt_kunag_|_foreign_key_KNA1')))
            ).alias('set_of_mandt_kunag_|_foreign_key_KNA1'),
            F.array_distinct(
                F.flatten(F.collect_set(F.col('set_of_mandt_kunrg_|_foreign_key_KNA1')))
            ).alias('set_of_mandt_kunrg_|_foreign_key_KNA1'),
            # F.array_distinct(
            #     F.flatten(F.collect_set(F.col('set_of_mandt_kunrg_vkorg_vtweg_spart_|_foreign_key_KNVV')))
            # ).alias('set_of_mandt_kunrg_vkorg_vtweg_spart_|_foreign_key_KNVV'),
        )
        .withColumn(date_col, F.col(date_col).cast("timestamp"))
        .withColumn('_group_id', F.concat_ws('_||_', *grouping_cols))
        .withColumn('_group_type_id', F.lit(group_type_id))
        .withColumn('_filter_id', F.lit(filter_id))
        .withColumn('_time_series_group_id', F.concat_ws('_||_', F.col('_group_type_id'), F.col('_filter_id'), F.col('_group_id')))
        .withColumn('primary_key', F.concat_ws('_||_', F.col(date_col).cast('string'), F.col('_time_series_group_id')))
        .withColumn(
            'count_of_mandt_matnr_|_foreign_key_MARA',
            F.size('set_of_mandt_matnr_|_foreign_key_MARA')
        )
        .withColumn(
            'count_of_mandt_kunag_|_foreign_key_KNA1',
            F.size('set_of_mandt_kunag_|_foreign_key_KNA1')
        )
        .withColumn(
            'count_of_mandt_kunrg_|_foreign_key_KNA1',
            F.size('set_of_mandt_kunrg_|_foreign_key_KNA1')
        )
        # .withColumn(
        #     'count_of_mandt_kunrg_vkorg_vtweg_spart_|_foreign_key_KNVV',
        #     F.size('set_of_mandt_kunrg_vkorg_vtweg_spart_|_foreign_key_KNVV')
        # )
        .withColumn(
            'avg_unit_price',
            F.col('sum_of_sales_value') / F.col('sum_of_billed_quantity_|_fkimg')
        )
        .withColumn(
            'cumulative_set_of_mandt_matnr_|_foreign_key_MARA',
            F.array_distinct(
                F.flatten(
                    F.collect_set(
                        F.col('set_of_mandt_matnr_|_foreign_key_MARA')
                    ).over(cumulative_window)
                )
            ),
        )
        .withColumn(
            'size_of_cumulative_set_of_mandt_matnr_|_foreign_key_MARA',
            F.size('cumulative_set_of_mandt_matnr_|_foreign_key_MARA')
        )
        .withColumn(
            'lagged_size_of_cumulative_set_of_mandt_matnr_|_foreign_key_MARA',
            F.lag('size_of_cumulative_set_of_mandt_matnr_|_foreign_key_MARA').over(cumulative_window)
        )
        .withColumn(
            'total_sales_value_of_past_30_days',
            F.sum('sum_of_sales_value').over(moving_30_day_window)
        )
        .withColumn(
            'rolling_30_day_avg_of_total_sales_value_of_past_30_days',
            F.avg('total_sales_value_of_past_30_days').over(moving_30_day_window)
        )
        .withColumn(
            'rolling_90_day_avg_of_total_sales_value_of_past_30_days',
            F.avg('total_sales_value_of_past_30_days').over(moving_90_day_window)
        )
        .withColumn(
            'rolling_30_day_avg_of_daily_sales_value',
            F.avg('sum_of_sales_value').over(moving_30_day_window)
        )
        .withColumn(
            'rolling_90_day_avg_of_daily_sales_value',
            F.avg('sum_of_sales_value').over(moving_90_day_window)
        )
        .withColumn(
            'absolute_diff_of_30_vs_90_day_rolling_avg_of_total_sales_value_of_past_30_days',
            F.col('rolling_30_day_avg_of_total_sales_value_of_past_30_days') - F.col('rolling_90_day_avg_of_total_sales_value_of_past_30_days')
        )
        .withColumn(
            'percentage_diff_of_30_vs_90_day_rolling_avg_of_total_sales_value_of_past_30_days',
            F.col('absolute_diff_of_30_vs_90_day_rolling_avg_of_total_sales_value_of_past_30_days') / F.abs(F.col('rolling_90_day_avg_of_total_sales_value_of_past_30_days'))
        )
    )
    return df2


def agg_customer_shipto(df, date_col, df_filters=[], filter_id='all'):
    # AGGREGATION: customer_shipto
    group_type_id = 'customer_shipto'
    grouping_cols = [
        F.col("mandt_kunnr_shipto_|_foreign_key_KNA1")
    ]
    def days(i):
        return i * 86400  # amount seconds in number of days

    cumulative_window = (
        Window
        .partitionBy(*grouping_cols)
        .orderBy(F.col(date_col).cast("timestamp").cast("long"))
    )

    moving_10_day_window = (
        Window
        .partitionBy(*grouping_cols)
        .orderBy(F.col(date_col).cast("timestamp").cast("long"))
        .rangeBetween(-days(10), Window.currentRow)
    )

    moving_30_day_window = (
        Window
        .partitionBy(*grouping_cols)
        .orderBy(F.col(date_col).cast("timestamp").cast("long"))
        .rangeBetween(-days(30), Window.currentRow)
    )

    moving_90_day_window = (
        Window
        .partitionBy(*grouping_cols)
        .orderBy(F.col(date_col).cast("timestamp").cast("long"))
        .rangeBetween(-days(90), Window.currentRow)
    )

    for df_filter in df_filters:
        df = df.filter(df_filter)

    df2 = (
        df
        .groupby(date_col, *grouping_cols)
        .agg(
            F.sum('count_of_billing_document_items').alias('count_of_billing_document_items'),
            F.sum('sum_of_billed_quantity_|_fkimg').alias('sum_of_billed_quantity_|_fkimg'),
            F.sum('sum_of_sales_value').alias('sum_of_sales_value'),
            F.array_distinct(
                F.flatten(F.collect_set(F.col('set_of_mandt_matnr_|_foreign_key_MARA')))
            ).alias('set_of_mandt_matnr_|_foreign_key_MARA'),
            F.array_distinct(
                F.flatten(F.collect_set(F.col('set_of_mandt_kunag_|_foreign_key_KNA1')))
            ).alias('set_of_mandt_kunag_|_foreign_key_KNA1'),
            F.array_distinct(
                F.flatten(F.collect_set(F.col('set_of_mandt_kunrg_|_foreign_key_KNA1')))
            ).alias('set_of_mandt_kunrg_|_foreign_key_KNA1'),
            # F.array_distinct(
            #     F.flatten(F.collect_set(F.col('set_of_mandt_kunrg_vkorg_vtweg_spart_|_foreign_key_KNVV')))
            # ).alias('set_of_mandt_kunrg_vkorg_vtweg_spart_|_foreign_key_KNVV'),
        )
        .withColumn(date_col, F.col(date_col).cast("timestamp"))
        .withColumn('_group_id', F.concat_ws('_||_', *grouping_cols))
        .withColumn('_group_type_id', F.lit(group_type_id))
        .withColumn('_filter_id', F.lit(filter_id))
        .withColumn('_time_series_group_id', F.concat_ws('_||_', F.col('_group_type_id'), F.col('_filter_id'), F.col('_group_id')))
        .withColumn('primary_key', F.concat_ws('_||_', F.col(date_col).cast('string'), F.col('_time_series_group_id')))
        .withColumn(
            'count_of_mandt_matnr_|_foreign_key_MARA',
            F.size('set_of_mandt_matnr_|_foreign_key_MARA')
        )
        .withColumn(
            'count_of_mandt_kunag_|_foreign_key_KNA1',
            F.size('set_of_mandt_kunag_|_foreign_key_KNA1')
        )
        .withColumn(
            'count_of_mandt_kunrg_|_foreign_key_KNA1',
            F.size('set_of_mandt_kunrg_|_foreign_key_KNA1')
        )
        # .withColumn(
        #     'count_of_mandt_kunrg_vkorg_vtweg_spart_|_foreign_key_KNVV',
        #     F.size('set_of_mandt_kunrg_vkorg_vtweg_spart_|_foreign_key_KNVV')
        # )
        .withColumn(
            'avg_unit_price',
            F.col('sum_of_sales_value') / F.col('sum_of_billed_quantity_|_fkimg')
        )
        .withColumn(
            'cumulative_set_of_mandt_matnr_|_foreign_key_MARA',
            F.array_distinct(
                F.flatten(
                    F.collect_set(
                        F.col('set_of_mandt_matnr_|_foreign_key_MARA')
                    ).over(cumulative_window)
                )
            ),
        )
        .withColumn(
            'size_of_cumulative_set_of_mandt_matnr_|_foreign_key_MARA',
            F.size('cumulative_set_of_mandt_matnr_|_foreign_key_MARA')
        )
        .withColumn(
            'lagged_size_of_cumulative_set_of_mandt_matnr_|_foreign_key_MARA',
            F.lag('size_of_cumulative_set_of_mandt_matnr_|_foreign_key_MARA').over(cumulative_window)
        )
        .withColumn(
            'total_sales_value_of_past_30_days',
            F.sum('sum_of_sales_value').over(moving_30_day_window)
        )
        .withColumn(
            'rolling_30_day_avg_of_total_sales_value_of_past_30_days',
            F.avg('total_sales_value_of_past_30_days').over(moving_30_day_window)
        )
        .withColumn(
            'rolling_90_day_avg_of_total_sales_value_of_past_30_days',
            F.avg('total_sales_value_of_past_30_days').over(moving_90_day_window)
        )
        .withColumn(
            'rolling_30_day_avg_of_daily_sales_value',
            F.avg('sum_of_sales_value').over(moving_30_day_window)
        )
        .withColumn(
            'rolling_90_day_avg_of_daily_sales_value',
            F.avg('sum_of_sales_value').over(moving_90_day_window)
        )
        .withColumn(
            'absolute_diff_of_30_vs_90_day_rolling_avg_of_total_sales_value_of_past_30_days',
            F.col('rolling_30_day_avg_of_total_sales_value_of_past_30_days') - F.col('rolling_90_day_avg_of_total_sales_value_of_past_30_days')
        )
        .withColumn(
            'percentage_diff_of_30_vs_90_day_rolling_avg_of_total_sales_value_of_past_30_days',
            F.col('absolute_diff_of_30_vs_90_day_rolling_avg_of_total_sales_value_of_past_30_days') / F.abs(F.col('rolling_90_day_avg_of_total_sales_value_of_past_30_days'))
        )
    )
    return df2
'''


def agg_manufacturer(df, date_col, df_filters=[], filter_id='all'):
    # AGGREGATION: manufacturer
    group_type_id = 'manufacturer'
    grouping_cols = [
        F.col("mandt_mfrnr_|_foreign_key_LFA1")
    ]

    def days(i):
        return i * 86400  # amount seconds in number of days

    cumulative_window = (
        Window
        .partitionBy(*grouping_cols)
        .orderBy(F.col(date_col).cast("timestamp").cast("long"))
    )

    moving_10_day_window = (
        Window
        .partitionBy(*grouping_cols)
        .orderBy(F.col(date_col).cast("timestamp").cast("long"))
        .rangeBetween(-days(10), Window.currentRow)
    )

    moving_30_day_window = (
        Window
        .partitionBy(*grouping_cols)
        .orderBy(F.col(date_col).cast("timestamp").cast("long"))
        .rangeBetween(-days(30), Window.currentRow)
    )

    moving_90_day_window = (
        Window
        .partitionBy(*grouping_cols)
        .orderBy(F.col(date_col).cast("timestamp").cast("long"))
        .rangeBetween(-days(90), Window.currentRow)
    )

    for df_filter in df_filters:
        df = df.filter(df_filter)

    df2 = (
        df
        .groupby(date_col, *grouping_cols)
        .agg(
            F.sum('count_of_billing_document_items').alias('count_of_billing_document_items'),
            F.sum('sum_of_billed_quantity_|_fkimg').alias('sum_of_billed_quantity_|_fkimg'),
            F.sum('sum_of_sales_value').alias('sum_of_sales_value'),
            F.array_distinct(
                F.flatten(F.collect_set(F.col('set_of_mandt_matnr_|_foreign_key_MARA')))
            ).alias('set_of_mandt_matnr_|_foreign_key_MARA'),
            F.array_distinct(
                F.flatten(F.collect_set(F.col('set_of_mandt_kunag_|_foreign_key_KNA1')))
            ).alias('set_of_mandt_kunag_|_foreign_key_KNA1'),
            F.array_distinct(
                F.flatten(F.collect_set(F.col('set_of_mandt_kunrg_|_foreign_key_KNA1')))
            ).alias('set_of_mandt_kunrg_|_foreign_key_KNA1'),
            F.array_distinct(
                F.flatten(F.collect_set(F.col('set_of_mandt_kunrg_vkorg_vtweg_spart_|_foreign_key_KNVV')))
            ).alias('set_of_mandt_kunrg_vkorg_vtweg_spart_|_foreign_key_KNVV'),
        )
        .withColumn(date_col, F.col(date_col).cast("timestamp"))
        .withColumn('_group_id', F.concat_ws('_||_', *grouping_cols))
        .withColumn('_group_type_id', F.lit(group_type_id))
        .withColumn('_filter_id', F.lit(filter_id))
        .withColumn('_time_series_group_id', F.concat_ws('_||_', F.col('_group_type_id'), F.col('_filter_id'), F.col('_group_id')))
        .withColumn('primary_key', F.concat_ws('_||_', F.col(date_col).cast('string'), F.col('_time_series_group_id')))
        .withColumn(
            'count_of_mandt_matnr_|_foreign_key_MARA',
            F.size('set_of_mandt_matnr_|_foreign_key_MARA')
        )
        .withColumn(
            'count_of_mandt_kunag_|_foreign_key_KNA1',
            F.size('set_of_mandt_kunag_|_foreign_key_KNA1')
        )
        .withColumn(
            'count_of_mandt_kunrg_|_foreign_key_KNA1',
            F.size('set_of_mandt_kunrg_|_foreign_key_KNA1')
        )
        .withColumn(
            'count_of_mandt_kunrg_vkorg_vtweg_spart_|_foreign_key_KNVV',
            F.size('set_of_mandt_kunrg_vkorg_vtweg_spart_|_foreign_key_KNVV')
        )
        .withColumn(
            'avg_unit_price',
            F.col('sum_of_sales_value') / F.col('sum_of_billed_quantity_|_fkimg')
        )
        .withColumn(
            'cumulative_set_of_mandt_matnr_|_foreign_key_MARA',
            F.array_distinct(
                F.flatten(
                    F.collect_set(
                        F.col('set_of_mandt_matnr_|_foreign_key_MARA')
                    ).over(cumulative_window)
                )
            ),
        )
        .withColumn(
            'size_of_cumulative_set_of_mandt_matnr_|_foreign_key_MARA',
            F.size('cumulative_set_of_mandt_matnr_|_foreign_key_MARA')
        )
        .withColumn(
            'lagged_size_of_cumulative_set_of_mandt_matnr_|_foreign_key_MARA',
            F.lag('size_of_cumulative_set_of_mandt_matnr_|_foreign_key_MARA').over(cumulative_window)
        )
        .withColumn(
            'total_sales_value_of_past_30_days',
            F.sum('sum_of_sales_value').over(moving_30_day_window)
        )
        .withColumn(
            'rolling_30_day_avg_of_total_sales_value_of_past_30_days',
            F.avg('total_sales_value_of_past_30_days').over(moving_30_day_window)
        )
        .withColumn(
            'rolling_90_day_avg_of_total_sales_value_of_past_30_days',
            F.avg('total_sales_value_of_past_30_days').over(moving_90_day_window)
        )
        .withColumn(
            'rolling_30_day_avg_of_daily_sales_value',
            F.avg('sum_of_sales_value').over(moving_30_day_window)
        )
        .withColumn(
            'rolling_90_day_avg_of_daily_sales_value',
            F.avg('sum_of_sales_value').over(moving_90_day_window)
        )
        .withColumn(
            'absolute_diff_of_30_vs_90_day_rolling_avg_of_total_sales_value_of_past_30_days',
            F.col('rolling_30_day_avg_of_total_sales_value_of_past_30_days') - F.col('rolling_90_day_avg_of_total_sales_value_of_past_30_days')
        )
        .withColumn(
            'percentage_diff_of_30_vs_90_day_rolling_avg_of_total_sales_value_of_past_30_days',
            F.col('absolute_diff_of_30_vs_90_day_rolling_avg_of_total_sales_value_of_past_30_days') / F.abs(F.col('rolling_90_day_avg_of_total_sales_value_of_past_30_days'))
        )
    )
    return df2
