"""
################################ DEPLOYMENT LIBRARY ################################
#
# Please add here any cleaning code you wish to use for your deployment.
# Make sure that you also add the new functions you specified to your config.json.
# Please see the documentation in the README of the repository for more details.
# https://github.palantir.build/foundry/bellhop-authoring
#
####################################################################################
"""

# PUBLIC FUNCTIONS
# These functions are exposed via config.json

# Note that each function *must* take these four args:
# primary_dataframe, cleaning_metadata_dataframe, primary_dataset, source.
# This is because they are dynamically called in cleaned.py and are expected to take these four args by default.
# Do not forget to add "your_function_name_here" to your config.json if you want it to apply to your datasets.

from bellhop_authoring_api.bellhop_authoring_api_config_source import (
    SourceConfig,
    Table,
)


def your_function_name_here(
    primary_dataframe,
    _cleaning_metadata_dataframe,
    _table: Table,
    _source_config: SourceConfig,
):
    return primary_dataframe


# PRIVATE FUNCTIONS
# These functions are used as utils

# These functions can be anything you need for intermediary logic. These can have any structure.


def _your_private_function(primary_dataframe):
    return primary_dataframe
