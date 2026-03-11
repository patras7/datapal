# =============================================================
# Converted from Palantir Transforms -> Databricks PySpark
# Generated on 2026-02-25T13:47:18.757075900Z (UTC)
# =============================================================

# Original file had no Palantir-specific code
# File copied as-is

#!/usr/bin/env python
"""Python project setup script."""

import os
from setuptools import find_packages, setup

setup(
    name=os.environ['PKG_NAME'],
    version=os.environ['PKG_VERSION'],

    description='Python data transformation project',

    author="ph-core-ontology-sap",

    packages=find_packages(exclude=['contrib', 'docs', 'test']),

    # Please specify your dependencies in conda_recipe/meta.yaml instead.
    install_requires=[],

    entry_points={
        'transforms.pipelines': [
            'root = myproject.pipeline:my_pipeline'
        ]
    }
)
