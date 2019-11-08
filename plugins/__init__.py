from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
import helpers

# Defining the plugin class
class CustomPlugin(AirflowPlugin):
    name = "usstock_custom_plugin"
    operators = [
        operators.StageJsonToS3,
        operators.S3CreateBucket,
    ]
    helpers = [
        helpers.StockSymbols,
    ]
