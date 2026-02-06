# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# #### PRJ105 ⬛ ADV Project (Sprint 5): Lakehouse Development - ALL TABLES
# > The code in this notebook is written as part of Week 5 of the Advanced Project, in [Fabric Dojo](https://skool.com/fabricdojo/about).
#  
# #### Step 1: Getting our variables
# Firstly, we need to get the variables from the variable library, so that our code will function no matter which workspace it is being called from. 
#  
# In this context, we are using the Spark-SQL four-part naming structure: 
# \`workspace_name\`.lakehouse_name.schema_name.table_name 
# 
# _Note: if your workspace name contains hyphens (rather than underscore), then you must use \` backtick to wrap around the workspace name._


# CELL ********************

import polars as pl 

variables = notebookutils.variableLibrary.getLibrary("lakehouse_variables_VL")

ws_ID = variables.LH_WORKSPACE_ID

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# #### Step 2: Define some metadata
# I just took the metadata from each of the three notebooks, and combined it into one Python object. 
# 
# We will build this into our metadata framework in a later Sprint - for now - this is good enough! 

# CELL ********************

LAKEHOUSE_METADATA = {
    "bronze": {
        "LH_ID_variable": "BRONZE_LH_ID",
        "LH_name_variable": "BRONZE_LH_NAME",
        "schemas": ["youtube"],
        "tables": {
            "youtube/channel":[
                ('channel_id', pl.Utf8), 
                ('channel_name', pl.Utf8), 
                ('channel_description', pl.Utf8), 
                ('view_count', pl.Int64), 
                ('subscriber_count', pl.Int64), 
                ('video_count', pl.Int16), 
                ('loading_TS', pl.Datetime("us", time_zone="UTC"))],
            "youtube/playlist_items": [
                ('channel_id' ,pl.Utf8), 
                ('video_id' ,pl.Utf8), 
                ('video_title' ,pl.Utf8), 
                ('video_description' ,pl.Utf8),
                ('thumbnail_url' , pl.Utf8),
                ('video_publish_TS' , pl.Datetime("us", time_zone="UTC")),
                ('loading_TS' , pl.Datetime("us", time_zone="UTC"))],
            "youtube/videos": [
                ('video_id' ,pl.Utf8), 
                ('video_view_count' ,pl.Int64), 
                ('video_like_count' , pl.Int64), 
                ('video_comment_count' ,pl.Int32),
                ('loading_TS' ,pl.Datetime("us", time_zone="UTC"))],
        }
    },
    "silver": {
        "LH_ID_variable": "SILVER_LH_ID",
        "LH_name_variable": "SILVER_LH_NAME",
        "schemas": ["youtube"],
        "tables": {
            "youtube/channel_stats":[
                ('channel_id', pl.Utf8), 
                ('channel_name', pl.Utf8), 
                ('channel_description', pl.Utf8), 
                ('view_count', pl.Int64), 
                ('subscriber_count', pl.Int64), 
                ('video_count', pl.Int16), 
                ('loading_TS', pl.Datetime("us", time_zone="UTC"))],
            "youtube/videos": [
                ('channel_id', pl.Utf8), 
                ('video_id', pl.Utf8), 
                ('video_title', pl.Utf8), 
                ('video_description', pl.Utf8),
                ('thumbnail_url', pl.Utf8),
                ('video_publish_TS', pl.Datetime("us", time_zone="UTC")),
                ('loading_TS', pl.Datetime("us", time_zone="UTC"))],
            "youtube/video_statistics": [
                ('video_id', pl.Utf8), 
                ('video_view_count', pl.Int64), 
                ('video_like_count', pl.Int64), 
                ('video_comment_count', pl.Int16),
                ('loading_TS', pl.Datetime("us", time_zone="UTC"))],
        }
    },
    "gold": {
        "LH_ID_variable": "GOLD_LH_ID",
        "LH_name_variable": "GOLD_LH_NAME",
        "schemas": ["marketing"],
        "tables": {
            "marketing/channels": [
                ('channel_surrogate_id', pl.Int16), 
                ('channel_platform', pl.Utf8),
                ('channel_account_name', pl.Utf8),
                ('channel_account_description', pl.Utf8),
                ('channel_total_subscribers', pl.Int64),
                ('channel_total_assets', pl.Int32),
                ('channel_total_views', pl.Int64),
                ('modified_TS', pl.Datetime("us", time_zone="UTC"))],
            "marketing/assets": [
                ('asset_surrogate_id',pl.Int16),
                ('asset_natural_id',pl.Utf8),
                ('channel_surrogate_id', pl.Int16),
                ('asset_title',pl.Utf8),
                ('asset_text',pl.Utf8), 
                ('asset_publish_date', pl.Datetime("us", time_zone="UTC")),
                ('modified_TS', pl.Datetime("us", time_zone="UTC"))],
            "marketing/asset_stats": [
                ('asset_surrogate_id',pl.Int16), 
                ('asset_total_impressions', pl.Int64),
                ('asset_total_views',pl.Int64), 
                ('asset_total_likes',pl.Int64),
                ('asset_total_comments', pl.Int32),
                ('modified_TS', pl.Datetime("us", time_zone="UTC"))],
        }
    }
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# #### Step 3: Define function
# Our function takes the lakehouse config as input (from the METADATA), and performs the Schema Creation and Table Creation, as per the metadata config. 


# CELL ********************

def create_lakehouse_objects(lakehouse_config):
    """
    Creates schemas and tables for a lakehouse based on configuration.
    
    Args:
        lakehouse_config: Dictionary containing lakehouse metadata
    """
    # Get lakehouse name from variables
    lh_ID = getattr(variables, lakehouse_config["LH_ID_variable"])
    lh_name = getattr(variables, lakehouse_config["LH_name_variable"])
    
    
    # Create tables
    for lh_schema_n_table, table_schema in lakehouse_config["tables"].items():
        lh_schema, table_name = lh_schema_n_table.split('/')
        lh_schema_path = f'abfss://{ws_ID}@onelake.dfs.fabric.microsoft.com/{lh_ID}/Tables/{lh_schema}'
        if table_name in [item.name for item in notebookutils.fs.ls(lh_schema_path)]:
            print(f'Table: "{table_name}" already exist in schema : "{lh_schema}" in lakehouse: {lh_name}, skipping it...')
            continue
        else:
            tempdf = pl.DataFrame(schema=table_schema)
            tempdf.write_delta(f'{lh_schema_path}/{table_name}')
            print(f'Table: "{table_name}" successfully created in schema: "{lh_schema}" in lakehouse:{lh_name} ✅')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# #### Step 4: Loop through the metadata, and run the function 

# CELL ********************

# Process all lakehouses
for layer, lakehouse_config in LAKEHOUSE_METADATA.items():
    create_lakehouse_objects(lakehouse_config)
    print(f'\n All lakehouse tables created for the {layer} medallion layer !! \n')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
