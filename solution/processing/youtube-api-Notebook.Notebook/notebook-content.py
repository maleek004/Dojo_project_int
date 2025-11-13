# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

## Imports
!pip install google-api-python-client > /dev/null 2>&1
import pandas as pd
from googleapiclient.discovery import build 
from IPython.display import JSON 

from datetime import datetime
import json

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Getting the Youtube API key from Azure Key Vault

# CELL ********************

api_key = notebookutils.credentials.getSecret('https://kv-dojo-project.vault.azure.net/','youtube-api-key')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Getting information about the lakehouse to write to from the variable library 

# CELL ********************

## get value from variable library
VL_name = 'lakehouse_variables_VL'
VL = notebookutils.variableLibrary.getLibrary(VL_name)
destination_lh_name = VL.BRONZE_LH_NAME
destination_lh_workspace_name = VL.BRONZE_LH_WORKSPACE_NAME

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Setting up the youtube API client

# CELL ********************

# Get credentials and create an API client
api_service_name = "youtube"
api_version = "v3"    
youtube = build(
        api_service_name, api_version, developerKey=api_key)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Functions

# CELL ********************

################################ construct abfs_path to write to based on relative path provided
def construct_abfs_write_path(relative_path, file_name): 
    """Constructs an ABFS path to write RAW JSON files into a Lakehouse
    It uses variables (from the Variable Library), that the path is dynamic across deployment environments
    """
    
    # 'variables' is the values from the Variable Library 
    ws_name = destination_lh_workspace_name
    lh_name = destination_lh_name

    formatted_date = datetime.now().strftime("%Y%m%d")

    timestamped_file_name = f"{formatted_date}-{file_name}.json" 

    abfs_path = f"abfss://{ws_name}@onelake.dfs.fabric.microsoft.com/{lh_name}.Lakehouse/Files/{relative_path}{timestamped_file_name}"
    
    return abfs_path



############################## write json to specific location 
def write_json_to_location(python_object, relative_path, file_name):  
    """takes a python object like list or dictionary and Writes as JSON files in Lakehouse"""

    abfs_path = construct_abfs_write_path(relative_path, file_name)

    json_string = json.dumps(python_object, indent=2)

    notebookutils.fs.put(abfs_path, json_string, overwrite=True)
    print(f"{abfs_path.split('/')[-1]} written to {relative_path} successfully 😎")


######### get channel stat details from the API as a ditionary 
def get_channel_details(youtube, channelid):
    """
    This function takes a Youtube API client and a channel ID
    and returns the stats json about that channel as retrieved from the API.

    Parameters
    ----------
    youtube : object
        The Youtube API client.
    channel_id : string
        A channel ID.

    Returns
    -------
     
        A dictionary that conttains channel stats.
    """



    try:
        request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channelid
        )
        response = request.execute()

        if response.get("items"):
            item = response["items"][0]
        else:
            print(f"No channel found for {channelid}")

    except Exception as e:
        print(f"An error occurred for {channelid}: {e}")
    
    
    return item



################## get the channel's 'uploads' playlist from the API as a list of dictionaries and the video id of all videos in the playlist as a list
def get_channel_playlist_and_video_ids(youtube, playlist_id):
    """ 
    returns a list containing the raw response for the channel's playlist id request and another of video IDs 
    the channel play;ist response will be stored as raw json file 
    while the video ID list will be used to retrieve the video stats for the channel"""
    
    video_IDs = []
    channel_playlist_response = []

    next_page_token = None

    while True:
        try:
            request = youtube.playlistItems().list(
                part="snippet,contentDetails",
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token
            )
            response = request.execute()

            for item in response.get('items', []):
                channel_playlist_response.append(item)
                video_id = item['contentDetails']['videoId']
                video_IDs.append(video_id)

            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break
        except Exception as e:
            print(f"Error fetching videos for playlist {playlist_id}: {e}")


    print(f"{len(video_IDs)} videos IDs have been added to the list.")
    return channel_playlist_response, video_IDs 


################ get specific details from the API, about all videos in the uploads playlist of a channel 
def get_video_details(youtube, videoID):
    """
    This function takes a Youtube API client and a list of video IDs
    and returns a list of dictionaries containing the video details.

    Parameters
    ----------
    youtube : object
        The Youtube API client.
    videoID : list
        A list of video IDs.

    Returns
    -------
        A list of dictionaries containing the video details.
    """
    video_details_response = []
    
    if not videoID:
        print("No video IDs provided.")
        return video_details_response

    default_thumbnail_url = "https://media.istockphoto.com/id/1409329028/vector/no-picture-available-placeholder-thumbnail-icon-illustration-design.jpg?s=612x612&w=0&k=20&c=_zOuJu755g2eEUioiOUdz_mHKJQJn-tDgIAhQzyeKUQ="

    for i in range(0, len(videoID), 50):
        batch = videoID[i:i+50]  
        video_id_str = ",".join(batch)  

        try:
            request = youtube.videos().list(
                    part="snippet,contentDetails,statistics,liveStreamingDetails",
                    id=video_id_str
                )
            response = request.execute()

            for video in response.get('items', []):  
                video_details_response.append(video)

            print(f"Processed {len(batch)} videos from batch {i//50 + 1}")

        except Exception as e:
           print(f"An error occurred in batch {i//50 + 1}: {e}")


    print(f"Total videos retrieved: {len(video_details_response)}")
    return video_details_response


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# #### get channel stat info and write it to Files/youtube_data_v3/channels/ in the destination lakehouse

# CELL ********************

channel_stat_relative_path = "youtube_data_v3/channels/"

channel_stat_response = get_channel_details(youtube , 'UCrvoIYkzS-RvCEb0x7wfmwQ')
write_json_to_location(channel_stat_response , channel_stat_relative_path , 'youtube_channel_stats')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### get channel's 'uploads' playlist info and write it to Files/youtube_data_v3/playlistItems/ in the lakehouse

# CELL ********************

channel_playlist_id = channel_stat_response.get("contentDetails").get("relatedPlaylists").get("uploads")
channel_playlist_relative_path = "youtube_data_v3/playlistItems/"

channel_playlist_response , video_IDs = get_channel_playlist_and_video_ids(youtube , channel_playlist_id)

write_json_to_location(channel_playlist_response , channel_playlist_relative_path , 'youtube_channel_playlist_items')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# #### Get details about all videos in channel and write it to Files/youtube_data_v3/videos/ in the destination lakehouse

# CELL ********************

video_details_relative_path = "youtube_data_v3/videos/"

video_details_response = get_video_details(youtube , video_IDs)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

write_json_to_location(video_details_response , video_details_relative_path , 'youtube_channel_video_details')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
