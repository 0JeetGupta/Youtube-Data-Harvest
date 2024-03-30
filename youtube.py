#before initializing the code install "pip install google-api-python-client" in the terminal
import googleapiclient.discovery
from pprint import pprint
import streamlit as st
import pandas as pd

api_key = 'AIzaSyAh_ajCgEPccQR1DgsateTWWmYbXLAcYIY'
api_service_name = "youtube"
api_version = "v3"

youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)

import mysql.connector

conn = mysql.connector.connect(host='localhost',username='root',password='12212112',database='youtube_harvest')
cursor = conn.cursor()
conn.commit()

cursor.execute('''create table if not exists channel_detail(channel_name VARCHAR(225),
                                                            publish_at VARCHAR(225),
                                                            playlist_id VARCHAR(225),
                                                            sub_count VARCHAR(225),
                                                            vid_count VARCHAR(225))''')
conn.commit()


cursor.execute('''create table if not exists video_ids(video_id VARCHAR(150),
                                                       video_name text)''')
conn.commit()


cursor.execute('''create table if not exists video_detail(channel_name VARCHAR(225),
                                                           video_id VARCHAR(225),
                                                           title VARCHAR(225),
                                                           published_date VARCHAR(225),
                                                           duration VARCHAR(225),
                                                           views VARCHAR(225),
                                                           likes VARCHAR(225),
                                                           comments VARCHAR(225)                                      
                                                            )''')
conn.commit()


cursor.execute('''create table if not exists comment_detail(comment_Text text,
                                                            comment_Author VARCHAR(225),
                                                            comment_Published VARCHAR(225))''')
conn.commit()


def get_channel_detail( channel_id ):
   request = youtube.channels().list(
       part="snippet,contentDetails,statistics",
       id = channel_id
       )
   response = request.execute()

   channel_data = {
    "channel_name" : response['items'][0]['snippet']['title'],
    "publish_at" : response['items'][0]['snippet']['publishedAt'],
    "playlist_id" : response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
    "sub_count" : response['items'][0]['statistics']['subscriberCount'],
    "vid_count" : response['items'][0]['statistics']['videoCount'],
    }
   
   cursor = conn.cursor()
   sql = '''INSERT INTO channel_detail(channel_name ,
                                        publish_at ,
                                        playlist_id ,
                                        sub_count ,
                                        vid_count) VALUES (%s, %s,%s,%s,%s)'''
   val = tuple(channel_data.values())
   cursor.execute(sql, val)

   conn.commit() 

   return channel_data


def get_videos_ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(id=channel_id,
                                    part='contentDetails').execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None

    while True:
        response1=youtube.playlistItems().list(
                                            part='snippet',
                                            playlistId=Playlist_Id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            data= {"video_id" : response1['items'][i]['snippet']['resourceId']['videoId'],
                   "video_name" : response1['items'][i]['snippet']['title']}
            video_ids.append(data)
        next_page_token=response1.get('nextPageToken')

        if next_page_token is None:
            break

        id_detail=[]
        for i in video_ids:
          id_detail.append(tuple(i.values()))
        
        cursor = conn.cursor()
        sql = '''INSERT INTO video_ids(video_id,
                                       video_name) VALUES (%s, %s)'''
        val = id_detail
        cursor.executemany(sql, val)

        conn.commit()    
    
    return video_ids


def get_video_detail(video_id):
    request=youtube.videos().list(
        part="snippet,ContentDetails,statistics",
        id=video_id
    )
    response=request.execute()

    for item in response["items"]:
        video_data =   {"channel_name":item['snippet']['channelTitle'],
                        "video_id":item['id'],
                        "title":item['snippet']['title'],
                        "published_date":item['snippet']['publishedAt'],
                        "duration":item['contentDetails']['duration'],
                        "views":item['statistics'].get('viewCount'),
                        "likes":item['statistics'].get('likeCount'),
                        "comments":item['statistics'].get('commentCount')
                          } 
        
    cursor = conn.cursor()
    sql = '''INSERT INTO video_detail(channel_name,
                                    video_id,
                                    title,
                                    published_date,
                                    duration,
                                    views,
                                    likes,
                                    comments) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)'''
    val = tuple(video_data.values())
    cursor.execute(sql, val)

    conn.commit()    
    return video_data


def get_comment_detail(video_id):
    comment_data=[]
    request=youtube.commentThreads().list(
        part="snippet",
        videoId=video_id,
        maxResults=50
    )
    response=request.execute()

    for i in range(len(response['items'])):
        data={
                "comment_Text":response['items'][i]['snippet']['topLevelComment']['snippet']['textDisplay'],
                "comment_Author":response['items'][i]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                "comment_Published":response['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt']
                }
        comment_data.append(data)
        
    comment_detail=[]    
    for i in comment_data:
      comment_detail.append(tuple(i.values()))   
    cursor = conn.cursor()
    sql = '''INSERT INTO comment_detail(comment_Text,
                                       comment_Author,
                                       comment_Published) VALUES (%s,%s,%s)'''
    val = comment_detail
    cursor.executemany(sql, val)

    conn.commit()        
    return comment_data   


st.title(":red[YOUTUBE DATA HARVESTING]")


channel_id=str(st.text_input("ENTER CHANNEL ID: "))
if st.button("show channel details"):
    channel_data=get_channel_detail(channel_id)
    st.dataframe(channel_data)
if st.button("show videos and ids"):
    video_ids=get_videos_ids(channel_id)
    st.dataframe(video_ids)

video_id=str(st.text_input("ENTER VIDEO ID: "))
if st.button("Show video details"):
    video_data=get_video_detail(video_id)
    st.dataframe(video_data)
if st.button("show video comments"):
    comment_data=get_comment_detail(video_id)
    st.dataframe(comment_data)
