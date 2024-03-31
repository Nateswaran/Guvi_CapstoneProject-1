import googleapiclient.discovery
from pprint import pprint
import pymongo
import psycopg2
import pandas as pd
import streamlit as st

api_key = 'AIzaSyCTHPxL36fx_vJLrznEhuESRXkBF1wZIYM'
youtube = googleapiclient.discovery.build("youtube", "v3", developerKey = api_key)


def channel_data(channel_id):
  request = youtube.channels().list(
          part = "snippet,contentDetails,statistics",
          id = channel_id
      )
  response = request.execute()

  channelDetails = dict(title = response['items'][0]['snippet']['title'],
                        subscriber_Count = response['items'][0]['statistics']['subscriberCount'],
                        video_Count = response['items'][0]['statistics']['videoCount'],
                        channelid = response['items'][0]['id'],
                        des = response['items'][0]['snippet']['description'],
                        PublishedAt = response['items'][0]['snippet']['publishedAt'],
                        thumbnails = response['items'][0]['snippet']['thumbnails']['medium']['url'],
                        overall_playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
                        views_count = response['items'][0]['statistics']['viewCount'])
  return channelDetails


def get_videos_ids(channel_id):
  video_ids = []
  request = youtube.channels().list(
            part = "contentDetails",
            id = channel_id
      )
  response = request.execute()

  overall_playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

  next_page_token = None

  while True:
    request = youtube.playlistItems().list(
            part="snippet",
            playlistId=overall_playlist_id,
            maxResults=50,
            pageToken=next_page_token,
        )

    response1 = request.execute()
    for i in range(len(response1['items'])):
      video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
    next_page_token = response1.get('nextPageToken')

    if next_page_token is None:
        break

  return video_ids




# For video information

def get_video_info(video_ids):
  video_data = []
  for video_id in video_ids:
        request = youtube.videos().list(
          part="snippet,contentDetails,statistics",
          id=video_id
      )
        response = request.execute()
        for item in response['items']:
          data = dict(channel_Name = item['snippet']['channelTitle'],
                      channel_Id = item['snippet']['channelId'],
                      video_Id = item['id'],
                      Title = item['snippet']['title'],
                      Tags = item['snippet'].get('tags'),
                      Thumbnails = item['snippet']['thumbnails']['default']['url'],
                      Description = item['snippet'].get('description'),
                      Published_date = item['snippet']['publishedAt'],
                      Duration = item['contentDetails']['duration'],
                      Views = item['statistics'].get('viewCount'),
                      Likes = item['statistics'].get('likeCount'),
                      Comments = item['statistics'].get('commentCount'),
                      Favorite_Count = item['statistics']['favoriteCount'],
                      Definition = item['contentDetails']['definition'],
                      Caption_Status = item['contentDetails']['caption']
                      )
          video_data.append(data)
  return video_data


def get_comment_info(video_ids):
  Comment_data = []
  try:
    for video_id in video_ids:
      request = youtube.commentThreads().list(
              part="snippet",
              videoId=video_id,
              maxResults=50
          )
      response = request.execute()

      for item in response['items']:
        data = dict(Comment_Id = item['snippet']['topLevelComment']['id'],
                    Video_Id = item['snippet']['topLevelComment']['snippet']['videoId'],
                    Comment_Text = item['snippet']['topLevelComment']['snippet']['textDisplay'],
                    Comment_Author = item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    Comment_Published = item['snippet']['topLevelComment']['snippet']['publishedAt'])
        Comment_data.append(data)
  except:
    pass
  return Comment_data


#mongoDB uploading

client = pymongo.MongoClient("mongodb+srv://nateswaran:nateswaran@nates.mv1qvw7.mongodb.net/?retryWrites=true&w=majority")
db = client["Youtube_data"]


def channel_details(channel_id):
    ch_details = channel_data(channel_id)
    vi_ids = get_videos_ids(channel_id)
    vi_details = get_video_info(vi_ids)
    com_details = get_comment_info(vi_ids)
    
    coll1 = db["channel_details"]
    coll1.insert_one({"channel_information":ch_details,
                      "video_information":vi_details,
                      "comment_information":com_details})
    return 'upload completed successfully'
  
  

#Table creation for channel, videos and comments

def channels_table(channel_name_single):
    mydb = psycopg2.connect(host = 'localhost',
                            user = 'postgres',
                            password = 'nates',
                            database = 'youtube_data',
                            port = '5432')
    cursor = mydb.cursor()



    create_query = '''create table if not exists channels1(title varchar(100),
                                                        channelid varchar(80) primary key,
                                                        subscriber_Count bigint,
                                                        views_count bigint,
                                                        video_Count int,
                                                        des text,
                                                        overall_playlist_id varchar(80))'''
    cursor.execute(create_query)
    mydb.commit()
        

        
        
    single_channel_detail = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({'channel_information.title':channel_name_single},{'_id':0}):
        single_channel_detail.append(ch_data['channel_information'])

    df_single_channel_detail = pd.DataFrame(single_channel_detail)


    for index, row in df_single_channel_detail.iterrows():
        insert_query = '''insert into channels1(title,
                                            channelid,
                                            subscriber_Count,
                                            views_count,
                                            video_Count,
                                            des,
                                            overall_playlist_id)
                                            
                                            values(%s,%s,%s,%s,%s,%s,%s)'''
        values = (row['title'],
                row['channelid'],
                row['subscriber_Count'],
                row['views_count'],
                row['video_Count'],
                row['des'],
                row['overall_playlist_id'])
        
        
        try:
            cursor.execute(insert_query,values)
            mydb.commit()
        except:
            news = f"Your provided channel name {channel_name_single} is already exists"
            return news
        
            
            
            
def videos_table(channel_name_single):
    mydb = psycopg2.connect(host = 'localhost',
                        user = 'postgres',
                        password = 'nates',
                        database = 'youtube_data',
                        port = '5432')
    cursor = mydb.cursor()


    create_query = '''create table if not exists videos(channel_Name varchar(100),
                                                        channel_Id varchar(100),
                                                        video_Id varchar(30),
                                                        Title varchar(200),
                                                        Tags text,
                                                        Thumbnails varchar(200),
                                                        Description text,
                                                        Published_date timestamp,
                                                        Duration interval,
                                                        Views bigint,
                                                        Likes bigint,
                                                        Comments int,
                                                        Favorite_Count int,
                                                        Definition varchar(30),
                                                        Caption_Status varchar(30))'''
                                                        
                                                        
    cursor.execute(create_query)
    mydb.commit()


    single_videos_detail = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({'channel_information.title':channel_name_single},{'_id':0}):
        single_videos_detail.append(ch_data['video_information'])

    df_single_video_detail = pd.DataFrame(single_videos_detail[0])


    for index, row in df_single_video_detail.iterrows():
        insert_query = '''insert into videos(channel_Name,
                                             channel_Id,
                                             video_Id,
                                             Title,
                                             Tags,
                                             Thumbnails,
                                             Description,
                                             Published_date,
                                             Duration,
                                             Views,
                                             Likes,
                                             Comments,
                                             Favorite_Count,
                                             Definition,
                                             Caption_Status)
                                            
                                            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
                                        
                                        
        values = (row['channel_Name'],
                  row['channel_Id'],
                  row['video_Id'],
                  row['Title'],
                  row['Tags'],
                  row['Thumbnails'],
                  row['Description'],
                  row['Published_date'],
                  row['Duration'],
                  row['Views'],
                  row['Likes'],
                  row['Comments'],
                  row['Favorite_Count'],
                  row['Definition'],
                  row['Caption_Status']
                  )
        
    
        cursor.execute(insert_query,values)
        mydb.commit()
        
        
def comments_table(channel_name_single):
    mydb = psycopg2.connect(host = 'localhost',
                        user = 'postgres',
                        password = 'nates',
                        database = 'youtube_data',
                        port = '5432')
    cursor = mydb.cursor()


    create_query = '''create table if not exists comments(Comment_Id varchar(100),
                                                        Video_Id varchar(60),
                                                        Comment_Text text,
                                                        Comment_Author varchar(150),
                                                        Comment_Published timestamp)'''
    cursor.execute(create_query)
    mydb.commit()


    single_comments_detail = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({'channel_information.title':channel_name_single},{'_id':0}):
        single_comments_detail.append(ch_data['comment_information'])

    df_single_comment_detail = pd.DataFrame(single_comments_detail[0])


    for index, row in df_single_comment_detail.iterrows():
        insert_query = '''insert into comments(Comment_Id,
                                            Video_Id,
                                            Comment_Text,
                                            Comment_Author,
                                            Comment_Published)
                                            
                                            values(%s,%s,%s,%s,%s)'''
                                        
                                        
        values = (row['Comment_Id'],
                row['Video_Id'],
                row['Comment_Text'],
                row['Comment_Author'],
                row['Comment_Published']
                )
        

        cursor.execute(insert_query,values)
        mydb.commit()
        
        
def tables(single_channel):
    news = channels_table(single_channel)
    if news:
        return news
    else:
        videos_table(single_channel)
        comments_table(single_channel)
        
        return "Tables created successfully"
  
  


def show_channels_table():
    ch_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({},{'_id':0, 'channel_information':1}):
        ch_list.append(ch_data['channel_information'])
    df = st.dataframe(ch_list)
    
    return df
  
  
def show_videos_table():
    vi_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for vi_data in coll1.find({},{'_id':0, 'video_information':1}):
        for i in range(len(vi_data['video_information'])):
            vi_list.append(vi_data['video_information'][i])
    df2 = st.dataframe(vi_list)
    
    return df2
  
  
def show_comments_table():
    com_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for com_data in coll1.find({},{'_id':0, 'comment_information':1}):
        for i in range(len(com_data['comment_information'])):
            com_list.append(com_data['comment_information'][i])
    df3 = st.dataframe(com_list)
    
    return df3
  
  
#Streamlit

with st.sidebar:
    st.title(":violet[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("Skills Take Away",divider='rainbow')
    st.caption(":blue[Python Scripting]")
    st.caption(":blue[Data Collection]")
    st.caption(":blue[MongoDB]")
    st.caption(":blue[API Integeration]")
    st.caption(":blue[Data Management using MongoDB and SQL]")
    
channel_id = st.text_input("Enter the channel ID")

if st.button("Collect and store data"):
    ch_ids = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({},{'_id':0, 'channel_information':1}):
        ch_ids.append(ch_data["channel_information"]["channelid"])
        
    if channel_id in ch_ids:
        st.success('Channel Details of the given channel id is already exists')
        
    else:
        insert = channel_details(channel_id)
        st.success(insert)
        
all_channels = []
db = client["Youtube_data"]
coll1 = db["channel_details"]
for ch_data in coll1.find({},{'_id':0, 'channel_information':1}):
   all_channels.append(ch_data['channel_information']['title'])
        
unique_channel = st.selectbox("Select the channel",all_channels)
        
if st.button('Migrate to SQL'):
    Table = tables(unique_channel)
    st.success(Table)
    
show_table = st.radio("SELECT THE TABLE FOR VIEW",(":rainbow[CHANNELS]","VIDEOS:movie_camera:","COMMENTS"))

if show_table == ':rainbow[CHANNELS]':
    show_channels_table()
    
elif show_table == 'VIDEOS:movie_camera:':
    show_videos_table()
    
elif show_table == 'COMMENTS':
    show_comments_table()
    
    
    
#SQL Connection

mydb = psycopg2.connect(host = 'localhost',
                    user = 'postgres',
                    password = 'nates',
                    database = 'youtube_data',
                    port = '5432')
cursor = mydb.cursor()

question = st.selectbox("Select your question",("1. ALL the videos and the channel name ",
                                                "2. Channels with most number of videos",
                                                "3. 10 most viewed videos",
                                                "4. Comments in each videos",
                                                "5. Videos with highest likes",
                                                "6. Likes of all videos",
                                                "7. Views of each channel",
                                                "8. Videos published in the year of 2022",
                                                "9. Average duration of all videos in each channel",
                                                "10. Videos with highest number of comments"))


if question == "1. ALL the videos and the channel name ":
    query1 = '''select title as videos,channel_name as channelname from videos'''
    cursor.execute(query1)
    mydb.commit()
    t1 = cursor.fetchall()
    df1 = pd.DataFrame(t1, columns = ['video title','channel name'])
    st.write(df1)
    
elif question == "2. Channels with most number of videos":
    query2 = '''select title as channelname,video_count as no_videos from channels1 order by video_count desc'''
    cursor.execute(query2)
    mydb.commit()
    t2 = cursor.fetchall()
    df2 = pd.DataFrame(t2, columns = ['channel name','No of videos'])
    st.write(df2)
    
elif question == "3. 10 most viewed videos":
    query3 = '''select views as views,channel_name as channelname,title as videotilte from videos
                where views is not null order by views desc limit 10'''
    cursor.execute(query3)
    mydb.commit()
    t3 = cursor.fetchall()
    df3 = pd.DataFrame(t3, columns = ['views','channel name','videotitle'])
    st.write(df3)
    
elif question == "4. Comments in each videos":
    query4 = '''select comments as no_comments,title as videotitle from videos where comments is not null'''
    cursor.execute(query4)
    mydb.commit()
    t4 = cursor.fetchall()
    df4 = pd.DataFrame(t4, columns = ['no of comments','videotitle'])
    st.write(df4)
    
elif question == "5. Videos with highest likes":
    query5 = '''select title as videotiltle,channel_name as channelname,likes as likecount
                    from videos where likes is not null order by likes desc'''
    cursor.execute(query5)
    mydb.commit()
    t5 = cursor.fetchall()
    df5 = pd.DataFrame(t5, columns = ['videotitle','channelname','likecount'])
    st.write(df5)
    
elif question == "6. Likes of all videos":
    query6 = '''select likes as likecount,title as videotitle from videos'''
    cursor.execute(query6)
    mydb.commit()
    t6 = cursor.fetchall()
    df6 = pd.DataFrame(t6, columns = ['likecount','videotitle'])
    st.write(df6)
    
elif question == "7. Views of each channel":
    query7 = '''select title as channelname,views_count as totalviews from channels1'''
    cursor.execute(query7)
    mydb.commit()
    t7 = cursor.fetchall()
    df7 = pd.DataFrame(t7, columns = ['channel name','total views'])
    st.write(df7)
    
elif question == "8. Videos published in the year of 2022":
    query8 = '''select title as video_title,published_date as videorelease,channel_name as channelname from videos
                where extract(year from published_date)=2022'''
    cursor.execute(query8)
    mydb.commit()
    t8 = cursor.fetchall()
    df8 = pd.DataFrame(t8, columns = ['videotitle','published_date','channelname'])
    st.write(df8)
    
elif question == "9. Average duration of all videos in each channel":
    query9 = '''select channel_name as channelname,AVG(duration) as averageduration from videos group by channel_name'''
    cursor.execute(query9)
    mydb.commit()
    t9 = cursor.fetchall()
    df9 = pd.DataFrame(t9, columns = ['channelname','averageduration'])

    t9 = []
    for index,row in df9.iterrows():
        channel_title = row['channelname']
        average_duration = row['averageduration']
        average_duration_str = str(average_duration)
        t9.append(dict(channeltitle = channel_title,avgduration = average_duration_str))
        
    df1 = pd.DataFrame(t9)
    st.write(df1)

elif question == "10. Videos with highest number of comments":
    query10 = '''select title as videotitle,channel_name as channelname,Comments as comments from videos 
                where Comments is not null order by comments desc'''
    cursor.execute(query10)
    mydb.commit()
    t10 = cursor.fetchall()
    df10 = pd.DataFrame(t10, columns = ['video title','channel name','comments'])
    st.write(df10)




