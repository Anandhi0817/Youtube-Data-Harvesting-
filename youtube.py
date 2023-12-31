from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st

#API key connection
def Api_connect():
    Api_Id="AIzaSyAl9NDAgzX7AxTb__79zhOb5DsiXzA0SBE"
    api_service_name="youtube"
    api_version="v3"
    youtube=build(api_service_name,api_version,developerKey=Api_Id)
    return youtube
youtube=Api_connect()

#Getting Channels information 
def get_channel_info(channel_id):
    request=youtube.channels().list(
                part="snippet,ContentDetails,statistics",
                id=channel_id
    )
    response=request.execute()

    for i in response['items']:
        data = dict(Channel_Name=i["snippet"]["title"],
                    Channel_Id=i["id"],
                    Subscribers=i['statistics']['subscriberCount'],
                    Views=i['statistics']['viewCount'],
                    Total_Videos=i['statistics']['videoCount'],
                    Channel_Description=i["snippet"]['description'],
                    Playlist_Id=i['contentDetails']['relatedPlaylists']['uploads'])
    return data

#Getting video ids 
def get_videos_ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(id=channel_id,
                                    part='contentDetails').execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None

    while True:
        response=youtube.playlistItems().list(
                                            part='snippet',
                                            playlistId=Playlist_Id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()

        for i in range(len(response['items'])):
            video_ids.append(response['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids

#Getting video details
def get_video_info(video_ids):
    Video_data=[]
    for videos_id in video_ids:
        request=youtube.videos().list(
            part='snippet,ContentDetails,statistics',
            id=videos_id
        )
        response=request.execute()

        for item in response['items']:
            data=dict(Channel_Name=item['snippet']['channelTitle'],
                    Channel_Id=item['snippet']['channelId'],
                    Video_Id=item['id'],
                    Title=item['snippet']['title'],
                    Tags=item.get('tags'),
                    Thumbnail=item['snippet']['thumbnails'],
                    Description=item.get('description'),
                    Published_Date=item['snippet']['publishedAt'],
                    Duration=item['contentDetails']['duration'],
                    Views=item.get('viewCount'),
                    Comments=item.get('commentCount'),
                    Favorite_Count=item['statistics']['favoriteCount'],
                    Definition=item['contentDetails']['definition'])
            Video_data.append(data)
    return Video_data

#Getting comment info
def get_comment_info(video_ids):
    Comment_data=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                part='snippet',
                videoId=video_id,
                maxResults=50
            )
            response=request.execute()
            for item in response['items']:
                data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                        Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                Comment_data.append(data)
    except:
        pass
    return Comment_data

#Getting playlist details
def get_playlist_details(channel_id):
        next_page_token=None
        All_data=[]
        while True:
            request=youtube.playlists().list(
                part='snippet,contentDetails',
                channelId=channel_id,
                maxResults=50,
                pageToken=next_page_token
            )
            response=request.execute()

            for item in response['items']:
                    data=dict(Playlist_Id=item['id'],
                            Title=item['snippet']['title'],
                            Channel_Id=item['snippet']['channelId'],
                            ChannelName=item['snippet']['channelTitle'],
                            PublishedAt=item['snippet']['publishedAt'],
                            Video_Count=item['contentDetails']['itemCount'])
                    All_data.append(data)

            next_page_token=response.get('nextPageToken')
            if next_page_token is None:
                break
        return All_data

# Mongo db Connections 
client=pymongo.MongoClient('mongodb://localhost:27017/')
mydb=client['Ýoutube']

#upload to Mongodb
def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    pl_details=get_playlist_details(channel_id)
    vi_ids=get_videos_ids(channel_id)
    vi_details=get_video_info(vi_ids)
    com_deails=get_comment_info(vi_ids)

    collection=mydb['channel_details']
    collection.insert_one({'channel_information':ch_details,'playlist_informaion':pl_details,
                           'video_information':vi_details,'comment_information':com_deails})
    return 'completed'

#Table creation & Getting infomation from mongoDB 
def channels_table():
    mydb=psycopg2.connect(host='localhost',
                        user='postgres',
                        password='mysql@11',
                        database='Youtube',
                        port='5432')
    cursor=mydb.cursor()

    drop_query=''''Drop table if exits channels'''
    cursor.execute(drop_query)
    mydb.commit()
    

    try:
        creation='''create table if not exists channels(Channel_Name varchar(100),
                                                    Channel_Id varchar(80) primary key,
                                                    Subscribers bigint,
                                                    Views bigint,
                                                    otal_Videos int,
                                                    Channel_Description text,
                                                    Playlist_Id varchar(80))'''
        cursor.execute(creation)
        mydb.commit()

    except:
        print('Channels successfully created')


ch_list=[]
mydb=client['Ýoutube']
collection=mydb['channel_details']
for ch_data in collection.find({},{'_id':0,'channel_information':1}):
    ch_list.append(ch_data['channel_information'])
df=pd.DataFrame(ch_list)


for index,row in df.iterrows():
        insert = '''INSERT into channels(Channel_Name,
                                                    Channel_Id,
                                                    Subscribers,
                                                    Views,
                                                    Total_Videos,
                                                    Channel_Description,
                                                    Playlist_Id)
                                        VALUES(%s,%s,%s,%s,%s,%s,%s)'''
            
        values =(
                row['Channel_Name'],
                row['Channel_Id'],
                row['Subscribers'],
                row['Views'],
                row['Total_Videos'],
                row['Channel_Description'],
                row['Playlist_Id'])
        try:                     
            cursor.execute(insert,values)
            mydb.commit()    
        except:
            print("Channels values are already inserted")

#Creating playlist tables
def playlists_table():
    mydb=psycopg2.connect(host='localhost',
                        user='postgres',
                        password='mysql@11',
                        database='Youtube',
                        port='5432')
    cursor=mydb.cursor()

    drop_query='''Drop table if exists Playlist'''
    cursor.execute(drop_query)
    mydb.commit()   

    try:
        creation='''create table if not exists Playlist(Playlist_Id varchar(100) primary key,
                                                    Title varchar(80),
                                                    Channel_Id varchar(100),
                                                    ChannelName varchar(100),
                                                    PublishedAt timestamp,
                                                    Video_Count int
                                                    )'''
        cursor.execute(creation)
        mydb.commit()

    except:
        print('playlist already Created')

pl_list=[]
mydb=client['Ýoutube']
collection=mydb['channel_details']
for pl_data in collection.find({},{'_id':0,'playlist_informaion':1}):
    for i in range(len(pl_data["playlist_informaion"])):
        pl_list.append(pl_data["playlist_informaion"][i])
df=pd.DataFrame(pl_list)

for index,row in df.iterrows():
        insert ='''INSERT into playlist(Playlist_Id,
                                        Title,
                                        Channel_Id,
                                        PublishedAt,
                                        Video_Count,
                                        ChannelName)
                                            
                                VALUES(%s,%s,%s,%s,%s,%s)'''
            
        values =(row['Playlist_Id'],
                 row['Title'],
                 row['Channel_Id'],
                 row['PublishedAt'],
                 row['Video_Count'],
                 row['ChannelName'])
        try:                  
                cursor.execute(insert,values)
                mydb.commit()
        except:
                print('playlist successfully inserted')

#Creating videos tables
def video_table():
    mydb = psycopg2.connect(host="localhost",
                user="postgres",
                password='mysql@11',
                database= 'Youtube',
                port = "5432"
                )
    cursor = mydb.cursor()
    
    drop_query = "drop table if exists videos"
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''create table if not exists videos(Channel_Name varchar(150),
                        Channel_Id varchar(100),
                        Video_Id varchar(50) primary key, 
                        Title varchar(150), 
                        Tags text,
                        Thumbnail varchar(225),
                        Description text, 
                        Published_Date timestamp,
                        Duration interval, 
                        Views bigint,
                        Comments int,
                        Favorite_Count int, 
                        Definition varchar(10), 
                        )'''                  
        cursor.execute(create_query)             
        mydb.commit()
    except:
        print("Videos Table alrady created")

vi_list=[]
mydb=client['Ýoutube']
collection=mydb['channel_details']
for vi_data in collection.find({},{'_id':0,'video_information':1}):
    for i in range(len(vi_data['video_information'])):
        vi_list.append(vi_data['video_information'][i])
df=pd.DataFrame(vi_list)



for index, row in df.iterrows():
    insert = '''INSERT INTO videos (Channel_Name,
                    Channel_Id,
                    Video_Id, 
                    Title, 
                    Tags,
                    Thumbnail,
                    Description, 
                    Published_Date,
                    Duration, 
                    Views, 
                    Comments,
                    Favorite_Count, 
                    Definition, 
                    )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
    values = (row['Channel_Name'],
                row['Channel_Id'],
                row['Video_Id'],
                row['Title'],
                row['Tags'],
                row['Thumbnail'],
                row['Description'],
                row['Published_Date'],
                row['Duration'],
                row['Views'],
                row['Comments'],
                row['Favorite_Count'],
                row['Definition'])
                                
    try:    
        cursor.execute(insert,values)
        mydb.commit()
    except:
        print("videos values already inserted in the table")        

#ccreating comments tables
def comments_table():
    mydb = psycopg2.connect(host="localhost",
                user="postgres",
                password='mysql@11',
                database= 'Youtube',
                port = "5432"
                )
    cursor = mydb.cursor()
    
    drop_query = "drop table if exists videos"
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''CREATE TABLE if not exists comments(Comment_Id varchar(100) primary key,
                       Video_Id varchar(80),
                       Comment_Text text, 
                       Comment_Author varchar(150),
                       Comment_Published timestamp)'''
        cursor.execute(create_query)
        mydb.commit()
        
    except:
        print("Comment Table already created")


com_list=[]
mydb=client['Ýoutube']
collection=mydb['channel_details']
for com_data in collection.find({},{'_id':0,'comment_information':1}):
    for i in range(len(com_data['comment_information'])):
        com_list.append(com_data['comment_information'][i])
df=pd.DataFrame(com_list)

for index, row in df.iterrows():
            insert_query = '''INSERT INTO comments (Comment_Id,
                                                    Video_Id ,
                                                    Comment_Text,
                                                    Comment_Author,
                                                    Comment_Published)
                                VALUES (%s, %s, %s, %s, %s)'''
            values = (row['Comment_Id'],
                        row['Video_Id'],
                        row['Comment_Text'],
                        row['Comment_Author'],
                        row['Comment_Published'])
            try:
                cursor.execute(insert_query,values)
                mydb.commit()
            except:
                print("This comments are already exist in comments table")

# Total tables Creation 
def tables():
    channels_table()
    playlists_table()
    videos_table()
    comments_table()
    return "Tables Created successfully"

def channels_table():
    ch_list=[]
    mydb=client['Ýoutube']
    collection=mydb['channel_details']
    for ch_data in collection.find({},{'_id':0,'channel_information':1}):
        ch_list.append(ch_data['channel_information'])
    channels_table=st.dataframe(ch_list)
    return channels_table

def playlists_table():
    pl_list=[]
    mydb=client['Ýoutube']
    collection=mydb['channel_details']
    for pl_data in collection.find({},{'_id':0,'playlist_informaion':1}):
        for i in range(len(pl_data["playlist_informaion"])):
            pl_list.append(pl_data["playlist_informaion"][i])
    playlists_table=st.dataframe(pl_list)
    return playlists_table

def videos_table():
    vi_list=[]
    mydb=client['Ýoutube']
    collection=mydb['channel_details']
    for vi_data in collection.find({},{'_id':0,'video_information':1}):
        for i in range(len(vi_data['video_information'])):
            vi_list.append(vi_data['video_information'][i])
    video_details=st.dataframe(vi_list)
    return videos_table

def comments_table():
    com_list=[]
    mydb=client['Ýoutube']
    collection=mydb['channel_details']
    for com_data in collection.find({},{'_id':0,'comment_information':1}):
        for i in range(len(com_data['comment_information'])):
            com_list.append(com_data['comment_information'][i])
    Comment_details=st.dataframe(com_list)
    return comments_table

#Streamlit part

with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("SKILL TAKE AWAY")
    st.caption('Python scripting')
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption(" Data Managment using MongoDB and SQL")

channel_id = st.text_input("Enter the Channel ID")


if st.button("Collect and Store data"):
        ch_ids = []
        mydbdb = client["Youtube"]
        collection = mydbdb["channel_details"]
        for ch_data in collection.find({},{"_id":0,"channel_information":1}):
            ch_ids.append(ch_data["channel_information"]["Channel_Id"])
        if channel_id in ch_ids:
            st.success("Channel details of the given channel id already exists")
        else:
            insert = channel_details(channel_id)
            st.success(insert)

if st.button("Migrate to SQL"):
    Table = tables()
    st.success(Table)

show_table=st.radio('SELECT TABLE FOR VIEW THE DETAILS',('CHANNELS','PLAYLIST','VIDEOS','COMMENTS'))

if show_table=="CHANNELS":
    channels_table()

elif show_table=="PLAYLIST":
    playlists_table()
    
elif show_table=="VIDEOS":
    videos_table()

elif show_table=="COMMENTS":
    comments_table()

#INSERTING QUESTIONS 
mydb = psycopg2.connect(host="localhost",
                user="postgres",
                password='mysql@11',
                database= 'Youtube',
                port = "5432"
                )
cursor = mydb.cursor()

question=st.selectbox('SELECT QUESTION',
                      ('1.What are the names of all the videos and their corresponding channels ?',
                        '2.Which channels have the most number of videos, and how many videos do they have ?',
                        '3.What are the top 10 most viewed videos and their respective channels ?',
                        '4.How many comments were made on each video,and what are their corresponding video names ?',
                        '5.Which videos have the highest number of likes,and what are their corresponding channel names ?',
                        '6.What is the total number of likes and dislikes for each video,and what are their corresponding video names ?',
                        '7.What is the total number of views for each channel,and what are their corresponding channel name ?',
                        '8.What are the names of all the channels that have published videos in the year 2022 ?',
                        '9.What is the average duration of all videos in each channel,and what are their corresponding channel names ?',
                        '10.Which videos have the highest number of comments,and what are their corresponding channel names ?'))

if question == '1. All the videos and the Channel Name':
    query1 = '''select Title as videos, channel_name as channelname from videos'''
    cursor.execute(query1)
    mydb.commit()
    table1=cursor.fetchall()
    df1=pd.DataFrame(table1, columns=["Video Title","Channel Name"])
    st.write(df1)

elif question == '2.Which channels have the most number of videos, and how many videos do they have ?':
    query2 = '''select channel_name as channelname,total_videos as No_videos from channels order by total_videos desc'''
    cursor.execute(query2)
    mydb.commit()
    table2=cursor.fetchall()
    df2=pd.DataFrame(table2, columns=["channel name","No of videos"])
    st.write(df2)

elif question == '3.What are the top 10 most viewed videos and their respective channels ?':
    query3 = '''select Views as views , channel_name as channelname,Title as VideoTitle from videos 
                        where Views is not null order by Views desc limit 10'''
    cursor.execute(query3)
    mydb.commit()
    table3= cursor.fetchall()
    df3=pd.DataFrame(table3, columns = ["views","channel Name","video title"])
    st.write(df3)

elif question == '4.How many comments were made on each video,and what are their corresponding video names ?':
    query4 = '''select Comments as No_comments ,Title as VideoTitle from videos where Comments is not null'''
    cursor.execute(query4)
    mydb.commit()
    table4=cursor.fetchall()
    df4=pd.DataFrame(table4, columns=["No Of Comments", "Video Title"])
    st.write(df4)

elif question == '5.Which videos have the highest number of likes,and what are their corresponding channel names ?':
    query5 = '''select Title as VideoTitle, channel_name as channelname, Likes as LikesCount from videos 
                       where Likes is not null order by Likes desc'''
    cursor.execute(query5)
    mydb.commit()
    table5= cursor.fetchall()
    df5=pd.DataFrame(table5, columns=["video Title","channel Name","like count"])
    st.write(df5)

elif question == '6.What is the total number of likes and dislikes for each video,and what are their corresponding video names ?':
    query6 = '''select Likes as likeCount,Title as VideoTitle from videos'''
    cursor.execute(query6)
    mydb.commit()
    table6= cursor.fetchall()
    df6=pd.DataFrame(table6, columns=["like count","video title"])
    st.write(df6)

elif question == '7.What is the total number of views for each channel,and what are their corresponding channel name ?':
    query7 = '''select channel_name as channelname, Views as Channelviews from channels'''
    cursor.execute(query7)
    mydb.commit()
    table7=cursor.fetchall()
    df7=pd.DataFrame(table7, columns=["channel name","total views"])
    st.write(df7)

elif question == '8.What are the names of all the channels that have published videos in the year 2022 ?':
    query8 = '''select Title as Video_Title, Published_Date as VideoRelease, channel_name as channelname from videos 
                where extract(year from Published_Date) = 2022'''
    cursor.execute(query8)
    mydb.commit()
    table8=cursor.fetchall()
    df8=pd.DataFrame(table8,columns=["Name", "Video Publised On", "ChannelName"])
    st.write(df8)

elif question == '9.What is the average duration of all videos in each channel,and what are their corresponding channel names ?':
    query9 =  '''SELECT channel_name as channelname, AVG(Duration) AS average_duration FROM videos GROUP BY channel_name'''
    cursor.execute(query9)
    mydb.commit()
    table9=cursor.fetchall()
    df9= pd.DataFrame(table9, columns=['ChannelTitle', 'Average Duration'])
    T9=[]
    for index, row in table9.iterrows():
        channel_title = row['ChannelTitle']
        average_duration = row['Average Duration']
        average_duration_str = str(average_duration)
        T9.append({"Channel Title": channel_title ,  "Average Duration": average_duration_str})
    st.write(pd.DataFrame(T9))

elif question == '10.Which videos have the highest number of comments,and what are their corresponding channel names ?':
    query10 = '''select Title as VideoTitle, channel_name as channelname, Comments as Comments from videos 
                       where Comments is not null order by Comments desc'''
    cursor.execute(query10)
    mydb.commit()
    table10=cursor.fetchall()
    df10=pd.DataFrame(table10, columns=['Video Title', 'Channel Name', 'NO Of Comments'])
    st.write(df10)