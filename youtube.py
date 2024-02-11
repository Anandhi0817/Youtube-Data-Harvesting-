from googleapiclient.discovery import build
import pymongo
import pymysql
import pandas as pd
import streamlit as st

#API key connection
def Api_connect():
    Api_Id="AIzaSyAXMG1tEiIAaXXqeeXArWF7GjgNimxeC9s"
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
def get_video_ids(channel_id):
    video_ids = []
    response = youtube.channels().list(
        id=channel_id,
        part='contentDetails'
    ).execute()
    
    if 'items' in response:
        playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    
    while True:
        # Fetch videos from the playlist
        playlist_items_response = youtube.playlistItems().list(
            playlistId=playlist_id,
            part='contentDetails',
            maxResults=50,
            pageToken=next_page_token
        ).execute()
        
        for item in playlist_items_response['items']:
            video_ids.append(item['contentDetails']['videoId'])
        
        next_page_token = playlist_items_response.get('nextPageToken')
        if not next_page_token:
            break

    return video_ids

#Getting video details
def get_video_info(video_ids):
    Video_data=[]
    for videos_id in video_ids:
        request=youtube.videos().list(
            part='snippet, ContentDetails, statistics',
            id=videos_id
        )
        response=request.execute()

        for item in response['items']:
            data=dict(Channel_Name = item["snippet"]["channelTitle"],
                    Channel_Id = item["snippet"]["channelId"],
                    Video_Id = item["id"],
                    Title = item["snippet"]["title"],
                    Thumbnail = item["snippet"]["thumbnails"],
                    Description = item["snippet"]["description"],    
                    Published_Date=item['snippet']['publishedAt'], 
                    Duration = item["contentDetails"]["duration"],
                    Like_Count = item['statistics'].get('likeCount'),
                    Views = item['statistics'].get('viewCount'),
                    Comments = item['statistics']['commentCount'])
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

# Mongo db Connections 
client=pymongo.MongoClient('mongodb://localhost:27017/')
mydb=client['Youtube']
collection = mydb["channel_details"]

#upload to Mongodb
def channel_details(channel_id):
        channel_details = get_channel_info(channel_id)
        video_ids = get_video_ids(channel_id)
        video_data = get_video_info(video_ids)
        comment_data = get_comment_info(video_ids)

        collection=mydb['channel_details']
        # Prepare the document for insertion
        channel_doc = {
            "channel_information": channel_details,
            "video_information": video_data,
            "comment_information": comment_data
        }
        # Insert the document
        result = collection.insert_one(channel_doc)
        return "Tables Created successfully !"

channel_id = "UC-O3_F-UpwzKvSkvO0DW9qg"  
insertion_status = channel_details(channel_id)
print(insertion_status)

#SQL Connection
myconnection = pymysql.connect(host = '127.0.0.1',user='root',passwd='Shalini@11')
cursor = myconnection.cursor()

# Establish Connection with current data
myconnection = pymysql.connect(host = '127.0.0.1',user='root',passwd='Shalini@11',database = "Youtube")
cursor = myconnection.cursor()

# Create videos table function
cursor.execute('''create table if not exists videos(Video_Id varchar(50) PRIMARY Key, 
              Channel_Name varchar(150), 
              Title varchar(150), 
              Description text, 
              Views bigint,
              Like_Count int,
              Published_Date timestamp,
              Comments int,
              Duration varchar(15),  
              Channel_Id varchar(100))''')
myconnection.commit()
print("Videos Table already created")

# Get video information and insert it into video table
vi_list = []
collection = mydb["channel_details"]

for vi_data in collection.find({}, {'_id': 0, 'video_information': 1}):
        if 'video_information' in vi_data:
            vi_list += vi_data['video_information']

df = pd.DataFrame(vi_list)

for _, row in df.iterrows():
    insert_query = '''
    INSERT IGNORE INTO videos (Video_Id,Channel_Name,Title,Description,Like_Count,Views,Comments,Published_Date,Duration,Channel_Id)
    
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s,%s,%s)'''
    
    values = (
        row['Video_Id'],
        row['Channel_Name'],
        row['Title'],
        row['Description'],
        row['Like_Count'],
        row['Views'],
        row['Comments'],
        row['Published_Date'],
        row['Duration'],
        row['Channel_Id']
    )
    cursor.execute(insert_query,values)
    myconnection.commit()
    
# Create channels table function
cursor.execute('''CREATE TABLE IF NOT EXISTS channels 
        (Channel_Name VARCHAR(50),
        Channel_Id VARCHAR(80) PRIMARY KEY, 
        Subscribers BIGINT, 
        Views BIGINT,
        Total_Videos INT,
        Playlist_Id VARCHAR(80),
        Channel_Description TEXT)''')   
myconnection.commit()
print("Channels Table already created !")

ch_list = []  

mydb = client['Youtube']
collection = mydb['channel_details']

for ch_data in collection.find({}, {'_id': 0, 'channel_information':1}):
    channel_info = ch_data.get('channel_information')
if channel_info:
    ch_list.append(channel_info)

df = pd.DataFrame(ch_list)

for _, row in df.iterrows():
    insert_query = '''
        INSERT IGNORE INTO channels (Channel_Name,Channel_Id,Subscribers,Views,Total_Videos,Playlist_Id,Channel_Description)
        
        VALUES (%s, %s, %s, %s, %s, %s, %s)'''
    
    values = (
        row['Channel_Name'],
        row['Channel_Id'],
        row['Subscribers'],
        row['Views'],
        row['Total_Videos'],
        row['Playlist_Id'],
        row['Channel_Description']
    )
    
    cursor.execute(insert_query, values)
    myconnection.commit()
    
# Create a comments table function
cursor.execute("""
    CREATE TABLE IF NOT EXISTS comments (
        Comment_Id varchar(100) PRIMARY KEY,
        Video_Id varchar(80),
        Comment_Text text,
        Comment_Author varchar(150))""")

myconnection.commit()
print("Comments Table already created !")

com_list = []
mydb = client["Youtube"]
collection = mydb["channel_details"]

if 'comment_information' in collection.find_one():
    for com_data in collection.find({}, {"_id": 0, "comment_information": 1}):
        comments = com_data.get("comment_information")
        if comments:
            com_list.extend(comments)
df = pd.DataFrame(com_list)

for _, row in df.iterrows():
    insert_query = '''
        INSERT IGNORE INTO comments (Comment_Id, Comment_Text, Video_Id, Comment_Author)
        
        VALUES (%s, %s, %s, %s)  '''
 
    values = (
        row['Comment_Id'],
        row['Comment_Text'],
        row['Video_Id'],
        row['Comment_Author']
    )
  
    cursor.execute(insert_query,values)
    myconnection.commit()       

# Total tables Creation 
def tables():
    channels_table()
    videos_table()
    comments_table()
    return "Tables Created successfully"

def channels_table():
    ch_list = []  
    mydb = client['Youtube']
    collection = mydb['channel_details']

    for ch_data in collection.find({}, {'_id': 0, 'channel_information':1}):
        channel_info = ch_data.get('channel_information')
    if channel_info:
        ch_list.append(channel_info)
    df = pd.DataFrame(ch_list)
    return channels_table

def videos_table():
    vi_list = []
    collection = mydb["channel_details"]

    for vi_data in collection.find({}, {'_id': 0, 'video_information': 1}):
        if 'video_information' in vi_data:
            vi_list += vi_data['video_information']
    df = pd.DataFrame(vi_list)
    return videos_table

def comments_table():
    com_list = []
    mydb = client["Youtube"]
    collection = mydb["channel_details"]

    if 'comment_information' in collection.find_one():
        for com_data in collection.find({}, {"_id": 0, "comment_information": 1}):
            comments = com_data.get("comment_information")
        if comments:
            com_list.extend(comments)
    df = pd.DataFrame(com_list)
    return comments_table

# Print the CSS to set the background
# Set the background color

st.markdown('''<style>@keyframes rainbowAnimation {2% { color: red; }        
                14% { color: orange; } 
                28% { color: yellow; } 
                42% { color: green; }
                56% { color: blue; }
                70% { color: indigo; }
                84% { color: violet; }
                100% { color: red; }
                }
                .rainbowText {animation: rainbowAnimation 4s infinite;}        
</style> <span class="rainbowText">Y</span><span class="rainbowText">O</span><span class="rainbowText">U</span><span class="rainbowText">T</span><span class="rainbowText">U</span><span class="rainbowText">B</span><span class="rainbowText">E</span><span class="rainbowText"> DATA</span><span class="rainbowText"> HARVESTING</span><span class="rainbowText"> AND</span><span class="rainbowText"> WAREHOUSING</span>''', unsafe_allow_html=True)
st.text("")
st.text("")

channel_id = st.text_input("Enter the channel ID")
if st.button("Collect and Store Data"):
    insertion_status = channel_details(channel_id)
    st.success(insertion_status)

# Button to migrate data to SQL
if st.button("Migrate to SQL"):
    Table = tables()
    st.success("Data migrated to SQL")

# Radio buttons to select which table to display
show_table=st.radio('SELECT TABLE FOR VIEW THE DETAILS',('CHANNELS','VIDEOS','COMMENTS'))

# Display selected table
if show_table=="CHANNELS":
    channels_table()

elif show_table=="VIDEOS":
    videos_table()

elif show_table=="COMMENTS":
    comments_table()

#Question Selection 
question = st.selectbox('SELECT QUESTION', ('1. All the videos and the Channel Name',
                                            '2. Channels with the most number of videos',
                                            '3. 10 most viewed videos',
                                            '4. Comments in each video',
                                            '5. Videos with the highest likes',
                                            '6. likes of all videos',
                                            '7. Views of each channel',
                                            '8. Videos published in the year 2022',
                                            '9. The average duration of all videos in each channel',
                                            '10. Videos with the highest number of comments'))


if question == '1. All the videos and the Channel Name':
    query1 = '''SELECT Title AS videos, `Channel_Name` AS Channel_Name FROM videos'''
    cursor.execute(query1)
    myconnection.commit() 
    table1=cursor.fetchall()
    df1=pd.DataFrame(table1, columns=["Video Title","Channel_Name"])
    st.write(df1)

elif question == '2. Channels with the most number of videos':
     query2 = '''SELECT Channel_Name as Channel_Name, Total_Videos as No_Videos FROM channels ORDER BY Total_Videos DESC'''
     cursor.execute(query2)
     myconnection.commit()
     table2 = cursor.fetchall()
     df2 = pd.DataFrame(table2, columns=["Channel_Name", "No of videos"])
     st.write(df2)

elif question == '3. 10 most viewed videos':
     query3 = '''SELECT Views AS views, Channel_Name AS Channel_Name, Title AS `Video Title` FROM videos WHERE Views IS NOT NULL ORDER BY Views DESC LIMIT 10'''
     cursor.execute(query3)
     myconnection.commit()
     table3= cursor.fetchall()
     df3=pd.DataFrame(table3, columns = ["Views","Channel_Name","Video Title"])
     st.write(df3)

elif question == '4. Comments in each videos':
     query4 = '''SELECT Comments AS No_Comments, Title AS `Video Title` FROM videos WHERE Comments IS NOT NULL'''
     cursor.execute(query4)
     myconnection.commit()
     table4=cursor.fetchall()
     df4=pd.DataFrame(table4, columns=["No Of Comments", "Video Title"])
     st.write(df4)

elif question == '5. Videos with highest likes':
     query5 ='''SELECT Title AS `Video Title`, Channel_Name AS Channel_Name, Like_Count AS Like_Count FROM videos WHERE Like_Count IS NOT NULL 
                ORDER BY Like_Count DESC'''           
     cursor.execute(query5)
     myconnection.commit()
     table5= cursor.fetchall()
     df5=pd.DataFrame(table5, columns=["Video Title","Channel_Name","Like_Count"])
     st.write(df5)

elif question == '6. Likes of all videos':
     query6 = '''SELECT Like_Count AS Like_Count, Title AS `Video Title` FROM videos'''
     cursor.execute(query6)
     myconnection.commit()
     table6= cursor.fetchall()
     df6=pd.DataFrame(table6, columns=["Like_Count", "Video Title"])
     st.write(df6)

elif question == '7. views of each channel':
     query7 = '''SELECT Channel_Name AS Channel_Name, Views AS Channel_View FROM channels'''
     cursor.execute(query7)
     myconnection.commit()
     table7=cursor.fetchall()
     df7=pd.DataFrame(table7, columns=["channel name","Total Views"])
     st.write(df7)

elif question == '8. videos published in the year 2022':
     query8 = '''SELECT Title AS `Video Title`, Published_Date AS `Video Release`, Channel_Name AS Channel_Name FROM videos WHERE YEAR(Published_Date) = 2022'''             
     cursor.execute(query8)
     myconnection.commit()
     table8 = cursor.fetchall()
     df8 = pd.DataFrame(table8, columns=["Name", "Video Published On", "Channel_Name"])
     st.write(df8)

elif question == '9. the average duration of all videos in each channel':
     query9 =  '''SELECT Channel_Name as Channel_Name, AVG(Duration) AS average_duration FROM videos GROUP BY channel_name'''
     cursor.execute(query9)
     myconnection.commit()
     table9 = cursor.fetchall()
     df9 = pd.DataFrame(table9, columns=['Channel Name', 'Average Duration'])
     T9 = []
     for row in table9:  
            channel_name = row[0]
            average_duration = row[1]
            average_duration_str = str(average_duration)
            T9.append({"Channel Name": channel_name ,  "Average Duration": average_duration_str})
     st.write(pd.DataFrame(T9))

elif question == '10. videos with the highest number of comments':
     query10 = '''SELECT Title AS `Video Title`, Channel_Name AS Channel_Name, Comments AS Comments FROM videos 
                       WHERE Comments IS NOT NULL ORDER BY Comments DESC'''
     cursor.execute(query10)
     myconnection.commit()
     table10=cursor.fetchall()
     df10=pd.DataFrame(table10, columns=['Video Title', 'Channel Name', 'NO Of Comments'])
     st.write(df10)
