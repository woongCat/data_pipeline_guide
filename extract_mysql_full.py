import pymysql
import csv
import boto3
import configparser

# mysql에 접속하기 위한 정보 받아오기
parser = configparser.ConfigParser()
parser.read("pipeline.conf")
HOSTNAME = parser.get("mysql_config", "HOSTNAME")
PORT = parser.get("mysql_config", "PORT")
DB_USER_NAME = parser.get("mysql_config", "DB_USER_NAME")
DB_PASSWORD = parser.get("mysql_config", "DB_PASSWORD")
DATABASE = parser.get("mysql_config", "DATABASE")

# mysql에 연결하기
conn = pymysql.connect(
    host=HOSTNAME,
    user=DB_USER_NAME,
    password=DB_PASSWORD,
    db=DATABASE,
    port=int(PORT)
)

if conn is None:
    print("Error connecting to the MySql database")
else:
    print("MySQL connection established")

# 쿼리문 작성
m_query = "SELECT * FROM Orders;"

# 추출할 파일 이름
local_filename = "order_extract.csv"

# mysql에 연결 후 추출해서 전체 가져오기
m_cursor = conn.cursor()
m_cursor.execute(m_query)
results = m_cursor.fetchall()

# csv파일로 작성하기
with open(local_filename, 'w') as fp:
    csv_w = csv.writer(fp, delimiter=',')
    csv_w.writerows(results)

# 연 파일들 종료하기
fp.close()
m_cursor.close()
conn.close()

# S3에 접속하기 위한 정보 받아오기
parser = configparser.ConfigParser()
parser.read("pipeline.conf")
ACCESS_KEY = parser.get("aws_s3_config", "ACCESS_KEY")
SECRET_KEY = parser.get("aws_s3_config", "SECRET_KEY")
BUCKET_NAME = parser.get("aws_s3_config", "BUCKET_NAME")

# S3에 연결
s3 = boto3.client(
    's3',
    aws_access_key_id = ACCESS_KEY,
    aws_secret_access_key = SECRET_KEY
)

s3_file = local_filename

# S3에 업로드하기
s3.upload_file(local_filename, BUCKET_NAME, s3_file)