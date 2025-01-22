from pymysqlreplication import BinLogStreamReader
from pymysqlreplication import row_event
import configparser


# mysql에 접속하기 위한 정보 받아오기
parser = configparser.ConfigParser()
parser.read("pipeline.conf")
HOSTNAME = parser.get("mysql_config", "HOSTNAME")
PORT = parser.get("mysql_config", "PORT")
DB_USER_NAME = parser.get("mysql_config", "DB_USER_NAME")
DB_PASSWORD = parser.get("mysql_config", "DB_PASSWORD")
DATABASE = parser.get("mysql_config", "DATABASE")

# mysql 연결 정보 딕셔너리로 묶어둠
mysql_settings = {
    "host" : HOSTNAME,
    "port" : int(PORT),
    "user" : DB_USER_NAME,
    'passwd' : DB_PASSWORD
}

# 이진로그 읽기
b_stream = BinLogStreamReader(
    connection_settings=mysql_settings,
    server_id=100,
    only_events=[
        row_event.DeleteRowsEvent,
        row_event.WriteRowsEvent,
        row_event.UpdateRowsEvent
    ]
)

for event in b_stream:
    event.dump()
    
b_stream.close()