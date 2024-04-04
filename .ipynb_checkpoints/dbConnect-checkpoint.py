# db
import pymysql
from sqlalchemy import create_engine

# MySQL 데이터베이스 연결 설정
engine = create_engine('mysql+pymysql://hs:0317@localhost/pdgg')
con = pymysql.connect(
    host = 'localhost',
    port = 3306,
    user = 'hs',
    password = '0317',
    database = 'pdgg')
cur = con.cursor()