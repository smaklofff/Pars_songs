import sqlalchemy
from sqlalchemy import create_engine, Column, Table, and_
import os
import json
import sys
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from datetime import datetime
import xml.etree.ElementTree as ET
from urllib.request import urlopen
from lxml import etree
from lxml import html


class MyClass:
    DATABASE_CONFIG_FILE = 'database_config.json'
    DATABASE_PW_ENV_VAR_NAME = 'TABS4ME_REPORTS_DB_PW'
    pages_usr = {'amdm': ['https://amdm.ru/akkordi/popular/all/page3/',
                          'https://amdm.ru/akkordi/popular/all/page1/',
                          'https://amdm.ru/akkordi/popular/all/page2/'],
                 'my-chord': ['https://my-chord.net/top/all/age/page/1/',
                              'https://my-chord.net/top/all/age/page/2/',
                              'https://my-chord.net/top/all/age/page/3/']}

    def __init__(self):
        self.metadata = None
        self.table = None
        self.engine = None
        self.conn = None
        self.add_data = []

    def creat_connection_to_db(self):
        db_info = MyClass.readJsonConfigFile(MyClass.DATABASE_CONFIG_FILE)
        db_info['password'] = MyClass.get_pass_for_db()
        self.engine = create_engine(f"mysql+pymysql://{db_info['user']}:"
                                    f"{db_info['password']}@{db_info['host']}/{db_info['database']}")
        self.conn = self.engine.connect()

    def creat_table(self):
        self.metadata = sqlalchemy.MetaData()
        self.table = Table('parser_songs', self.metadata,
                           Column('id', sqlalchemy.Integer(), primary_key=True),
                           Column('band_name', sqlalchemy.String(200), nullable=False),
                           Column('song_name', sqlalchemy.String(200), nullable=False),
                           Column('added_at_time', sqlalchemy.DateTime(), nullable=False))
        self.metadata.create_all(self.engine)

    @staticmethod
    def readJsonConfigFile(filename):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        try:
            with open(os.path.join(current_dir, filename), 'r', encoding='utf-8') as file:
                return json.loads(file.read())
        except FileNotFoundError as exc:
            print('Cant find json config file with name: ' +
                  filename + '\n Error:' + str(exc))
            sys.exit(1)
        except Exception as exc:
            print('Cant find json config file with name: ' +
                  filename + '\n Error:' + str(exc))
            sys.exit(2)

    @staticmethod
    def get_pass_for_db():
        try:
            return os.environ.get(MyClass.DATABASE_PW_ENV_VAR_NAME)
        except Exception as exc:
            print('Environment variable \"' + MyClass.DATABASE_PW_ENV_VAR_NAME +
                  '\" is not set: ' + str(exc))
            sys.exit(3)

    def parse_amdm(self):
        for i in range(len(MyClass.pages_usr['amdm'])):
            try:
                with open(f'pages/amdm_{i}.html', encoding='utf-8') as input_file:
                    text = input_file.read()
            except FileNotFoundError as exc:
                print('Cant find json config file with name: ' +
                      filename + '\n Error:' + str(exc))
                sys.exit(4)

            tree = html.fromstring(text)
            try:
                bands_and_songs = tree.xpath('//body/div[@class = "content-table"]/article/table/tr/td[2]/*/text()')
            except Exception as exp:
                print("incorrect Xpath:\n" + exp)

            for i in range(0, len(bands_and_songs)-1,2):
                band_name = bands_and_songs[i]
                song_name = bands_and_songs[i+1].replace('ё', 'е')
                data_to_add = {
                    'band_name': band_name,
                    'song_name': song_name,
                    'added_at_time': datetime.now().strftime('%Y-%m-%d, %H:%M:%S')
                }
                MyClass.start_checking_duplicate(self, band_name, song_name, data_to_add)


    def parse_my_chords(self):
        for i in range(len(MyClass.pages_usr['my-chord'])):
            try:
                with open(f'pages/my-chord_{i}.html', encoding='utf-8') as input_file:
                    text = input_file.read()
            except FileNotFoundError as exc:
                print('Cant find json config file with name: ' +
                      filename + '\n Error:' + str(exc))
                sys.exit(4)

            tree = html.fromstring(text)
            try:
                bands_and_songs = tree.xpath('//*[@id="topnews-page"]/ul[1]/li/div[1]/a/text()')
            except Exception as exp:
                print("Incorrect Xpath:\n" + exp)

            for i_bands_and_songs in bands_and_songs:
                try:
                    band_name = i_bands_and_songs.split(' - ')[0]
                    song_name = i_bands_and_songs.split(' - ')[1].replace('ё', 'е')
                except IndexError:
                    continue
                data_to_add = {
                    'band_name': band_name,
                    'song_name': song_name,
                    'added_at_time': datetime.now().strftime('%Y-%m-%d, %H:%M:%S')
                }
                MyClass.start_checking_duplicate(self, band_name, song_name, data_to_add)


    def start_checking_duplicate(self, band_name, song_name, data_to_add):
        duplicate_in_db = MyClass.check_duplicate_in_db(self, band_name, song_name)
        duplicate_in_in_curr_session = MyClass.check_duplicate_in_curr_session(self, data_to_add)
        if not duplicate_in_db and not duplicate_in_in_curr_session:
            self.add_data.append(data_to_add)

    def check_duplicate_in_db(self, band_name, song_name):
        duplicate_in_db = sqlalchemy.select([self.table]).where(and_(
            self.table.c.band_name == band_name,
            self.table.c.song_name == song_name)
        )
        return bool(self.conn.execute(duplicate_in_db).fetchall())

    def check_duplicate_in_curr_session(self, data_to_add):
        if self.add_data:
            for row in self.add_data:
                if row['band_name'] == data_to_add['band_name'] and row['song_name'] == data_to_add['song_name']:
                    return True
        return False

    def insert_data(self):
        if self.add_data:
            self.conn.execute(sqlalchemy.insert(self.table), self.add_data)
        else:
            print('There are no new entries')

    @staticmethod
    def download_pages():
        headers = {'User-Agent': UserAgent().chrome, 'Content-Type': 'text/html'}
        for key in MyClass.pages_usr.keys():
            urls = MyClass.pages_usr[key]
            for page_number in range(len(urls)):
                page = requests.get(urls[page_number], headers=headers)
                path = f'pages/{key}_{page_number}.html'
                with open(path, mode='w', encoding='utf-8') as output_file:
                    output_file.write(page.text)


    def start_parsing(self):
        MyClass.creat_connection_to_db(self)
        MyClass.creat_table(self)
        MyClass.parse_amdm(self)
        MyClass.parse_my_chords(self)
        MyClass.insert_data(self)


parser = MyClass()
parser.download_pages() # скачать страницы
# parser.start_parsing()

