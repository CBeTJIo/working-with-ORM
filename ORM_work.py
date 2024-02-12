import sqlalchemy
import json
from sqlalchemy.orm import sessionmaker
import configparser
from models import create_tables, Publisher, Book, Shop, Stock, Sale
import datetime

config = configparser.ConfigParser()
config.read("settings.ini")
login = config["DATA"]["login"]
password = config["DATA"]["password"]
db_name = config["DATA"]["db_name"]
localhost = config["DATA"]["localhost"]

DSN = f'postgresql://{login}:{password}@localhost:{localhost}/{db_name}'
engine = sqlalchemy.create_engine(DSN)

create_tables(engine)

Session = sessionmaker(bind=engine)
session = Session()

with open("fixtures.json") as f:
    json_reader = json.load(f)

for record in json_reader:
    model = {
        'publisher': Publisher,
        'shop': Shop,
        'book': Book,
        'stock': Stock,
        'sale': Sale,
    }[record.get('model')]
    session.add(model(id=record.get('pk'), **record.get('fields'))) # нужно пояснение по двум звездочкам, что это значит? (**)
session.commit()

# По имени издателя выдать построчно факты покупки книг этого издателя:
# название книги | название магазина, в котором была куплена эта книга | стоимость покупки | дата покупки

pub_name = "O’Reilly"

result = session.query(Book.title, Shop.name, Sale.price, Sale.date_sale) \
    .join(Publisher, Publisher.id == Book.id_publisher) \
    .join(Stock, Stock.id_book == Book.id) \
    .join(Shop, Shop.id == Stock.id_shop)  \
    .join(Sale, Sale.id_stock == Stock.id) \
    .filter(Publisher.name == pub_name)

for row in result:
    print(f'{row[0]:<39} | {row[1]:<8} | {row[2]:<5} | {(row[3]).date()}')


session.close()