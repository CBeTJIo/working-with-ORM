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

# ВЫгрузка данных из "fixtures.json"
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

# Поиск по имени или id издателя(input)
def get_shops(search_word):
    result = session.query(
        Book.title, Shop.name, Sale.price, Sale.date_sale
    ).select_from(Shop) \
        .join(Stock, Stock.id_shop == Shop.id) \
        .join(Book, Book.id == Stock.id_book) \
        .join(Publisher, Publisher.id == Book.id_publisher) \
        .join(Sale, Sale.id_stock == Stock.id)
    if search_word.isdigit():
        f_result = result.filter(Publisher.id == search_word).all()
    else:
        f_result = result.filter(Publisher.name == search_word).all()
    for book_name, shop_name, sale_cost, sale_date in f_result:
        print(f"{book_name: <40} | {shop_name: <10} | {sale_cost: <8} | {sale_date.strftime('%d-%m-%Y')}")

if __name__ == '__main__':
    search_word = input("Введите имя или айди публициста: ")
    get_shops(search_word)

session.close()