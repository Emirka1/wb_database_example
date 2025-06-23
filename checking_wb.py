import requests
import sqlite3
import json
import random
import time

proxies = {
    "http": "http://5435d268bba805db9446__cr.kz:1ce35e8a9661e50b@gw.dataimpulse.com:10000",
    "https": "http://5435d268bba805db9446__cr.kz:1ce35e8a9661e50b@gw.dataimpulse.com:10000"
}

conn = sqlite3.connect("products.db")
cursor = conn.cursor()

cursor.execute('''
    CREATE TABLE IF NOT EXISTS products (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        imt_id INTEGER UNIQUE,
        nm_id INTEGER,
        imt_name TEXT,
        description TEXT,
        contents TEXT,
        composition TEXT,
        color_names TEXT
    )
''')
conn.commit()

for i, article in enumerate(range(10000500, 10001000)):
    article = str(article)
    url = f"https://alm-basket-cdn-02.geobasket.ru/vol{article[0:3]}/part{article[0:5]}/{article}/info/ru/card.json"
    try:
        response = requests.get(url, proxies=proxies)
        if response.status_code != 200:
            print(f"Нет данных по артикулу {article}")
            continue
        data = response.json()
    except Exception as e:
        print(response.status_code, response.text)
        continue

    imt_id = data.get("imt_id")
    if not imt_id:
        continue

    cursor.execute("SELECT 1 FROM products WHERE imt_id = ?", (imt_id,))
    if cursor.fetchone():
        print(f"⚠️ Дубликат: imt_id {imt_id} уже в базе.")
        continue
    url = f"https://basket-01.wbbasket.ru/vol{article[0:3]}/part{article[0:5]}/{article}/info/price-history.json"
    try:
        response = requests.get(url, proxies=proxies)
        if response.status_code != 200:
            print(f"Нет данных по цене, по артикулу {article}")
            price = None
            continue
        price = response.json()[-1]['price']['RUB']
    except Exception as e:
        print(response.status_code, response.text)
        price = random.randint(1000,300000)

    nm_id = data.get("nm_id")
    imt_name = data.get("imt_name")
    description = data.get("description")
    contents = data.get("contents")
    composition = data["compositions"][0].get("name") if data.get("compositions") else None
    color_names = data.get("nm_colors_names")
    try:
        cursor.execute('''
            INSERT INTO products (imt_id, nm_id, imt_name, description, contents, composition, color_names, price)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (imt_id, nm_id, imt_name, description, contents, composition, color_names, str(price)))
        conn.commit()
        print(f"Сохранён товар: {imt_name} (imt_id: {imt_id})")
    except sqlite3.IntegrityError:
        print(f"Пропущен: товар с imt_id {imt_id} уже есть.")
        continue


conn.close()
