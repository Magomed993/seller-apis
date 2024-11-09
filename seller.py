import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """Получить список товаров магазина озон.

       Аргументы:
            last_id: последний идентификатор, входит в параметр payload
            client_id: идентификатор клиента
            seller_token: токен продавца
       Возврат:
            json: словарь с результатом
       Пример корректного исполнения функции:
            >>last_id = (id)
            >>client_id = (id клиента)
            >>seller_token = (токен продавца)
            >> get_product_list(last_id, client_id, seller_token)
            {
                словарь: значение
            }
       Пример некорректного исполнения функции:
            >>client_id = (id клиента)
            >>get_product_list(last_id, client_id, seller_token)
            Error

    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """Получить артикулы товаров магазина озон.

       Аргументы:
            client_id: идентификатор клиента
            seller_token: токен продавца
       Возврат:
            list: список с артикулами
       Пример корректного исполнения функции:
            >>client_id = (id клиента)
            >>seller_token = (токен продавца)
            >>get_offer_ids(client_id, seller_token)
            [список id]
       Пример некорректного исполнения функции:
            >>client_id = (id клиента)
            >>get_offer_ids(client_id, seller_token)
            Error

    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """Обновить цены товаров.

       Аргументы:
            prices (list): список цен
            client_id: идентификатор клиента
            seller_token: токен продавца
       Возврат:
            {
                джинсы: новая цена,
            }
       Пример корректного исполнения функции:
            >>prices = [list]
            >>client_id = (id клиента)
            >>seller_token = (токен продавца)
            >>update_price(prices, client_id, seller_token)
            {джинсы: новая цена,}
       Пример некорректного исполнения функции:
            >>client_id = (id клиента)
            >>update_price(prices, client_id, seller_token)
            Error

    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """Обновить остатки.

       Аргументы:
            stocks (list): список остатков
            client_id: идентификатор клиента
            seller_token: токен продавца
       Возврат:
            {
                джинсы: новый остаток,
            }
       Пример корректного исполнения функции:
            >>stocks = [list]
            >>client_id = (id клиента)
            >>seller_token = (токен продавца)
            >>update_stocks(stocks, client_id, seller_token)
            {джинсы: новый остаток,}
       Пример некорректного исполнения функции:
            >>stocks = [list]
            >>client_id = (id клиента)
            >>update_stocks(stocks, client_id, seller_token)
            Error
    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Скачать файл ostatki с сайта casio.

       Аргументы:
            нет
       Возврат: list(dict):
            [{
                код: значение,
                наименование товара: значение,
                изображение: значение,
                цена: значение,
                количество: значение
            }]
       Пример корректного исполнения функции:
            >>download_stock()
            [{код: значение,
            наименование товара: значение,
            изображение: значение,
            цена: значение,
            количество: значение}]
       Пример некорректного исполнения функции:
            >>download_stock(1, 2)
            Error
    """
    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """Создать остатки.

       Аргументы:
            watch_remnants: остатки (часы) созданные функцией download_stock()
            offer_ids: артикли из полученных товаров
       Возврат: list(dict): создает список с вложенным словарем,
       в котором указан код и его остаток
       Пример корректного исполнения функции:
            >>watch_remnants = download_stock()
            >>offer_ids = get_offer_ids(...)
            >>create_stocks(watch_remnants, offer_ids)
            [{offer_id: код, stock: остаток}]
       Пример некорректного исполнения функции:
            >>watch_remnants = download_stock(1, 2)
            >>offer_ids = get_offer_ids(...)
            >>create_stocks(watch_remnants, offer_ids)
            Error
    """
    # Уберем то, что не загружено в seller
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Создать цены.

       Аргументы:
            watch_remnants: остатки (часы) созданные функцией download_stock()
            offer_ids: артикли из полученных товаров
       Возврат: list(dict): создает список с вложенным словарем,
       в котором указаны значения по схожести offer_ids и кодом - подробное описание.
       Пример корректного исполнения функции:
            >>watch_remnants = download_stock()
            >>offer_ids = get_offer_ids(...)
            >>create_prices(watch_remnants, offer_ids)
            [{  "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": Код,
                "old_price": "0",
                "price": Цена
            },]
       Пример некорректного исполнения функции:
            >>watch_remnants = download_stock(1, 2)
            >>offer_ids = get_offer_ids(...)
            >>create_prices(watch_remnants, offer_ids)
            Error
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """Преобразовать цену.

       Аргументы:
            price (str): цена
       Возврат:
            str: цена заменённая на конкретно цифры
       Пример корректного исполнения функции:
            >>price = 5'990.00 руб.
            >> price_conversion(price)
            5990
       Пример некорректного исполнения функции:
            >>price = 5'990.00 руб.
            >>price_conversion(price)
            5'990.00 руб.

    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Разделить список lst на части по n элементов.

           Аргументы:
            lst (list): список
            n (int): на сколько частей делить список
       Возврат:
            lst: небольшие списки разделенные на n частей.
       Пример корректного исполнения функции:
            >>lst = [1, 2, 3, 4, 5, 6]
            >> n = 3
            >>divide(lst, n)
            [1, 2, 3]
            [4, 5, 6]
       Пример некорректного исполнения функции:
            >>lst = []
            >> n = 3
            >>divide(lst, n)
            [] - пустой список

    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """Загрузить цены.

       Аргументы:
            watch_remnants: остатки (часы) созданные функцией download_stock()
            client_id: идентификатор клиента
            seller_token: токен продавца
       Возврат: list(dict): создает список с вложенным словарем,
       в котором указаны значения по схожести offer_ids и кодом - подробное описание.
       Пример корректного исполнения функции:
            >>watch_remnants = download_stock()
            >>client_id = (id клиента)
            >>seller_token = (токен продавца)
            >>upload_prices(watch_remnants, client_id, seller_token)
            [{  "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": Код,
                "old_price": "0",
                "price": Цена
            },]
       Пример некорректного исполнения функции:
            >>watch_remnants = download_stock()
            >>client_id = (id клиента)
            >>upload_prices(watch_remnants, seller_token)
            Error

    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """Загрузить цены.

       Аргументы:
            watch_remnants: остатки (часы) созданные функцией download_stock()
            client_id: идентификатор клиента
            seller_token: токен продавца
       Возврат 2-ух результатов:
            not_empty - список запасов с ненулевым значением
            stocks - список с остатками зависящие от их количества
       Пример корректного исполнения функции:
            >>watch_remnants = download_stock()
            >>client_id = (id клиента)
            >>seller_token = (токен продавца)
            >>upload_stocks(watch_remnants, client_id, seller_token)
            [список не равный нулю]
            [список с остатками]
       Пример некорректного исполнения функции:
            >>watch_remnants = download_stock()
            >>client_id = (id клиента)
            >>upload_stocks(watch_remnants, seller_token)
            Error

    """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
