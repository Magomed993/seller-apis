import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """Получить список товаров с магазина яндекс маркет.

       Аргументы:
            page: страница
            campaign_id: идентификатор компании
            access_token: токен ключ
       Возврат:
            json: словарь товара с магазина
       Пример корректного исполнения функции:
            >>page= (страница)
            >>campaign_id = (id компании)
            >>access_token = (токен доступа)
            >>get_product_list(page, campaign_id, access_token)
            {
                джинсы: значение
            }
       Пример некорректного исполнения функции:
            >>campaign_id = (id компании)
            >>access_token = (токен доступа)
            >>get_product_list(page, campaign_id, access_token)
            Error

    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """Обновить остатки.

       Аргументы:
            stocks: остатки
            campaign_id: идентификатор компании
            access_token: токен доступа
       Возврат:
            {
                джинсы: новый остаток,
            }
       Пример корректного исполнения функции:
            >>stocks = остатки
            >>campaign_id = id компании
            >>access_token = токен доступа
            >>update_stocks(stocks, campaign_id, access_token)
            {джинсы: новый остаток,}
       Пример некорректного исполнения функции:
            >>campaign_id = id компании
            >>access_token = токен доступа
            >>update_stocks(stocks, campaign_id, access_token)
            Error
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """Обновить цены товаров.

       Аргументы:
            prices: цены
            campaign_id: идентификатор компании
            access_token: токен доступа
       Возврат:
            {
                джинсы: новая цена,
            }
       Пример корректного исполнения функции:
            >>prices = [цены]
            >>campaign_id = id компании
            >>access_token = токен доступа
            >>update_price(prices, campaign_id, access_token)
            {джинсы: новая цена,}
       Пример некорректного исполнения функции:
            >>access_token = токен доступа
            >>update_price(prices, campaign_id, access_token)
            Error

    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """Получить артикулы товаров Яндекс маркета.

       Аргументы:
            campaign_id: идентификатор компании
            market_token: токен доступа
       Возврат:
            list: список с артикулами
       Пример корректного исполнения функции:
            >>campaign_id = (id компании)
            >>market_token = (токен доступа)
            >>get_offer_ids(campaign_id, market_token)
            [список id]
       Пример некорректного исполнения функции:
            >>campaign_id = (id компании)
            >>get_offer_ids(campaign_id, market_token)
            Error
    """
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """Создать остатки.

       Аргументы:
            watch_remnants: остатки (часы) созданные функцией download_stock()
            offer_ids: артикли из полученных товаров
            warehouse_id: идентификатор склада
       Возврат: list(dict): создает список с вложенным словарем,
       в котором указан код и его остаток
       Пример корректного исполнения функции:
            >>watch_remnants = download_stock()
            >>offer_ids = get_offer_ids(...)
            >>warehouse_id = id склада
            >>create_stocks(watch_remnants, offer_ids, warehouse_id)
            [{остатки, количество которых зависит от количества остатков}]
       Пример некорректного исполнения функции:
            >>watch_remnants = download_stock(1, 2)
            >>offer_ids = get_offer_ids(...)
            >>create_stocks(watch_remnants, offer_ids, warehouse_id)
            Error
    """
    # Уберем то, что не загружено в market
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Создать цены.

       Аргументы:
            watch_remnants: остатки (часы) созданные функцией download_stock()
            offer_ids: артикли из полученных товаров
       Возврат: list(dict): создает список с вложенным словарем,
       в котором указаны значения цены определенного кода (его номера).
       Пример корректного исполнения функции:
            >>watch_remnants = download_stock()
            >>offer_ids = get_offer_ids(...)
            >>create_prices(watch_remnants, offer_ids)
            [{  "id": номер кода,
                "price": {
                    value: цена,
                    currenceId: RUR
                }
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
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """Загрузить цены.

       Аргументы:
            watch_remnants: остатки (часы) созданные функцией download_stock()
            campaign_id: идентификатор компании
            market_token: токен доступа
       Возврат: list(dict): перераспределенные цены.
       Пример корректного исполнения функции:
            >>watch_remnants = download_stock()
            >>campaign_id = (id компании)
            >>market_token = (токен доступа)
            >>upload_prices(watch_remnants, campaign_id, market_token)
            [{  price: цены
            },]
       Пример некорректного исполнения функции:
            >>watch_remnants = download_stock()
            >>campaign_id = (id компании)
            >>upload_prices(watch_remnants, campaign_id, market_token)
            Error

    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    """Загрузить остатки.

       Аргументы:
            watch_remnants: остатки (часы) созданные функцией download_stock()
            campaign_id: идентификатор компании
            market_token: токен доступа
            warehouse_id: идентификатор склада
       Возврат 2-ух результатов:
            not_empty - список запасов с ненулевым значением
            stocks - список с остатками зависящие от их количества
       Пример корректного исполнения функции:
            >>watch_remnants = download_stock()
            >>campaign_id = (id компании)
            >>market_token = (токен доступа)
            >>upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id)
            [список не равный нулю]
            [список с остатками]
       Пример некорректного исполнения функции:
            >>watch_remnants = download_stock()
            >>campaign_id = (id компании)
            >>upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id)
            Error

    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
