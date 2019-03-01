"""
It is a script that has functions to parse work.ua site.
"""


def connect_to_database(host=False, database=False, username=False, password=False):
    """
    connect_to_database is a function that takes connection data and return a sqlalchemy engine.
    It is useful with pandas.

    :param host: example: 192.168.0.1 or localhost
    :param database: Database name
    :param username: example: postres
    :param password: password to database
    :return: a sqlalchemy engine
    """
    from sqlalchemy import create_engine
    import getpass

    if host is False:
        host = input("Host: ")
    if database is False:
        database = input("Database: ")
    if username is False:
        username = input("Username: ")
    if password is False:
        password = getpass.getpass(prompt='Password: ', stream=None)

    return create_engine(f'postgresql://{username}:{password}@{host}/{database}')


def get_cities():
    """
    get_cities is a function that parse work.ua site to get cities for parsing vacancies.
    It saves information about cities like:
    - city_name         name of city or town. Example: "Київ"
    - city_lat_name     name of city or town using Latin alphabet. Example: "kyiv"
    - city_link         url. Example: "https://www.work.ua/jobs-kyiv/"
    - city_latitude     latitude. Example: "50.4501071"
    - city_longitude    longitude. Example: "30.5240501"

    :return: list of cities from work.ua (city_lat_name)
    """
    import requests
    import pandas as pd
    from bs4 import BeautifulSoup
    from time import time
    import datetime
    from geopy.geocoders import Nominatim
    from ll import ll

    site_link = "https://www.work.ua"
    engine = connect_to_database()
    start = time()
    for i in range(0, 1000):
        start_page = time()
        url = f"{site_link}/jobs/?region={i}advs=1"
        response = requests.get(url)
        results_page = BeautifulSoup(response.content, 'lxml')
        if results_page.find('h1', id="cityPage") is not None:
            city_id = i
            city_link = results_page.find('meta', property="og:url").get('content')
            city_name = results_page.find('input', id="city").get('value')
            city_lat_name = city_link[25:-1]
            if city_lat_name in ll:
                city_latitude = ll[city_lat_name]['city_latitude']
                city_longitude = ll[city_lat_name]['city_longitude']
                print(city_lat_name, city_latitude, city_longitude)
            else:
                geolocator = Nominatim()
                location = geolocator.geocode(city_lat_name.replace("_", " "), timeout=10)
                if location is not None:
                    city_latitude = location.latitude
                    city_longitude = location.longitude
                else:
                    location = geolocator.geocode(city_name, timeout=10)
                    if location is None:
                        city_latitude = None
                        city_longitude = None
                    else:
                        city_latitude = location.latitude
                        city_longitude = location.longitude
            cities_dict = {"city_id": city_id,
                           "city_name": city_name,
                           "city_lat_name": city_lat_name,
                           "city_link": city_link,
                           "city_latitude": city_latitude,
                           "city_longitude": city_longitude}
            cities = pd.DataFrame(cities_dict, index=[0])
            cities.to_sql(f'city_work_ua_{datetime.date.today()}', engine, if_exists='append')
            print(i, city_name, time() - start_page, datetime.datetime.now())
        else:
            print(i, None, time() - start_page, datetime.datetime.now())
    print(time() - start)
    sql_query_city = f'SELECT * FROM public."city_work_ua_{datetime.date.today()}"'
    cities_list = pd.read_sql(sql_query_city, engine)
    city_lat_name_list = list(cities_list.city_lat_name)
    return city_lat_name_list


def get_categories():
    """
    get_categories is a function that parse work.ua site to get categories for parsing vacancies.
    It saves information about categories like:
    - category_names         name of category. Example: "IT, комп'ютери, інтернет"
    - category_links         url. Example: "https://www.work.ua/jobs-kyiv-it/"
    - category_values        like id. Example: "1"
    - category_lat_names     name of city or town using Latin alphabet. Example: "it"

    :return: list of categories from work.ua (category_lat_names)
    """
    import requests
    import pandas as pd
    import datetime
    from bs4 import BeautifulSoup

    site_link = "https://www.work.ua"
    url = f"{site_link}/jobs-kyiv/?advs=1"
    postgres_engine = connect_to_database()
    response = requests.get(url)
    page = BeautifulSoup(response.content, 'lxml')
    all_filters = page.find_all('a', class_="filter-link catlink")
    all_checkbox = page.find('div', id="category_selection").find_all('input', type="checkbox")
    category_names = list(map(lambda j: j.get_text(), all_filters))
    category_links = list(map(lambda j: f"{site_link}{j.get('href')}", all_filters))
    category_values = list(map(lambda j: j.get("value"), all_checkbox))
    category_lat_names = list(map(lambda j: j[30:-1], category_links))
    categories_dict = {
        "category_value": category_values,
        "category_name": category_names,
        "category_link": category_links,
        "category_lat_name": category_lat_names
    }
    categories = pd.DataFrame.from_dict(categories_dict)
    categories.to_sql(f'category_list_work_ua{datetime.date.today()}',
                      postgres_engine,
                      if_exists='replace')
    return list(categories.category_lat_name)


def get_vacancies(cities=('kyiv',), categories=('it',)):
    """
    get_vacancies is a function that parse work.ua site to get information about vacancies
    and save the data into database. It saves information about vacancies like:
    - vacancy_id            Example: '3430955'
    - vacancy_link          Example: 'https://www.work.ua/jobs/3430955/'
    - vacancy_title         Example: 'Big Data Architect'
    - company_title         Example: 'SoftServe'
    - vacancy_salary        Example: 'NULL' or '50000'
    - publication_date      Example: '2019-02-22'
    - vacancy_city          Example: 'kyiv'
    - vacancy_category      Example: 'it'

    :param cities: list of cities (default value is "kyiv").
                   Use get_cities() function to get list of all cities.

    :param categories: list of categories (default value is "it").
                       Use get_categories() function to get list of all categories.
    :return: pandas.DataFrame with information about vacancies
    """
    import requests
    import pandas as pd
    import datetime
    from bs4 import BeautifulSoup

    site_link = 'https://www.work.ua'
    cards1 = "card card-hover card-visited wordwrap job-link js-hot-block"
    cards2 = "card card-hover card-visited wordwrap job-link"
    sal = "nowrap"
    empty_page = 'За вашим запитом з вибраними фільтрами вакансій поки немає.'
    postgres_engine = connect_to_database()

    def parse_cards(all_cards, city, category_lat_name):
        """
        parse_card is a function to parse all cards with vacancy info that is got from web page.

        :param all_cards: list of cards
        :param city: current city
        :param category_lat_name: current category
        :return: if all_cards do not have information about vacancy
                 the function returns "False" else returns "None"
        """

        def convert_date(list_of_items):
            """
            convert_date is a function to convert date from string to datetime.date format
            :param list_of_items: list of information about date
            :return: datetime.date
            """
            month = int(list_of_items[1] \
                        .replace('січня', '1').replace('лютого', '2').replace('березня', '3') \
                        .replace('квітня', '4').replace('травня', '5').replace('червня', '6') \
                        .replace('липня', '7').replace('серпня', '8').replace('вересня', '9') \
                        .replace('жовтня', '10').replace('листопада', '11').replace('грудня', '12'))
            day = int(list_of_items[0])
            year = int(list_of_items[2])
            return datetime.date(year, month, day)

        vacancy_links = (list(map(lambda j: None if j.a is None else \
            f'{site_link}{j.a.get("href")}', all_cards)))
        vacancy_ids = (list(map(lambda j: None if j is None else \
            j.replace(f'{site_link}/jobs/', '').replace('/', ''), vacancy_links)))
        temp = (list(map(lambda j: None if j.a is None else \
            j.a.get('title').split(", вакансія від "), all_cards)))
        vacancy_titles = (list(map(lambda j: None if j == [] else j[0], temp)))
        temp_dates = (list(map(lambda j: None if j == [] else j[1].split(' '), temp)))
        publication_dates = (list(map(convert_date, temp_dates)))
        company_titles = (list(map(lambda i: None if i.b is None else i.b.get_text(), all_cards)))
        vacancy_salaries = (list(map(lambda j: None if j.find('span', class_=sal) is None else \
            int(j.find('span', class_=sal) \
                .get_text() \
                .replace('\xa0', '') \
                .replace('грн', '') \
                .replace('*', '')), all_cards)))
        multiplay = len(vacancy_ids)

        vacancy_cities = ((city + "===") * multiplay).split("===")[:-1]
        vacancy_categories = ((category_lat_name + "===") * multiplay).split("===")[:-1]
        data = {"vacancy_id": vacancy_ids, "vacancy_link": vacancy_links,
                "vacancy_title": vacancy_titles, "company_title": company_titles,
                "vacancy_salary": vacancy_salaries, "publication_date": publication_dates,
                "vacancy_city": vacancy_cities, "vacancy_category": vacancy_categories}
        if not data['vacancy_id']:
            return False
        temp_result = pd.DataFrame.from_dict(data)
        temp_result.to_sql(f'work_ua_vacancies_{datetime.date.today()}',
                           postgres_engine,
                           if_exists='append')
        print(city, category_lat_name, datetime.datetime.now())

    for city_name in cities:
        for category in categories:
            page_number = 1
            url = f"{site_link}/jobs-{city_name}-{category}/?page={page_number}"
            response = requests.get(url)
            results_page = BeautifulSoup(response.content, 'lxml')
            while results_page.find('b').get_text() != empty_page:
                cards = results_page.find_all('div', class_=cards1)
                if cards:
                    parse_cards(cards, city_name, category)

                cards = results_page.find_all('div', class_=cards2)
                if cards:
                    parse_cards(cards, city_name, category)
                page_number += 1
                url = f"{site_link}/jobs-{city_name}-{category}/?page={page_number}"
                response = requests.get(url)
                results_page = BeautifulSoup(response.content, 'lxml')
    result_sql = f'SELECT * FROM public."work_ua_vacancies_{datetime.date.today()}"'
    return pd.read_sql(result_sql, postgres_engine)
