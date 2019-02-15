def connect_to_database(host=False, database=False, username=False, password=False):
    """
    connect_to_database is a function that takes connection data and return a sqlalchemy engine.
    It is useful with pandas.

    :param host: example: 192.168.0.1 or localhost
    :param database: Database name
    :param username: example: postgres
    :param password: password to database
    :return: a sqlalchemy engine
    """
    from sqlalchemy import create_engine
    import getpass

    if not host:
        host = input("Host: ")
    if not database:
        database = input("Database: ")
    if not username:
        username = input("Username: ")
    if not password:
        password = getpass.getpass(prompt='Password: ', stream=None)

    return create_engine(f'postgresql://{username}:{password}@{host}/{database}')


def get_vacancy_links():
    """
    get_vacancy_links is a function to get all vacancy links from djinni.co site.
    It saves all links in SQL database.

    :return = pandas.DataFrame that has all vacancy links from djinni.co site
    """

    import datetime

    import pandas as pd
    import requests
    from bs4 import BeautifulSoup

    site_link = 'https://djinni.co'
    page_url = f"{site_link}/jobs/?page="
    jobs_item = "list-jobs__item"
    first_response = requests.get(page_url)
    first_page = BeautifulSoup(first_response.content, 'lxml')
    numbers_of_vacancies = int(first_page.find('small', class_="text-muted").get_text())
    numbers_of_pages = round(numbers_of_vacancies / 15) + 1
    engine = connect_to_database()
    all_time = datetime.datetime.now()
    for i in range(1, numbers_of_pages):
        start = datetime.datetime.now()
        response = requests.get(f'{page_url}{i}')
        results_page = BeautifulSoup(response.content, 'lxml')
        items = results_page.find_all('li', class_=jobs_item)
        vacancy_links = (list(
            map(lambda j: None if j.div is None else f'{site_link}{j.find("a", class_="profile").get("href")}', items)))
        data = {"vacancy_link": vacancy_links}
        df = pd.DataFrame.from_dict(data)
        df.to_sql(f'djinni_vacancy_links2_{datetime.date.today()}', engine, if_exists='append')
        print(f'{i}, {page_url}{i}', datetime.datetime.now() - start)

    query = f'SELECT vacancy_link FROM public."djinni_vacancy_links2_{datetime.date.today()}";'
    vacancy_links_df = pd.read_sql(query, engine)

    del engine, site_link, jobs_item,
    print(f'{numbers_of_pages - 1} pages were parsed, {numbers_of_vacancies} vacancy links was saved it took\
            {datetime.datetime.now() - all_time}')
    return vacancy_links_df


def get_vacancies(vacancy_links_df):
    """
    get_vacancies is function to parse djinni.co site.
    For every vacancy link, it gets information about vacancies from the site.
    It saves information about vacancies in SQL database.

    :param vacancy_links_df: pandas.DataFrame that has vacancy links
    :return: pandas.DataFrame that has information about vacancies like:
                - vacancy_link
                - position
                - specialization
                - city
                - title
                - published_date
                - recruiter
                - recruiter_company
                - recruiter_link
                - descriptions
                - about_company
                - index
    """
    import datetime

    import pandas as pd
    import requests
    from bs4 import BeautifulSoup

    def convert_date(temp):
        month = int(temp[1].replace('января', '1').replace('февраля', '2').replace('марта', '3')\
                                    .replace('апреля', '4').replace('мая', '5').replace('июня', '6')\
                                    .replace('июля', '7').replace('августа', '8').replace('сентября', '9')\
                                    .replace('октября', '10').replace('ноября', '11').replace('декабря', '12'))
        day = int(temp[0])
        year = int(temp[2])
        return datetime.date(year, month, day)

    site_link = 'https://djinni.co'
    ind = 0
    all_time = datetime.datetime.now()
    engine = connect_to_database()
    for url in vacancy_links_df.vacancy_link:
        start = datetime.datetime.now()
        response = requests.get(url)
        results_page = BeautifulSoup(response.content, 'lxml')
        ind += 1
        position = results_page.find("div", class_="page-header").find("h1")\
            .get_text().replace("\n", "").replace("  ", "")
        specialization = results_page.find("div", class_="page-header").find_all("li")[1].get_text().replace("\n", "")
        try:
            city = results_page.find("div", class_="page-header").find_all("li")[2].get_text().replace("\n", "")
        except IndexError:
            city = specialization
            specialization = None
        try:
            title = results_page.find("p", class_="profile").get_text()
        except AttributeError:
            title = None
        try:
            published_date = convert_date(results_page.find("div", class_="profile-page-section text-small")\
                                          .get_text().replace("\n", "").replace(".","").split(" ")[26:29])
        except AttributeError:
            published_date = None
        recruiter = results_page.find("img", class_="list-jobs__userpic back-recruiter-image").get('alt')
        if results_page.find("div", class_="list-jobs__details").get_text().replace("\n", "").replace("\xa0", "")\
                .split("   ")[0] == "":
            recruiter_company = results_page.find("div", class_="list-jobs__details").get_text().replace("\n", "")\
                .replace("\xa0", "").split("   ")[8]
        else:
            recruiter_company = results_page.find("div", class_="list-jobs__details").get_text().replace("\n", "")\
                .replace("\xa0", "").split("   ")[2]
        recruiter_link = f'{site_link}{results_page.find("div", class_="list-jobs__details").find("a").get("href")}'
        descriptions = results_page.find_all("div", class_="profile-page-section")[1].get_text().replace("\n", " ")
        try:
            about_company = results_page.find_all("div", class_="profile-page-section")[2].get_text().replace("\n", " ")
            if 'Вакансия опубликована' in about_company:
                about_company = None
        except IndexError:
            about_company = None

        data = {"vacancy_link": url, "position": position,
                "specialization": specialization, "city": city,
                "title": title, "published_date": published_date,
                "recruiter": recruiter, "recruiter_company": recruiter_company,
                "recruiter_link": recruiter_link, "descriptions": descriptions,
                "about_company": about_company, 'index': ind}

        if data['position'] == "":
            continue
        df = pd.DataFrame(data, index=[0])
        df.set_index('index')
        df.to_sql(f'djinni3_{datetime.date.today()}', engine, if_exists='append')
        print(ind, url, datetime.datetime.now() - start)

    query = f'SELECT vacancy_link FROM public."djinni3_{datetime.date.today()}";'
    vacancies = pd.read_sql(query, engine)
    del engine
    print(datetime.datetime.now() - all_time)
    return vacancies


get_vacancies(get_vacancy_links())
