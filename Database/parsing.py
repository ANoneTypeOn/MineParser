import datetime
from time import sleep
import logging
import localapi
import json
import codecs
from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains

from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.ui import WebDriverWait

from database import Parsing
from database import createdb

createdb()

checkpars = Parsing.checkpars
addpars = Parsing.addpars

options = Options()
options.add_argument("enable-automation")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--disable-gpu")
options.add_argument("--headless")
driver = webdriver.Chrome(options=options)

logging.basicConfig(filename='../logs/parsing.log', level=logging.ERROR)


def griefstation():
    data = localapi.post('https://www.griefstation.ru/engine/ajax.php?type=donaters')
    decodeddata = codecs.decode(data.text.encode(), 'utf-8-sig')
    jsonobj = json.loads(decodeddata)
    for i in jsonobj:
        nickname = i['nick']
        date = i['date']
        donate = i['name']
        if checkpars(donate, nickname, 'griefstation'):
            continue
        else:
            addpars(nickname, donate, 'griefstation', date)
    # Готов


def craftyou():
    dats = str(datetime.date.today())
    response = localapi.get('https://craftyou.su/engine/ajax.php?type=donaters').text
    if response is not None and response != 'undefine':
        jsonobject = json.loads(response)
        for i in range(0, 5):
            nickname = jsonobject[i]['name']
            donate = jsonobject[i]['product']
            date = jsonobject[i]['date']
            if date.startswith('Сегодня'):
                date = date.replace('Сегодня в', dats)
            if checkpars(donate, nickname, 'craftyou'):
                continue
            else:
                addpars(nickname, donate, 'craftyou', date)
    # Готов


def lorencraft(driverd):
    driverd.get('http://shop.lcnew.ru/')
    sleep(2)

    cards = driverd.find_elements_by_class_name('card-footer.border-0')
    for i in cards:
        ActionChains(driverd).move_to_element(i).perform()
        fragment = str(i.text).replace('x1', '').split('/n')
        donate = fragment[0]
        nickname = fragment[1]
        date = str(datetime.date.today())
        if checkpars(donate, nickname, 'lorentcraft'):
            continue
        else:
            addpars(nickname, donate, 'lorentcraft', date)


class GetDataError(Exception):
    """POST/GET запрос пофейлился"""
    pass


def greenworld():
    data = localapi.get('https://gwdon.ru/app/components/ajax.php?method=LastBuy')
    if data.status_code == 200:
        encodebuffer = str(data.text).encode().decode('utf-8')
        jsondata = json.loads(encodebuffer)['html']
        for i in jsondata:
            nickname = i['NickName']
            donate = i['GroupName'] + f''' ({i['__Price']}р.)'''
            print(donate)
            date = datetime.date.today()
            if checkpars(donate, nickname, 'greenworld'):
                continue
            else:
                addpars(nickname, donate, 'greenworld', str(date))

    else:
        raise GetDataError
    return


def playmine(driverd):
    servers = ["https://playmine.ru/", "https://tntland.ru/", "https://griefland.ru/", "https://barsmine.ru/",
               "https://supermine.su/", "https://supergrief.ru/", "https://lucky-world.ru/", "https://musteryworld.ru/",
               "https://funnygame.su/", "https://sunny-world.su/", "http://lastmine.ru/"]

    serversname = ['playmine', 'tntland', 'griefland', 'barsmine', 'supermine', 'supergrief', 'luckyworld', 'musteryworld', 'funnygame', 'sunnyworld', 'lastmine']
    for d in range(len(servers)):
        j = servers[d]
        datenow = datetime.date.today()

        # Рендер сайта >>

        driverd.get(j)
        # << Рендер сайта

        sleep(2)  # Сон
        element = driverd.find_elements_by_class_name("payment-id.window.item-id")  # Парсинг элемента

        for i in element:
            x = str(i.text).split('\n')  # Создание списка

            # Небольшой костыль и проверка на пустоту значения >>
            try:
                nickname = (x[1]).replace('─ ', '')
            except IndexError:
                break
            # << Небольшой костыль и проверка на пустоту значения

            if '[' in x[0]:
                donate = (x[0]).replace('[', '').replace(']', '')
            else:
                donate = x[0]
            # << Основная часть

            # Проверка и добавление в БД >>
            if checkpars(donate, nickname, serversname[d]):
                continue
            else:
                addpars(nickname, donate, serversname[d], str(datenow))


def minegucci(driverd):
    driverd.get('https://minegucci.ru/')
    element = driverd.find_elements_by_class_name('last-buy-id')
    for i in element:
        chosenelement = str(i.text).split('\n')
        nickname = chosenelement[0]
        donate = chosenelement[1].removeprefix('[').removesuffix(']')

        date = datetime.date.today()
        if checkpars(donate, nickname, 'minegucci'):
            continue
        else:
            addpars(nickname, donate, 'minegucci', str(date))


def griefcube():
    jsonobject = localapi.get('https://griefcube.ru/engine/last.json').json()['last']
    for i in jsonobject:
        nickname = i['nick']
        donate = i['item'] + '(' + i['price'] + ')'
        time = i['date'].replace('/', '-')
        if checkpars(donate, nickname, 'griefcube'):
            continue
        else:
            addpars(nickname, donate, 'griefcube', time)


def foxmc(driverd):
    driverd.get('https://foxdonate.ru/engine/ajax.php?type=donaters')
    elements = driverd.find_elements_by_class_name('name')
    for i in elements:
        donates = ['Кейсы с донатом', 'Ключа Донат-Кейс', 'Ключ Донат-Кейс']
        content = i.text
        thelist = content.split('\n')
        nickname = thelist[0]
        donate = thelist[1].replace('[ ', '').replace(' ]', '').replace('[', '').replace(']', '')
        for z in donates:
            if 'Кейсы' in donate:
                donate = donate.replace(z, 'кейсы')
            if 'Ключ' in donate:
                donate = donate.replace(z, 'ключи')
        time = thelist[2].replace('Сегодня в', datetime.date.today())
        if checkpars(donate, nickname, 'foxmc'):
            continue
        else:
            addpars(nickname, donate, 'foxmc', time)


def sandpex(driverd):
    driverd.get('https://www.sandpex.ru/')
    sleep(2)
    thisel = driverd.find_elements_by_class_name('info-user')
    for i in thisel:
        i = i.text
        b = i.split('\n')
        nickname = b[0]
        donate = b[1].replace('Купил ', '').replace('Купила ', '')
        date = datetime.date.today()

        if checkpars(donate, nickname, 'sandpex'):
            continue
        else:
            addpars(nickname, donate, 'sandpex', str(date))


def litecloud(driverd):
    driverwait = WebDriverWait(driver, 2)
    driver.get('https://litecloud.me/login')
    driverd.find_element_by_id('player_name').send_keys('admin')
    sleep(1)
    driverd.find_element_by_id('shop-login-button').click()
    driverwait.until(ec.url_changes(''))
    sleep(2)
    elements = driverd.find_elements_by_class_name('nickname-box-info')
    for i in elements:
        element = str(i.text)
        if element.startswith('Admin'):
            continue
        element = element.split('\n')
        nickname = element[0]
        donate = element[1]
        if donate == 'Бургер админу' or donate == 'На жвачку вайлу':
            continue
        date = element[2].replace(' января ', '-1-').replace(' февраля ', '-2-').replace(' марта ', '-3-').replace(
            ' апреля ', '-4-').replace(' мая ', '-5-').replace(' июня ', '-6-').replace(' июля ', '-7-').replace(' августа ', '-8-').replace(
            ' сентября ', '-9-').replace(' октября ', '-10-').replace(' ноября ', '-11-').replace(' декабря ', '-12-')
        if checkpars(donate, nickname, 'litecloud'):
            continue
        else:
            addpars(nickname, donate, 'litecloud', date)


def minerush(driverd):
    driverd.get('https://minerush.ru/')
    elements = driverd.find_elements_by_class_name('last-buy-wrapper')
    for i in elements:
        element = i.text
        k = element.split('\n')
        nickname = k[0]
        donate = k[1].replace('[', '').replace(']', '')
        date = k[2].replace('Сегодня в', datetime.date.today())
        if checkpars(donate, nickname, 'minerush'):
            continue
        else:
            addpars(nickname, donate, 'sandpex', date)


def magicstore(driverd):
    donateslist = ['1 ключ ', '5 ключе', '10 ключ', 'Г.Потте', 'Оверлор', 'Божеств', 'Статус ', 'Сброс о', '200 Жет',
                   '300 Жет', '500 Жет', '1000 Же', '$10.000', '$20.000', '$50.000', '$100.00', '$200.00', '10000 О',
                   '20000 О']

    driverd.get('https://mc-magicstore.ru/')
    sleep(2)
    nnames = driverd.find_elements_by_class_name('name')
    nnames.remove(nnames[0])

    previlegies = driverd.find_elements_by_class_name('priv')
    dates = driverd.find_elements_by_class_name('date')
    for i in range(len(nnames) - 1):
        nickname = nnames[i].text
        donate = str(previlegies[i].text).replace('...', '')
        if donate.startswith(donateslist[0]):
            donate = '1 ключ'
        if donate.startswith(donateslist[1]):
            donate = '5 ключей'
        if donate.startswith(donateslist[2]):
            donate = '10 ключей'
        if donate.startswith(donateslist[3]):
            donate = 'Г.Поттер'
        if donate.startswith(donateslist[4]):
            donate = 'Оверлорд'
        if donate.startswith(donateslist[5]):
            donate = 'Божество'
        if donate.startswith(donateslist[6]):
            donate = 'YouTube'
        if donate.startswith(donateslist[7]):
            donate = 'Сброс очков (RPG Surv.)'
        if donate.startswith(donateslist[8]):
            donate = '200 жетонов'
        if donate.startswith(donateslist[9]):
            donate = '300 жетонов'
        if donate.startswith(donateslist[10]):
            donate = '500 жетонов'
        if donate.startswith(donateslist[11]):
            donate = '1000 жетонов'
        if donate.startswith(donateslist[12]):
            donate = donateslist[12] + ' Skyblock'
        if donate.startswith(donateslist[13]):
            donate = donateslist[13] + ' Skyblock'
        if donate.startswith(donateslist[14]):
            donate = donateslist[14] + ' Skyblock'
        if donate.startswith(donateslist[15]):
            donate = donateslist[15] + ' Skyblock'
        if donate.startswith(donateslist[16]):
            donate = donateslist[16] + ' Skyblock'
        if donate.startswith(donateslist[17]):
            donate = donateslist[17] + ' Маг. Hardcore'
        if donate.startswith(donateslist[18]):
            donate = donateslist[18] + 'Маг. Hardcore'
        if donate.startswith('Лорд'):
            donate = 'Лорд'

        date = str(dates[i].text).replace('Сегодня в', str(datetime.date.today()))

        if checkpars(donate, nickname, 'magicstore'):
            continue
        else:
            addpars(nickname, donate, 'magicstore', date)


def sunrise(driverd):
    driverd.get('https://sunmc.ru/')
    sleep(2)
    elements = driverd.find_elements_by_class_name('main-page__purchases-item-info')
    for i in elements:
        element = str(i.text).split('\n')
        if element == ['']:
            continue
        donate = element[0].replace('Покупка рублей', 'Рубли').replace('Кейс с донатом', 'Кейс')
        nickname = element[1]
        date = str(datetime.date.today())
        if checkpars(donate, nickname, 'sunrise'):
            continue
        else:
            addpars(nickname, donate, 'sunrise', date)


def jetmine(driverd):
    driverd.get('https://jetmine.ru/')
    sleep(2)
    k = driver.find_elements_by_class_name('panel')
    for i in k:
        i = str(i.text).split('\n')
        if i == ['']:
            continue
        donate = i[2].replace('ПРИВИЛЕГИЮ', '').replace('КЕЙС С С ДОНАТОМ', 'Кейс')
        nickname = i[0]
        date = datetime.date.today()
        if checkpars(donate, nickname, 'jetmine'):
            continue
        else:
            addpars(nickname, donate, 'jetmine', str(date))


def destroycraft(driverd):
    driverd.get('http://ds-craft.ru')
    sleep(2)
    a = driverd.find_element_by_class_name('online_purchases_game-container.live')
    chains = ActionChains(driverd)
    chains.move_to_element(a)
    chains.perform()
    sleep(0.5)
    b = a.find_elements_by_css_selector('*')
    for i in b:
        data = i.get_attribute('data-original-title').split('\n')
        nickname = data[0].removeprefix('Ник: ')
        donate = data[1].removeprefix('                ')
        date = datetime.date.today()

        if checkpars(donate, nickname, 'destroycraft'):
            continue
        else:
            addpars(nickname, donate, 'destroycraft', str(date))


def unigrief(driverd):
    driverd.get('https://unitgrief.ru/')
    sleep(2)
    a = driverd.find_elements_by_class_name('card-footer.pb-0.border-0.text-center')
    for i in a:
        chains = ActionChains(driverd)
        chains.move_to_element(i)
        chains.perform()
        sleep(0.5)

        data = str(i.text).split('\n')

        donate = ''
        for o in range(3):  # Дабы если что иксы не просачивались (к примеру - '1 Кейс с рублями, x1')
            donate = data[0].removesuffix(', x' + str(o))
        nickname = data[1]
        date = datetime.date.today()

        if donate == '':
            continue

        if checkpars(donate, nickname, 'unigrief'):
            continue
        else:
            addpars(nickname, donate, 'unigrief', str(date))


def grandworld():
    data = localapi.post('https://grand-buy.ru/engine/ajax.php?type=donaters')
    text = data.text
    today = datetime.date.today()
    for i in text.splitlines():
        if i.startswith('             title="Игрок: <b>'):
            i = i.removeprefix('             title="Игрок: <b>').replace('</b>', '').replace('<br>', '')
            splitt = i.split('Товар: ')
            splittt = splitt[1].split('Дата: ')

            nickname = splitt[0]
            donate = splittt[0]
            datez = splittt[1].removesuffix('">')
            if datez.startswith('Сегодня'):
                date = today
            else:
                try:
                    date = today.replace(day=(today.day - 1))
                except ValueError:
                    month = today.month - 1
                    day = 31 - 1
                    date = today.replace(month=month, day=day)

            if checkpars(donate, nickname, 'grandworld'):
                continue
            else:
                addpars(nickname, donate, 'grandworld', str(date))
        else:
            continue


def blackrise(driverd):
    driverd.get('https://blackrise.ru/')
    sleep(2)
    a = driverd.find_elements_by_class_name('card-footer.pb-0.border-0.text-center')
    for i in a:
        chains = ActionChains(driverd)
        chains.move_to_element(i)
        chains.perform()
        sleep(0.5)

        data = str(i.text).split('\n')

        donate = ''
        for o in range(3):  # Дабы если что иксы не просачивались (к примеру - '1 Кейс с рублями, x1')
            donate = data[0].removesuffix(', x' + str(o))
        nickname = data[1]
        date = datetime.date.today()

        if donate == '':
            continue

        if checkpars(donate, nickname, 'blackrise'):
            continue
        else:
            addpars(nickname, donate, 'blackrise', str(date))


def lattycraft():
    donatelst = ['Dragon', 'Latty', 'Elite', 'Titan', 'Pro', 'Vip', '25 кейсов', '10 кейсов', '5 кейсов', '2 кейса',
                 'Кейс', '5000 рублей', '1500 рублей', '700 рублей', '500 рублей', '150 рублей', '50 рублей',
                 '20 рублей', 'Полный разбан']
    jsonobject = localapi.get('https://lattycraft.ru/shop/ajax/purchases').json()['result']
    for i in jsonobject:
        donate = 'Кейс'
        try:
            donate = donatelst[i['product_id'] - 1]
        except IndexError:
            if i['product_id'] == 33:
                donate = 'Кейс'
        nickname = i['account']
        date = datetime.date.today()
        if checkpars(donate, nickname, 'lattycraft'):
            continue
        else:
            addpars(nickname, donate, 'lattycraft', str(date))


try:
    while True:
        try:
            griefstation()
        except Exception as e:
            logging.exception(f'Exception: {e}. GriefStation')
        logging.info('GriefStation закончен')

        try:
            lorencraft(driver)
        except Exception as e:
            logging.exception(f'Exception: {e}. Lorencraft')
        logging.info('Lorencraft закончен')

        try:
            greenworld()
        except Exception as e:
            logging.exception(f'Exception: {e}. Greenworld')
        logging.info('Greenworld закончен')

        try:
            craftyou()
        except Exception as e:
            logging.exception(f'Exception: {e}. Craftyou')
        logging.info('Craftyou закончен')

        try:
            blackrise(driver)
        except Exception as e:
            logging.exception(f'Exception: {e}. Blackrise')
            logging.info('Blackrise закончен')

        try:
            unigrief(driver)
        except Exception as e:
            logging.exception(f'Exception: {e}. Unigrief')
        logging.info('Unigrief закончен')

        try:
            destroycraft(driver)
        except Exception as e:
            logging.exception(f'Exception: {e}. Destroycraft')
        logging.info('Destroycraft закончен')

        try:
            lattycraft()
        except Exception as e:
            logging.exception(f'Exception: {e}. Lattycraft')
        logging.info('Lattycraft закончен')

        try:
            griefcube()
        except Exception as e:
            logging.exception(f'Exception: {e}. Griefcube')
        logging.info('Griefcube закончен')

        try:
            playmine(driver)
        except Exception as e:
            logging.exception(f'Exception: {e}. Playmine')
        logging.info('Playmine закончен')

        try:
            grandworld()
        except Exception as e:
            logging.exception(f'Exception: {e}. Grandworld')
        logging.info('Grandworld закончен')

        try:
            sunrise(driver)
        except Exception as e:
            logging.exception(f'Exception: {e}. Sunrise')
        logging.info('Sunrise закончен')

        try:
            jetmine(driver)
        except Exception as e:
            logging.exception(f'Exception: {e}. Jetmine')
        logging.info('Jetmine закончен')

        try:
            magicstore(driver)
        except Exception as e:
            logging.exception(f'Exception: {e}. Magicstore')
        logging.info('Magicstore закончен')

        try:
            minerush(driver)
        except Exception as e:
            logging.exception(f'Exception: {e}. Minerush')
        logging.info('Minerush закончен')

        try:
            minegucci(driver)
        except Exception as e:
            logging.exception(f'Exception: {e}. Minegucci')
        logging.info('Minegucci закончен')

        try:
            litecloud(driver)
        except Exception as e:
            logging.exception(f'Exception: {e}. Litecloud')
        logging.info('Litecloud закончен')

        try:
            sandpex(driver)
        except Exception as e:
            logging.exception(f'Exception: {e}. Sandpex')
        logging.info('Sandpex закончен')

        try:
            foxmc(driver)
        except Exception as e:
            logging.exception(f'Exception: {e}. Foxmc')
        logging.info('Foxmc закончен')

        sleep(2)

except Exception as e:
    driver.quit()
    logging.info(f'Парсинг завершен, причина: {e}')
    logging.info('\n\n\n\n')
    logging.shutdown()
