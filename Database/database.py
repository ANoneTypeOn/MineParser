from datetime import date
from time import time
import psycopg2
from typing import Optional, Union
import configparser
from psycopg2.extras import NumericRange

userConn = psycopg2.connect(database='bot', user='parsing', password='papga', port='5432')
userConn.autocommit = True
cursor = userConn.cursor()


def createdb() -> None:
    """Функция для создания БД"""
    cursor.execute(
        f'CREATE TABLE IF NOT EXISTS Telegram(ID BIGINT, is_buyed BOOLEAN, buy_date DATE, expired_date DATE, records TEXT, attempts INTEGER, lastrow INT4RANGE NOT NULL, leftlists INTEGER, is_applied BOOLEAN, rowid INTEGER GENERATED ALWAYS AS IDENTITY);')
    cursor.execute('CREATE TABLE IF NOT EXISTS blacklist(ID INTEGER, reason TEXT);')
    cursor.execute('CREATE TABLE IF NOT EXISTS balance(ID INTEGER, balance INTEGER);')
    cursor.execute(
        'CREATE TABLE IF NOT EXISTS QIWI(UID INTEGER, trid INTEGER, sum INTEGER, date TEXT, rowid INTEGER GENERATED ALWAYS AS IDENTITY);')
    cursor.execute('INSERT INTO balance VALUES (560035353, -999) ON CONFLICT DO NOTHING;')
    cursor.execute('INSERT INTO balance VALUES (372611602, -999) ON CONFLICT DO NOTHING;')

    # cursor.execute('''INSERT INTO Telegram VALUES (YOURID, TRUE, '2021-01-01', '2099-12-01', '0', 99999, '[0, 2]', 99999, TRUE) ON CONFLICT DO NOTHING;''')

    cursor.execute(
        'CREATE TABLE IF NOT EXISTS parsed(nickname TEXT, donate TEXT, server TEXT, p_date DATE, rowid INTEGER GENERATED ALWAYS AS IDENTITY);')

    cursor.execute('''CREATE TABLE IF NOT EXISTS flood(uid INTEGER, timestamp INTEGER);''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS openaccounts(username TEXT, password TEXT, donate TEXT, server TEXT, price INTEGER, additionalauth BOOLEAN DEFAULT FALSE, rowid INTEGER GENERATED ALWAYS AS IDENTITY);''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS buyedaccounts(username TEXT, password TEXT, donate TEXT, server TEXT, price INTEGER, additionalauth BOOLEAN, rowid INTEGER);''')


class Shop:
    async def getprice(lotid: int) -> Union[tuple, None]:
        """Получение цены по ID лота"""
        cursor.execute('''SELECT username, donate, price FROM openaccounts WHERE rowid = %s;''', (lotid,))
        data = cursor.fetchone()
        return data

    async def remove(lotid: int) -> None:
        """Удаление купленных лотов"""
        cursor.execute('INSERT INTO buyedaccounts (username, password, donate, server, price, additionalauth, rowid) (SELECT * FROM openaccounts WHERE rowid = %s);', (lotid,))
        cursor.execute('DELETE FROM openaccounts WHERE rowid = %s;', (lotid,))

    async def buy(lotid: int, uid: int) -> Union[bool, str, None]:
        """Получение всех деталей (покупка) аккаунта из магазина"""
        cursor.execute('SELECT * FROM openaccounts WHERE rowid = {lotid};')
        data = cursor.fetchone()

        if data is not None:
            price = data[4]
            if await Bot.Bank.buy(uid, price):
                await Shop.remove(lotid)
                return data
            else:
                return 'Недостаточно средств на счету'

        else:
            return 'Аккаунта в продаже не найдено, деньги с баланса не списаны.\n\n Вероятно, лот продан... Или его попросту не существует'

    @staticmethod
    def returnaccounts() -> Union[tuple, None]:
        """Все аккаунты которые есть в открытой БД"""
        cursor.execute('SELECT username, donate, server, price, rowid FROM openaccounts;')
        data = cursor.fetchall()
        return data


class Parsing:  # ПАРСИНГ РЕШЕНО ОСТАВИТЬ СИНХРОННЫМ
    def addpars(nickname: str, donate: str, server: str, dates: str) -> None:
        """Добавление спаршенных данных"""
        cursor.execute('INSERT INTO parsed VALUES (%s, %s, %s, %s);', (nickname, donate, server, dates))

    def checkpars(donate: str, nickname: str, server: str) -> bool:
        """Проверка парсинга"""
        cursor.execute('SELECT nickname FROM parsed WHERE nickname = %s AND donate = %s AND server = %s;', (nickname, donate, server))
        nickn = cursor.fetchone()

        if nickn is not None and nickn != '':
            return True
        else:
            return False

    def data(server: str, nickname: str) -> Union[tuple, None]:
        """Вывод всех данных по юзеру на сервере"""
        cursor.execute('SELECT * FROM parsed WHERE nickname = %s AND server = %s;', (nickname, server,))
        data = cursor.fetchone()

        if data is not None:
            return data
        else:
            return None

    def getnickname(nickname: str, server: str) -> Union[tuple, None]:
        cursor.execute('SELECT * FROM parsed WHERE nickname = %s AND server = %s;', (nickname, server))
        info = cursor.fetchone()

        return info or None


class Bot:
    config = configparser.ConfigParser()
    config.read('config.ini')

    maxlists = config.getint('market', 'listslimit')
    vips = [YOURID]

    class Blacklist:
        async def blacklist(uid: int, reason: Optional[str] = None) -> True:
            """Внесение в ЧС"""
            if reason is not None:
                cursor.execute('INSERT INTO blacklist VALUES (%s, %s) ON CONFLICT DO NOTHING;', (uid, reason,))
            else:
                cursor.execute('INSERT INTO blacklist VALUES (%s) ON CONFLICT DO NOTHING;', (uid,))

            return True

        async def checkbl(uid: int) -> bool:
            """Проверка на ЧС"""
            if await Bot.TgTools.antiflood(uid):
                return True

            cursor.execute('SELECT ID FROM blacklist WHERE ID = %s;', (uid,))
            output = cursor.fetchone()

            if output is None:
                return False
            else:
                return True

    class Bank:
        async def apply(uid: int) -> None:
            """Функция подтверждения и согласия с условиями пополнения кивоса, платежку в будущем изменится"""
            cursor.execute('UPDATE Telegram SET is_applied = TRUE WHERE ID = %s;', (uid,))

        async def balance(uid: int) -> Union[int, None]:
            """Баланс юзверя"""
            if uid in Bot.vips:
                return 9999999

            cursor.execute('SELECT balance FROM balance WHERE ID = %s;', (uid,))
            output, = cursor.fetchone()
            return output

        async def checkapply(uid: int) -> Union[bool, None]:
            """Проверка на подтверждение"""
            cursor.execute('SELECT is_applied FROM Telegram WHERE ID = %s;', (uid,))

            try:
                result, = cursor.fetchone()
            except TypeError:
                return False

            return result

        async def balanceup(uid: int, datez: Union[int, str], transaction_id: int, trsum: int) -> None:
            """Увеличение баланса"""
            cursor.execute('INSERT INTO QIWI VALUES (%s, %s, %s, %s);', (int(uid), transaction_id, trsum, datez))
            bal = int(await Bot.Bank.balance(uid) + trsum)

            cursor.execute('UPDATE balance SET balance = %s WHERE ID = %s;', (bal, uid,))

        async def buy(uid: int, sumt: int) -> Union[bool, None]:
            """Покупка чего-то (в основном для магазина)"""
            if uid in Bot.vips:
                return True

            balance = int(await Bot.Bank.balance(uid)) - sumt
            if balance < 0 and uid not in Bot.vips:
                return False
            else:
                cursor.execute('UPDATE balance SET balance = %s WHERE ID = %s;', (balance, uid,))
                return True

        async def buysub(uid: int, subprice: int) -> [str, None]:
            """Покупка подписки"""
            balance = int(await Bot.Bank.balance(uid)) - subprice
            if balance < 0:
                return 'Недостаточно средств'
            date_today = date.today()

            try:
                dateexp = date_today.replace(month=date_today.month + 3)
            except ValueError:
                month = date_today.month - 9
                year = date_today.year + 1
                dateexp = date_today.replace(year=year, month=month)

            cursor.execute('UPDATE balance SET balance = %s WHERE ID = %s;', (balance, uid,))
            cursor.execute('UPDATE Telegram SET is_buyed = TRUE WHERE ID = %s;', (uid,))
            cursor.execute('UPDATE Telegram SET leftlists = %s WHERE ID = %s;', (Bot.maxlists, uid,))
            cursor.execute('UPDATE Telegram SET buy_date = %s WHERE ID = %s;', (date_today, uid,))
            cursor.execute('UPDATE Telegram SET expired_date = %s WHERE ID = %s;', (dateexp, uid,))

            return 'Операция прошла успешно'

    class Sublists:
        async def attemptlists(uid: int) -> Union[bool, int, None]:
            """Проверяет остатки по листам у юзверя"""
            cursor.execute('SELECT leftlists FROM Telegram WHERE ID = %s;', (uid,))

            try:
                output, = cursor.fetchone()
            except Exception as e:
                print(e)
                return False

            return output

        async def generatelist(userid: int) -> Union[list, bool, None]:
            """Генерация (первичное взятие данных с БД) листов для подписок"""
            cursor.execute('SELECT lastrow FROM Telegram WHERE ID = %s;', (userid,))

            try:
                lastrowid, = cursor.fetchone()
            except TypeError:
                lastrowid = NumericRange(1, 3)

            cursor.execute(
                'SELECT rowid FROM parsed WHERE rowid NOT BETWEEN %s AND %s ORDER BY random() LIMIT 1;', (lastrowid.lower - 10, lastrowid.upper - 1,))  # NumericRange выдает последнее значение на 1 больше чем было задано
            rowid, = cursor.fetchone()

            cursor.execute('SELECT * FROM parsed WHERE rowid >= %s LIMIT 10;', (rowid,))
            data = cursor.fetchall()
            balcheck = await Bot.Sublists.attemptlists(userid)

            if balcheck <= 0:
                return False

            cursor.execute('UPDATE Telegram SET lastrow = '[%s, %s]' WHERE ID = %s;', (rowid, rowid + 10, userid,))
            await Bot.Sublists.decreaselists(userid)

            return data

        async def decreaselists(uid: int) -> Union[int, None]:
            """Уменьшает листы юзверя"""
            cursor.execute('SELECT leftlists FROM Telegram WHERE ID = %s;', (uid,))
            output, = cursor.fetchone()
            cursor.execute('UPDATE Telegram SET leftlists = %s WHERE ID = %s;', (output - 1, uid,))

            if output is None:
                return None
            else:
                return output

    class TgTools:
        async def antiflood(uid: int) -> Union[bool, None]:
            """Функция от флуда"""
            cursor.execute('INSERT INTO flood VALUES (%s, %s);', (uid, time(),))

            cursor.execute('SELECT uid FROM flood WHERE uid = %s AND timestamp > %s;', (uid, int(time()) - 10,))
            data = cursor.fetchall()

            if len(data) >= 10:
                await Bot.Blacklist.blacklist(uid, 'Флуд командами')
                return True
            else:
                return False

        async def attempts(uid: int) -> Union[int, None]:
            """Проверяет попытки юзверя на пополнение кивоса"""
            cursor.execute('SELECT attempts FROM Telegram WHERE ID = %s;', (uid,))
            output, = cursor.fetchone()

            if output is None:
                return None
            else:
                return output

        async def updateattempts(uid: int, number: int) -> bool:
            """Обновление (обычно обнуление) попыток"""
            cursor.execute('UPDATE Telegram SET attempts = %s WHERE ID = %s;', (number, int(uid),))
            return True

        async def checkuser(uid: str) -> Union[int, None]:
            """Проверка, есть ли юзер в БД или нет"""
            cursor.execute('SELECT ID FROM Telegram WHERE ID = %s;', (uid,))

            if cursor.fetchone() is not None:
                return True
            else:
                return False

        async def subscription(uid: int) -> Union[bool, None]:
            """Проверка на подпиську"""
            cursor.execute('SELECT is_buyed FROM Telegram WHERE ID = %s;', (int(uid),))
            result, = cursor.fetchone()

            return result

        async def start(uid: int) -> Union[bool, None]:
            """Стартовая функция для юзверей"""
            cursor.execute('INSERT INTO Telegram VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s);', (uid, False, None, None, "", 3, [0, 2], 0, False,))

            if uid != 123456:
                cursor.execute('INSERT INTO balance VALUES(%s, %s);', (uid, 0,))
            else:
                return True

            return True

    class ScheduleTools:
        def setall(maxlists: int) -> None:
            """Обнуление попыток для купивших подпыську"""
            cursor.execute('UPDATE Telegram SET leftlists = %s WHERE is_buyed = True;', (maxlists, True,))

        @staticmethod
        async def floodvacuum() -> None:
            """Очистка устаревших записей надзора за флудом чтобы не засорять БД"""
            cursor.execute('DELETE FROM flood WHERE timestamp < %s;', (int(time()) - 10,))

        @staticmethod
        async def clearexpired() -> None:
            """Удаление подписок у юзеров, у которых подписки просрочены"""
            cursor.execute(
                               'UPDATE Telegram SET is_buyed = %s, buy_date = %s, expired_date = %s, leftlists = %s WHERE expired_date <= %s;', (False, None, None, 0, date.today(),)
                              )


class Debugging:
    def generateparses(ccount: int, dtime: str):
        for i in range(ccount):
            cursor.execute('INSERT INTO parsed VALUES (%s, %s, %s, %s);', (i, i, "debug", dtime,))

        return True

    def generateshop(ccount: int):
        for i in range(ccount):
            cursor.execute('INSERT INTO openaccounts VALUES (%s, %s, %s, %s, %s);', (i, i, i, "debug", 1,))

        return True
