# Внешние зависимости
import configparser
import logging
from datetime import datetime
from pyrogram import Client, filters
from apscheduler.schedulers.asyncio import AsyncIOScheduler


# Внутренние зависимости
from Database import database
from Modules import keyboard, protection, qiwi, templates

config = configparser.ConfigParser()  # Конфиг
config.read('config.ini')

subprice = config.getint('market', 'subprice')
maxlists = config.getint('market', 'listslimit')

# Dev config
api_id = config.get('api', 'id')
api_hash = config.get('api', 'hash')
session = config.get('etc', 'session')
bot = config.get('api', 'token')

database.createdb()

starttime = (datetime.now()).strftime("%d.%m.%y %I:%M%p")

logging.basicConfig(filename='logs/log({}).log'.format(starttime), level=logging.ERROR)


client = Client(session, api_id, api_hash, bot_token=bot, sleep_threshold=5)


async def attempts(uid):
    """Функция для проверки количества оставшихся попыток пользователя на проверку оплаты"""
    attempt = int(await database.Bot.TgTools.attempts(uid))
    attempt = attempt - 1
    if attempt <= 0:
        await database.Bot.Blacklist.blacklist(uid, 'Все попытки исчерпаны')
        return None
    else:
        await database.Bot.TgTools.updateattempts(uid, attempt)
        return attempt


@client.on_message(filters.command(['start']))
async def start(cli, msg):
    """Команда /start. Проверка на черный список и далее"""
    if await database.Bot.Blacklist.checkbl(msg.from_user.id):
        return

    await database.Bot.TgTools.start(msg.from_user.id)
    await client.send_message(msg.from_user.id, templates.START_MESSAGE, reply_markup=keyboard.main_markup)


@client.on_message(filters.command(['buy']))
async def debugfunc(cli, msg):
    """Команда /buy. Проверка на черный список и далее"""
    if await database.Bot.Blacklist.checkbl(msg.from_user.id):
        return

    datas = str(msg.text).split(' ')
    datas.__delitem__(0)

    try:
        lotid = int(datas[0])
    except ValueError:
        await msg.reply('Чтобы купить что-то вставьте ID лота в команду /buy (id лота)')
        return

    result = await database.Shop.buy(lotid, msg.from_user.id)
    await client.send_message(msg.from_user.id, await templates.Generators.generatebuyed(result))


@client.on_message(filters.command(['check']))
async def checkcommand(cli, msg):
    """Команда /buy. Проверка на черный список и возможность добавить в ЧС при обнаружении попытки SQL-Инъекции"""
    if await protection.Database.fullcheck(msg.text):
        await msg.reply('Умно (нет)')
        await database.Bot.Blacklist.blacklist(msg.from_user.id, 'Попытка дропнуть БД')
        return

    uid = msg.from_user.id
    if await database.Bot.Blacklist.checkbl(uid):
        return

    result = await qiwi.check(uid)

    if result:
        trid = result[2]
        await database.Bot.TgTools.updateattempts(uid, 3)
        await database.Bot.Bank.balanceup(uid, int((datetime.now()).strftime("%d.%m.%y")), trid, result[1])
        await client.send_message(uid, 'Проверка прошла успешно, баланс пополнен!')
        return

    else:
        await client.send_message(uid, 'Бот не обнаружил транзакцию, Обращайтесь в Т/п.')


@client.on_callback_query()
async def check(cli, callbackq):
    if await database.Bot.Blacklist.checkbl(callbackq.from_user.id):
        return

    uid = int(callbackq.from_user.id)  # Юзверь ID

    if callbackq.data == 'mainmenu':  # Главное меню
        await client.send_message(uid, templates.START_MESSAGE, reply_markup=keyboard.main_markup)
        return

    if callbackq.data == 'shop':  # Магазин
        await client.send_message(uid, templates.SHOP, reply_markup=keyboard.shop)
        return

    if callbackq.data == 'Pressed':  # Подтверждение чтения условий
        if await database.Bot.Bank.checkapply(uid):
            await client.send_message(uid, 'Вы уже подтвердили, что прочитали все условия')
            await client.delete_messages(uid, callbackq.message.message_id)
            await client.send_message(uid, text=str(templates.FORM).format(uid), reply_markup=keyboard.buyed)
            return

        await database.Bot.Bank.apply(uid)
        await client.delete_messages(uid, callbackq.message.message_id)
        await client.send_message(uid, text=str(templates.FORM).format(uid), reply_markup=keyboard.buyed)
        return

    if callbackq.data == 'buysub':  # Приобритение подписки
        if await database.Bot.TgTools.subscription(uid):
            await client.delete_messages(uid, callbackq.message.message_id)
            await client.send_message(uid,
                                      'Подписка уже приобретена.\n\nДля получения информации по нику воспользуйтесь кнопкой "Генерация страницы"')
            return

        if await database.Bot.Bank.buysub(uid, subprice) == 'Недостаточно средств':
            await client.delete_messages(uid, callbackq.message.message_id)
            await client.send_message(uid, 'Недостаточно средств для покупки подписки')
            return

        await client.delete_messages(uid, callbackq.message.message_id)
        await client.send_message(uid, 'Поздравляем с приобретением подписки!')
        return

    if callbackq.data == 'Checkbuy':  # Проверка на пополнение
        result = await qiwi.check(uid)

        if result:
            trid = result[2]
            await database.Bot.TgTools.updateattempts(uid, 3)
            await database.Bot.Bank.balanceup(uid, int((datetime.now()).strftime("%d.%m.%y")), trid, result[1])

            await client.delete_messages(uid, callbackq.message.message_id)
            await client.send_message(uid, 'Проверка прошла успешно, баланс пополнен!')
            return

        else:
            await client.delete_messages(uid, callbackq.message.message_id)
            await client.send_message(uid, f'Бот не обнаружил транзакцию, проверьте статус платежа (должно быть "Успешно"), и комментарий. В случае если все правильно, воспользуйтесь командой `/check`.')
    return


@client.on_message(filters.text)
async def text(cli, msg):
    if await database.Bot.Blacklist.checkbl(msg.from_user.id):
        return

    if msg.text == 'Генерация страницы':
        databuffer = await database.Bot.Sublists.generatelist(msg.from_user.id)
        data = await templates.Generators.generatelist(databuffer)
        if str(data).startswith('Опа!'):
            await client.send_message(12356789, 'КОРАБЛЬ ТОНЕТ!')
            logging.error(
                'КОРАБЛЬ ТОНЕЕЕЕТ!')
        else:
            await client.send_message(msg.from_user.id, data)
        return

    if msg.text == 'Магазин':
        await client.send_message(msg.from_user.id, templates.SHOP, reply_markup=keyboard.shop)
        return

    if msg.text == 'Пополнение баланса':
        await client.send_message(msg.from_user.id, templates.HOWTOBUY, reply_markup=keyboard.apply)
        return

    if msg.text == 'Главное меню':  # Два главных меню, тут это триггер на текстовое сообщение, там же на нажатие кнопки
        await client.send_message(msg.from_user.id, templates.START_MESSAGE, reply_markup=keyboard.main_markup)
        return

    if msg.text == 'Баланс':  # Баланс юзера
        response = await templates.Generators.generatebal(msg.from_user.id)
        await client.send_message(msg.from_user.id, response, reply_markup=keyboard.balancemarkup)
        return

    if msg.text == 'Аккаунты':  # Аккаунты из accounts.json, в жсоне т.к заказчику так будет легче заносить
        accs = database.Shop.returnaccounts()
        if accs is None or accs == []:
            await client.send_message(msg.from_user.id, 'Сорян, но весь товар раскупили, приходи попозже\n\n <u>Как всегда</u>')
            return

        await client.send_message(msg.from_user.id, await templates.Generators.generateshop(accs))
        return

    if msg.text == 'Подписка':  # О подписке
        await client.send_message(msg.from_user.id, templates.SUBSCRIPTION, reply_markup=keyboard.submarkup)
        return

    if msg.text == 'FAQ':  # Канал FAQ бота
        await client.send_message(msg.from_user.id, templates.FAQ_MESSAGE)
        return


    await msg.reply(templates.UNKNOWN_COMMAND)
    return

schedule = AsyncIOScheduler()

schedule.add_job(database.Bot.ScheduleTools.clearexpired, "cron", day_of_week='*', hour='1', minute=0, second=0)
schedule.add_job(database.Bot.ScheduleTools.setall, "cron", day_of_week='*', hour='2', minute=0, second=0, args=[maxlists])
schedule.add_job(database.Bot.ScheduleTools.floodvacuum, "interval", seconds=10)

schedule.start()

try:
    client.run()
except Exception as e:
    schedule.pause()
    schedule.remove_all_jobs()
    schedule.shutdown()

    logging.exception(f'RAISED EXCEPTION {e}, SHUTTING DOWN')
    logging.info('\n\n')
    logging.shutdown()
