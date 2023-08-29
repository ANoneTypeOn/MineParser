from pyrogram.types import ReplyKeyboardMarkup, InlineKeyboardMarkup, InlineKeyboardButton, KeyboardButton

main_markup = ReplyKeyboardMarkup(
    [
        [
            KeyboardButton("Баланс"),  # Первая строка в клаве
            KeyboardButton("Магазин")
        ],
        [
            KeyboardButton("Пополнение баланса")  # Вторая строка в клаве
        ],
        [
            KeyboardButton("Обратная связь")  # Третья строка в клаве
        ]
    ], resize_keyboard=True)  # Авторазмер кнопок

submarkup = InlineKeyboardMarkup(
    [
        [
            InlineKeyboardButton('Купить', 'buysub')
        ]
    ]
)

balancemarkup = InlineKeyboardMarkup(
    [
        [
            InlineKeyboardButton('Главное меню', 'mainmenu')
        ],
        [
            InlineKeyboardButton('Магазин', 'shop')
        ]
    ], )

shop = ReplyKeyboardMarkup(
    [
        [
            KeyboardButton("Подписка"),
            KeyboardButton("Аккаунты")
        ],
        [
            KeyboardButton("Генерация страницы")
        ],
        [
            KeyboardButton("Баланс")
        ],
        [
            KeyboardButton("Главное меню")
        ]
    ], resize_keyboard=True)

apply = InlineKeyboardMarkup([[InlineKeyboardButton('Подтверждаю', callback_data='Pressed')]])

buyed = InlineKeyboardMarkup([[InlineKeyboardButton('Оплатил', callback_data='Checkbuy')]])
