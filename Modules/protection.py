from typing import Union


class Database:
    async def dropping(self: str):  # Антидроппинг БД
        listing = self.split(' ')
        for i in listing:
            word = str(i).lower()
            if word == 'drop' or word == 'database':
                return True
        return False

    async def checking(self: str):  # Проверка на длину команд и аргументов
        lenght = len(self)
        if lenght >= 15:
            return True
        return False

    async def fullcheck(self: Union[str, int]):  # Комплексная проверка
        if await Database.checking(str(self)) or await Database.dropping(str(self)):
            return True
        return False
