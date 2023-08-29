import requests
import random

def newbot(ip: str, port: int, nickname: str, server: str):
    data = {
        "ip": ip,
        "port": port,
        "nickname": nickname,
        "server": server
    }  # Действующая модель отправки данных
    data = requests.post('http://localhost:3010', json=data).text
    print(data)

# TODO: прикрутить в основу, серверная часть (прием команд, только добавление ботов) рабочий, с фильтрацией
newbot('localhost', 2, 'TEST' + str(random.randint(1, 7)), 'local' + str(random.randint(1, 99)))
