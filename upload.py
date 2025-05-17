from flask import Flask, request, jsonify
import vk_api
import sqlite3
import signal
import sys
from threading import Lock

app = Flask(__name__)

GROUP_TOKEN = "TOKEN"  # Токен сообщества
ACCESS_TOKEN = "TOKEN"  # Пользовательский токен для API, https://vkhost.github.io я брал отсюда
CONFIRMATION_CODE = "TOKEN"  # Код подтверждения Callback API
SECRET_KEY = "KEY"  # Секретный ключ (если используется)

vk_session = vk_api.VkApi(token=GROUP_TOKEN)
vk = vk_session.get_api()

hash_set = set()
db_lock = Lock() #нужно просмотреть 


def init_db():
    with db_lock:
        conn = sqlite3.connect('data/users.db')
        cursor = conn.cursor()

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS user (
            id INTEGER PRIMARY KEY,
            link TEXT,
            image TEXT
        )
        ''')

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS hash_sum (
            start_hash TEXT PRIMARY KEY
        )
        ''')

        conn.commit() #не используется транзакция и возможно, что не обязательно делать коммит
        conn.close()


def hash_from_db():
    with db_lock:
        conn = sqlite3.connect('data/users.db')
        cursor = conn.cursor()
        hashes = set([el[0] for el in cursor.execute('SELECT start_hash FROM hash_sum').fetchall()])
        conn.close()
        return hashes


def hash_to_db():
    with db_lock:
        conn = sqlite3.connect('data/users.db')
        cursor = conn.cursor()

        cursor.execute('DELETE FROM hash_sum')

        for h in hash_set:
            cursor.execute('INSERT OR IGNORE INTO hash_sum (start_hash) VALUES (?)', (h,))

        conn.commit() #не используется транзакция и возможно, что не обязательно делать коммит
        conn.close()


def send_message(user_id, message):
    vk.messages.send(
        user_id=user_id,
        message=message,
        random_id=0
    )


def db_insert(user_id, group_id, images):
    with db_lock:
        conn = sqlite3.connect('data/users.db')
        cursor = conn.cursor()
        img_str = ';'.join(images)
        cursor.execute(
            'INSERT OR REPLACE INTO user (id, link, image) VALUES (?, ?, ?)',
            (user_id, group_id, img_str)
        )
        conn.commit() #не используется транзакция и возможно, что не обязательно делать коммит
        conn.close()


def get_images_from_group(user_id, group_id):
    try:
        clean_group_id = group_id.split('?')[0]

        group_info = vk.groups.getById(group_id=clean_group_id)[0]
        posts = vk.wall.get(owner_id=-group_info['id'], count=100)['items']
        images = []

        for post in posts:
            attachments = post.get('attachments', [])
            for attachment in attachments:
                if attachment['type'] == 'photo':
                    photo = attachment['photo']
                    max_size_url = max(photo['sizes'], key=lambda size: size['width'])['url']
                    hash_img = str(photo['id'])
                    if hash_img in hash_set:
                        continue
                    hash_set.add(hash_img)
                    images.append(max_size_url)

        if not images:
            return "Вы уже загрузили все изображения из этой группы."

        db_insert(user_id, clean_group_id, images)
        return "Изображения успешно загружены!"

    except vk_api.exceptions.ApiError as e:
        return f"Ошибка VK API: {str(e)}"
    except Exception as e:
        return f"Произошла ошибка: {str(e)}"


@app.route('/', methods=['POST'])
def callback_handler():
    data = request.get_json()

    if 'secret' in data and data['secret'] != SECRET_KEY:
        return jsonify({'response': 'invalid secret key'}), 403

    if data['type'] == 'confirmation':
        return CONFIRMATION_CODE

    elif data['type'] == 'message_new':
        message = data['object']['message']
        user_id = message['from_id']
        text = message['text']

        group_id = text.split('/')[-1]

        result = get_images_from_group(user_id, group_id)
        send_message(user_id, result)

    return 'ok'


def shutdown_handler(signum, frame):
    print("\nСохранение данных перед завершением...")
    hash_to_db()
    sys.exit(0)


if __name__ == '__main__':
    init_db()
    hash_set = hash_from_db()

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    app.run(host='0.0.0.0', port=5000)