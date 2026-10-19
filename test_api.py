import json
import urllib.request
import urllib.error
import sys
from urllib.parse import urlencode
BASE_URL = "http://localhost:8000"

def send_request(path, method="GET", data=None, headers=None, params=None):
    url = f"{BASE_URL}{path}"
    headers = headers or {}
    if params:
        url += "?" + urlencode(params)
    if data:
        data_bytes = json.dumps(data).encode("utf-8")
        headers["Content-Type"] = "application/json"
    else:
        data_bytes = None

    req = urllib.request.Request(url, data=data_bytes, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req) as response:
            status = response.status
            body = response.read().decode("utf-8")
            return status, json.loads(body) if body else {}
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8")
        try:
            err_json = json.loads(body)
        except Exception:
            err_json = body
        return e.code, err_json
    except Exception as e:
        print(f"Ошибка соединения с {url}: {e}")
        sys.exit(1)

def run_tests():
    print("=== Запуск тестов API ===")

    # 1. Регистрация нового пользователя
    user_data = {
        "first_name": "Сидор",
        "second_name": "Сидоров",
        "birthdate": "1990-01-01",
        "gender": "Мужской",
        "biography": "Автоматический тест профиля",
        "city": "Новосибирск",
        "password": "test_password_123"
    }

    print("\n1. Тест регистрации пользователя (/user/register)...")
    status, res = send_request("/user/register", method="POST", data=user_data)
    if status == 201:
        user_id = res.get("user_id")
        print(f"[Успех] Пользователь зарегистрирован. ID: {user_id}")
    else:
        print(f"[Ошибка] Код ответа: {status}, Тело: {res}")
        sys.exit(1)

    # 2. Логин пользователя
    login_data = {
        "id": user_id,
        "password": "test_password_123"
    }
    print("\n2. Тест авторизации (/login)...")
    status, res = send_request("/login", method="POST", data=login_data)
    if status == 200:
        token = res.get("token")
        print(f"[Успех] Авторизация успешна. Токен: {token[:]}...")
    else:
        print(f"[Ошибка] Код ответа: {status}, Тело: {res}")
        sys.exit(1)

    # 3. Получение профиля пользователя
    print(f"\n3. Тест получения профиля пользователя (/user/get/{user_id})...")
    headers = {"Authorization": f"Bearer {token}"}
    status, res = send_request(f"/user/get/{user_id}", method="GET", headers=headers)
    if status == 200:
        print("[Успех] Данные профиля получены успешно:")
        print(json.dumps(res, indent=2, ensure_ascii=False))
    else:
        print(f"[Ошибка] Код ответа: {status}, Тело: {res}")
        sys.exit(1)

    # 4. Поиск пользователей
    print("\n3.1. Тест поиска пользователей (/user/search)...")

    status, res = send_request(
        "/user/search",
        method="GET",
        headers={
            "Authorization": f"Bearer {token}"
        },
        params={
            "first_name": "Ал",
            "second_name": "Пет"
        }
    )

    if status == 200:
        print(f"[Успех] Найдено пользователей: {len(res)}")
    else:
        print(f"[Ошибка] Код ответа: {status}, Тело: {res}")
        sys.exit(1)
    # 4. Тест авторизации с неверным паролем

    print("\n4. Тест авторизации с неверным паролем...")
    bad_login_data = {
        "id": user_id,
        "password": "wrong_password"
    }
    status, res = send_request("/login", method="POST", data=bad_login_data)
    if status == 401:
        print("[Успех] Запрос отклонен верным кодом 401")
    else:
        print(f"[Ошибка] Ожидался код 401, получен {status}: {res}")
        sys.exit(1)

    # 5. Тест получения профиля без авторизации
    print("\n5. Тест получения профиля без авторизации...")
    status, res = send_request(f"/user/get/{user_id}", method="GET")
    if status == 401 or status == 403:
        print(f"[Успех] Запрос успешно отклонен с кодом {status}")
    else:
        print(f"[Ошибка] Ожидался код 401/403, получен {status}: {res}")
        sys.exit(1)

    print("\n=== Все тесты пройдены успешно! ===")

    # 6. Получение несуществующего пользователя
    print("\n6. Тест получения несуществующего пользователя...")

    fake_id = "00000000-0000-0000-0000-000000000000"

    status, res = send_request(
        f"/user/get/{fake_id}",
        method="GET",
        headers={"Authorization": f"Bearer {token}"}
    )

    if status == 404:
        print("[Успех] Несуществующий пользователь отклонен 404")
    else:
        print(f"[Ошибка] Ожидался 404, получен {status}: {res}")


    # 7. Некорректный UUID
    print("\n7. Тест неправильного UUID...")

    status, res = send_request(
        "/user/get/12345",
        method="GET",
        headers={"Authorization": f"Bearer {token}"}
    )

    if status == 400:
        print("[Успех] Неверный UUID отклонен 400")
    else:
        print(f"[Ошибка] Ожидался 400, получен {status}: {res}")


    # 8. Регистрация без города
    print("\n8. Тест обязательных полей...")

    bad_user = {
        "first_name": "Иван",
        "second_name": "Петров",
        "birthdate": "1990-01-01",
        "gender": "М",
        "password": "123456"
    }

    status, res = send_request(
        "/user/register",
        method="POST",
        data=bad_user
    )

    if status == 422:
        print("[Успех] Валидация обязательных полей работает")
    else:
        print(f"[Ошибка] Ожидался 422, получен {status}: {res}")


    # 9. Короткий пароль
    print("\n9. Тест короткого пароля...")

    bad_password = {
        "first_name": "Тест",
        "second_name": "Пароль",
        "birthdate": "1990-01-01",
        "gender": "М",
        "city": "Москва",
        "password": "123"
    }

    status, res = send_request(
        "/user/register",
        method="POST",
        data=bad_password
    )

    if status == 422:
        print("[Успех] Минимальная длина пароля проверяется")
    else:
        print(f"[Ошибка] Ожидался 422, получен {status}: {res}")


    # 10. SQL Injection
    print("\n10. Тест защиты от SQL Injection...")

    injection_user = {
        "first_name": "'; DROP TABLE users; --",
        "second_name": "Hacker",
        "birthdate": "1990-01-01",
        "gender": "M",
        "city": "Moscow",
        "password": "123456"
    }


    status, res = send_request(
        "/user/register",
        method="POST",
        data=injection_user
    )

    if status == 201:
        print("[Успех] SQL Injection не выполняется")
    else:
        print(f"[Ошибка] Неожиданный ответ {status}: {res}")


    # 11. Неверный JWT
    print("\n11. Тест битого JWT...")

    status, res = send_request(
        f"/user/get/{user_id}",
        method="GET",
        headers={
            "Authorization": "Bearer fake_token"
        }
    )

    if status == 401:
        print("[Успех] Битый JWT отклонен")
    else:
        print(f"[Ошибка] Ожидался 401, получен {status}: {res}")


    # 12. Authorization без Bearer
    print("\n12. Тест неправильного Authorization header...")

    status, res = send_request(
        f"/user/get/{user_id}",
        method="GET",
        headers={
            "Authorization": token
        }
    )

    if status in (401,403):
        print("[Успех] Неверный Authorization отклонен")
    else:
        print(f"[Ошибка] Получен {status}: {res}")

if __name__ == "__main__":
    run_tests()
