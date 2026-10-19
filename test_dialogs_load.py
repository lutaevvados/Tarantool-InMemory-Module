"""
Скрипт нагрузочного тестирования и сравнения производительности подсистемы диалогов.

Сравнивает 2 режима работы:
  1. ДО:   Citus PostgreSQL (дисковая/распределенная SQL СУБД)
  2. ПОСЛЕ: Tarantool In-Memory СУБД с Lua UDF (хранимыми процедурами)

Метрики:
  - RPS (Запросов в секунду)
  - Среднее время отклика (Mean Latency)
  - P95 / P99 перцентили времени отклика
  - Процент ошибок (Error Rate)
"""

import argparse
import json
import os
import sys
import time
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Tuple

BASE_URL = os.getenv("BASE_URL", "http://localhost:8000")


def http_request(path: str, method: str = "GET", data: dict = None, token: str = None, params: str = None) -> Tuple[int, dict, float]:
    url = f"{BASE_URL}{path}"
    if params:
        url += f"?{params}"

    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    body_bytes = json.dumps(data).encode("utf-8") if data else None

    req = urllib.request.Request(url, data=body_bytes, headers=headers, method=method)
    start_time = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=10.0) as resp:
            latency = (time.perf_counter() - start_time) * 1000.0  # ms
            res_body = resp.read().decode("utf-8")
            return resp.status, json.loads(res_body) if res_body else {}, latency
    except urllib.error.HTTPError as e:
        latency = (time.perf_counter() - start_time) * 1000.0
        res_body = e.read().decode("utf-8")
        try:
            parsed = json.loads(res_body)
        except Exception:
            parsed = {}
        return e.code, parsed, latency
    except Exception as e:
        latency = (time.perf_counter() - start_time) * 1000.0
        return 500, {"error": str(e)}, latency


def setup_users() -> Tuple[str, str, str, str]:
    """Создает двух пользователей для тестирования диалога и возвращает (user_a_id, token_a, user_b_id, token_b)."""
    print("-> Создание тестовых пользователей...")
    ts = int(time.time() * 1000)
    user_a_req = {
        "first_name": "PerfUserA",
        "second_name": f"Test_{ts}",
        "birthdate": "1995-05-05",
        "gender": "male",
        "city": "Moscow",
        "password": "pass_perf_user_123"
    }
    user_b_req = {
        "first_name": "PerfUserB",
        "second_name": f"Test_{ts}",
        "birthdate": "1996-06-06",
        "gender": "female",
        "city": "Moscow",
        "password": "pass_perf_user_123"
    }

    s_a, r_a, _ = http_request("/user/register", "POST", user_a_req)
    s_b, r_b, _ = http_request("/user/register", "POST", user_b_req)

    if s_a != 201 or s_b != 201:
        print(f"Ошибка создания пользователей: {r_a}, {r_b}")
        sys.exit(1)

    user_a_id = r_a["user_id"]
    user_b_id = r_b["user_id"]

    # Login
    _, login_a, _ = http_request("/login", "POST", {"id": user_a_id, "password": "pass_perf_user_123"})
    _, login_b, _ = http_request("/login", "POST", {"id": user_b_id, "password": "pass_perf_user_123"})

    token_a = login_a["token"]
    token_b = login_b["token"]

    print(f"✓ Пользователь A: {user_a_id}")
    print(f"✓ Пользователь B: {user_b_id}")
    return user_a_id, token_a, user_b_id, token_b


def set_backend(token: str, backend_name: str):
    """Устанавливает активный бэкенд на сервере."""
    status, body, _ = http_request("/admin/dialogs/backend/set", "POST", token=token, params=f"backend={backend_name}")
    if status == 200:
        print(f"✓ Переключен бэкенд на сервере: {backend_name.upper()}")
    else:
        print(f"Ошибка переключения бэкенда: {status} {body}")


def run_benchmark(
    backend_name: str,
    token_a: str,
    user_b_id: str,
    threads: int = 10,
    duration_sec: float = 10.0,
) -> Dict:
    print(f"\n==================================================")
    print(f"   ЗАПУСК НАГРУЗОЧНОГО ТЕСТА: БЭКЕНД [{backend_name.upper()}]")
    print(f"   Потоков: {threads}, Длительность: {duration_sec} сек")
    print(f"==================================================")

    set_backend(token_a, backend_name)

    # Прогрев (warmup)
    for i in range(5):
        http_request(f"/dialog/{user_b_id}/send", "POST", {"text": f"warmup_{i}"}, token=token_a)

    latencies: List[float] = []
    success_count = 0
    error_count = 0
    stop_flag = False

    def worker_loop():
        nonlocal success_count, error_count, stop_flag
        idx = 0
        while not stop_flag:
            idx += 1
            # 70% WRITE (/send), 30% READ (/list)
            if idx % 10 < 7:
                st, resp, lat = http_request(
                    f"/dialog/{user_b_id}/send",
                    "POST",
                    {"text": f"Perf load test msg #{idx}"},
                    token=token_a
                )
            else:
                st, resp, lat = http_request(
                    f"/dialog/{user_b_id}/list",
                    "GET",
                    token=token_a
                )

            if st == 200 or st == 201:
                success_count += 1
                latencies.append(lat)
            else:
                error_count += 1

    start_t = time.perf_counter()
    with ThreadPoolExecutor(max_workers=threads) as executor:
        futures = [executor.submit(worker_loop) for _ in range(threads)]
        time.sleep(duration_sec)
        stop_flag = True

    elapsed_t = time.perf_counter() - start_t

    latencies.sort()
    total_reqs = success_count + error_count
    rps = total_reqs / elapsed_t if elapsed_t > 0 else 0
    avg_lat = sum(latencies) / len(latencies) if latencies else 0
    p50 = latencies[int(len(latencies) * 0.50)] if latencies else 0
    p95 = latencies[int(len(latencies) * 0.95)] if latencies else 0
    p99 = latencies[int(len(latencies) * 0.99)] if latencies else 0
    err_rate = (error_count / total_reqs * 100.0) if total_reqs > 0 else 0

    res = {
        "backend": backend_name.upper(),
        "total_requests": total_reqs,
        "success_requests": success_count,
        "error_requests": error_count,
        "error_rate_pct": round(err_rate, 2),
        "duration_sec": round(elapsed_t, 2),
        "rps": round(rps, 2),
        "avg_latency_ms": round(avg_lat, 2),
        "p50_latency_ms": round(p50, 2),
        "p95_latency_ms": round(p95, 2),
        "p99_latency_ms": round(p99, 2),
    }

    print(f"Результаты {backend_name.upper()}:")
    print(f"  - Всего запросов: {total_reqs} за {elapsed_t:.2f}s")
    print(f"  - RPS: {rps:.2f} req/sec")
    print(f"  - Средняя задержка: {avg_lat:.2f} ms")
    print(f"  - P95: {p95:.2f} ms | P99: {p99:.2f} ms")
    print(f"  - Ошибки: {error_count} ({err_rate:.2f}%)")

    return res


def print_comparison(citus_res: Dict, tarantool_res: Dict):
    print("\n" + "="*70)
    print("          СРАВНИТЕЛЬНЫЙ ОТЧЕТ НАГРУЗОЧНОГО ТЕСТИРОВАНИЯ")
    print("="*70)
    print(f"{'Метрика':<25} | {'CITUS (SQL DB)':<20} | {'TARANTOOL (In-Memory UDF)':<25}")
    print("-" * 75)
    print(f"{'RPS (req/sec)':<25} | {citus_res['rps']:<20} | {tarantool_res['rps']:<25}")
    print(f"{'Mean Latency (ms)':<25} | {citus_res['avg_latency_ms']:<20} | {tarantool_res['avg_latency_ms']:<25}")
    print(f"{'P95 Latency (ms)':<25} | {citus_res['p95_latency_ms']:<20} | {tarantool_res['p95_latency_ms']:<25}")
    print(f"{'P99 Latency (ms)':<25} | {citus_res['p99_latency_ms']:<20} | {tarantool_res['p99_latency_ms']:<25}")
    print(f"{'Error Rate (%)':<25} | {citus_res['error_rate_pct']:<20} | {tarantool_res['error_rate_pct']:<25}")
    print("="*70)

    if citus_res['rps'] > 0:
        rps_boost = (tarantool_res['rps'] - citus_res['rps']) / citus_res['rps'] * 100.0
        lat_reduction = (citus_res['avg_latency_ms'] - tarantool_res['avg_latency_ms']) / citus_res['avg_latency_ms'] * 100.0
        print(f"★ ПРИРОСТ ПРОИЗВОДИТЕЛЬНОСТИ TARANTOOL:")
        print(f"   • Прирост пропускной способности (RPS): +{rps_boost:.1f}%")
        print(f"   • Снижение времени отклика (Latency): -{lat_reduction:.1f}%")
    print("="*70 + "\n")


def main():
    parser = argparse.ArgumentParser(description="Load test script for Dialogs module comparison.")
    parser.add_argument("--threads", type=int, default=15, help="Number of concurrent threads")
    parser.add_argument("--duration", type=float, default=10.0, help="Benchmark duration per backend (seconds)")
    args = parser.parse_args()

    user_a_id, token_a, user_b_id, token_b = setup_users()

    # 1. Замер ДО (Citus)
    citus_res = run_benchmark("citus", token_a, user_b_id, threads=args.threads, duration_sec=args.duration)

    time.sleep(2.0)

    # 2. Замер ПОСЛЕ (Tarantool UDF)
    tarantool_res = run_benchmark("tarantool", token_a, user_b_id, threads=args.threads, duration_sec=args.duration)

    # 3. Итоговое сравнение
    print_comparison(citus_res, tarantool_res)


if __name__ == "__main__":
    main()
