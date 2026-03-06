#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import time
import sqlite3
import threading
import asyncio
import websockets
import json
import os
import random
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ============================================
# CONFIGURACIÓN
# ============================================
API_CRASH = 'https://api-cs.casino.org/svc-evolution-game-events/api/stakecrash/latest'
API_SLIDE = 'https://api-cs.casino.org/svc-evolution-game-events/api/stakeslide/latest'

# 30+ user‑agents rotativos
USER_AGENTS = [
    # Windows + Chrome
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
    # Windows + Firefox
    'Mozilla/5.0 (Windows NT 10.0; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Windows NT 10.0; rv:120.0) Gecko/20100101 Firefox/120.0',
    'Mozilla/5.0 (Windows NT 10.0; rv:119.0) Gecko/20100101 Firefox/119.0',
    # Windows + Edge
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0',
    # macOS + Safari
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Safari/605.1.15',
    # macOS + Chrome
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    # Linux
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0',
    # iOS
    'Mozilla/5.0 (iPhone; CPU iPhone OS 17_1_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (iPad; CPU OS 17_1_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1',
    # Android
    'Mozilla/5.0 (Linux; Android 14; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.6099.230 Mobile Safari/537.36',
    'Mozilla/5.0 (Linux; Android 13; SM-G998B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.6045.163 Mobile Safari/537.36',
    # Bots (algunas APIs los permiten)
    'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)',
    'Mozilla/5.0 (compatible; Bingbot/2.0; +http://www.bing.com/bingbot.htm)',
    # Otros
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:115.0) Gecko/20100101 Firefox/115.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:115.0) Gecko/20100101 Firefox/115.0',
    'Mozilla/5.0 (X11; CrOS x86_64 14541.0.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
]

DB_FILE = 'eventos.db'
MAX_HISTORY = 600          # Número máximo de eventos por API

# Configuración de backoff
BASE_SLEEP = 1.0
MAX_SLEEP = 60.0

# ============================================
# BASE DE DATOS
# ============================================
def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS eventos
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  api TEXT,
                  event_id TEXT,
                  maxMultiplier REAL,
                  roundDuration REAL,
                  startedAt TEXT,
                  timestamp_recepcion TEXT)''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_api ON eventos (api)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_timestamp ON eventos (timestamp_recepcion)')
    conn.commit()
    conn.close()

init_db()

def guardar_evento(api, event_id, maxMultiplier, roundDuration, startedAt):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    timestamp = datetime.now().isoformat()
    c.execute('''INSERT INTO eventos (api, event_id, maxMultiplier, roundDuration, startedAt, timestamp_recepcion)
                 VALUES (?, ?, ?, ?, ?, ?)''',
              (api, event_id, maxMultiplier, roundDuration, startedAt, timestamp))
    conn.commit()
    # Mantener solo los últimos MAX_HISTORY eventos por API
    c.execute('''DELETE FROM eventos WHERE id IN (
                    SELECT id FROM eventos WHERE api = ? ORDER BY timestamp_recepcion DESC LIMIT -1 OFFSET ?
                )''', (api, MAX_HISTORY))
    conn.commit()
    conn.close()
    return timestamp

def obtener_ultimos_eventos(api, limite=MAX_HISTORY):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''SELECT api, event_id, maxMultiplier, roundDuration, startedAt, timestamp_recepcion
                 FROM eventos WHERE api = ? ORDER BY timestamp_recepcion DESC LIMIT ?''', (api, limite))
    filas = c.fetchall()
    conn.close()
    eventos = []
    for fila in filas:
        eventos.append({
            'api': fila[0],
            'event_id': fila[1],
            'maxMultiplier': fila[2],
            'roundDuration': fila[3],
            'startedAt': fila[4],
            'timestamp_recepcion': fila[5]
        })
    return eventos

# ============================================
# CONFIGURACIÓN DE SESIÓN HTTP
# ============================================
def crear_sesion():
    sesion = requests.Session()
    retry = Retry(
        total=2,                         # reintentos totales (evita ciclos largos)
        backoff_factor=0.1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=['GET']
    )
    adapter = HTTPAdapter(max_retries=retry)
    sesion.mount('http://', adapter)
    sesion.mount('https://', adapter)
    return sesion

# Usaremos una sesión global, pero rotaremos user-agent por petición
sesion_global = crear_sesion()

# ============================================
# FUNCIÓN DE CONSULTA CON BACKOFF Y ROTACIÓN DE USER-AGENT
# ============================================
# Estado por API (crash y slide)
api_status = {
    'crash': {'consecutive_errors': 0, 'next_allowed_time': 0},
    'slide': {'consecutive_errors': 0, 'next_allowed_time': 0}
}

def get_random_user_agent():
    return random.choice(USER_AGENTS)

def consultar_con_backoff(url, api_nombre):
    status = api_status[api_nombre]
    now = time.time()

    # Si la API está en período de espera, dormir y retornar None
    if now < status['next_allowed_time']:
        wait = status['next_allowed_time'] - now
        print(f"⏳ {api_nombre} en espera por {wait:.1f}s (backoff)")
        time.sleep(wait)
        return None

    headers = {'User-Agent': get_random_user_agent()}
    try:
        resp = sesion_global.get(url, headers=headers, timeout=5)

        # Si la API pide esperar con Retry-After
        if 'Retry-After' in resp.headers:
            retry_after = int(resp.headers['Retry-After'])
            status['next_allowed_time'] = time.time() + retry_after
            status['consecutive_errors'] += 1
            print(f"⚠️ {api_nombre} pide esperar {retry_after}s")
            return None

        if resp.status_code == 200:
            status['consecutive_errors'] = 0
            return resp.json()
        elif resp.status_code == 429:
            retry_after = int(resp.headers.get('Retry-After', 2 ** status['consecutive_errors']))
            status['next_allowed_time'] = time.time() + retry_after
            status['consecutive_errors'] += 1
            print(f"⚠️ {api_nombre} rate limited. Esperando {retry_after}s")
            return None
        elif 500 <= resp.status_code < 600:
            status['consecutive_errors'] += 1
            backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** status['consecutive_errors']))
            status['next_allowed_time'] = time.time() + backoff
            print(f"❌ {api_nombre} error {resp.status_code}. Backoff {backoff:.1f}s")
            return None
        else:
            print(f"⚠️ {api_nombre} código no esperado: {resp.status_code}")
            return None

    except requests.exceptions.Timeout:
        status['consecutive_errors'] += 1
        backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** status['consecutive_errors']))
        status['next_allowed_time'] = time.time() + backoff
        print(f"⏰ {api_nombre} timeout. Backoff {backoff:.1f}s")
        return None
    except Exception as e:
        status['consecutive_errors'] += 1
        backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** status['consecutive_errors']))
        status['next_allowed_time'] = time.time() + backoff
        print(f"💥 {api_nombre} error: {e}. Backoff {backoff:.1f}s")
        return None

# ============================================
# SERVIDOR WEBSOCKET
# ============================================
connected_clients = set()
websocket_loop = None
stop_websocket = threading.Event()

async def websocket_handler(websocket):
    connected_clients.add(websocket)
    try:
        # Enviar historial al conectar (para crash y slide)
        for api in ['crash', 'slide']:
            eventos = obtener_ultimos_eventos(api, MAX_HISTORY)
            if eventos:
                await websocket.send(json.dumps({
                    'tipo': 'historial',
                    'api': api,
                    'eventos': eventos
                }, default=str))
        await websocket.wait_closed()
    finally:
        connected_clients.remove(websocket)

async def websocket_server():
    global websocket_loop
    port = int(os.environ.get('PORT', 8080))
    async with websockets.serve(websocket_handler, "0.0.0.0", port):
        print(f"✅ Servidor WebSocket escuchando en puerto {port}")
        websocket_loop = asyncio.get_running_loop()
        while not stop_websocket.is_set():
            await asyncio.sleep(1)

def start_websocket_server():
    asyncio.set_event_loop(asyncio.new_event_loop())
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(websocket_server())
    except Exception as e:
        print(f"Error en WebSocket server: {e}")

# Lanzar servidor WebSocket en un hilo separado
threading.Thread(target=start_websocket_server, daemon=True).start()

async def _async_broadcast(message):
    if connected_clients:
        await asyncio.gather(
            *[client.send(message) for client in connected_clients],
            return_exceptions=True
        )

def broadcast(event_data):
    if websocket_loop is None or not connected_clients:
        return
    message = json.dumps(event_data, default=str)
    asyncio.run_coroutine_threadsafe(_async_broadcast(message), websocket_loop)

# ============================================
# BUCLE PRINCIPAL (Crash y Slide)
# ============================================
print("🚀 Iniciando monitoreo robusto de Crash y Slide. Presiona Ctrl+C para detener.")

crash_ids = set()
slide_ids = set()

try:
    while True:
        now = time.time()

        # --- Crash ---
        if now >= api_status['crash']['next_allowed_time']:
            crash_data = consultar_con_backoff(API_CRASH, 'crash')
            if crash_data:
                api_id = crash_data.get('id')
                if api_id and api_id not in crash_ids:
                    crash_ids.add(api_id)
                    data_inner = crash_data.get('data', {})
                    result = data_inner.get('result', {})
                    max_mult = result.get('maxMultiplier')
                    round_dur = result.get('roundDuration')
                    started_at = data_inner.get('startedAt')

                    # Validar multiplicador
                    if max_mult is not None and max_mult > 0:
                        timestamp = guardar_evento('crash', api_id, max_mult, round_dur, started_at)
                        broadcast({
                            'tipo': 'crash',
                            'id': api_id,
                            'maxMultiplier': max_mult,
                            'roundDuration': round_dur,
                            'startedAt': started_at,
                            'timestamp_recepcion': timestamp
                        })
                        print(f"✅ Crash nuevo: ID={api_id} maxMult={max_mult}")
                    else:
                        print(f"⚠️ Crash ID {api_id} con multiplicador inválido: {max_mult}")

        # --- Slide ---
        if now >= api_status['slide']['next_allowed_time']:
            slide_data = consultar_con_backoff(API_SLIDE, 'slide')
            if slide_data:
                api_id = slide_data.get('id')
                if api_id and api_id not in slide_ids:
                    slide_ids.add(api_id)
                    data_inner = slide_data.get('data', {})
                    result = data_inner.get('result', {})
                    max_mult = result.get('maxMultiplier')
                    started_at = data_inner.get('startedAt')

                    if max_mult is not None and max_mult > 0:
                        timestamp = guardar_evento('slide', api_id, max_mult, None, started_at)
                        broadcast({
                            'tipo': 'slide',
                            'id': api_id,
                            'maxMultiplier': max_mult,
                            'roundDuration': None,
                            'startedAt': started_at,
                            'timestamp_recepcion': timestamp
                        })
                        print(f"✅ Slide nuevo: ID={api_id} maxMult={max_mult}")
                    else:
                        print(f"⚠️ Slide ID {api_id} con multiplicador inválido: {max_mult}")

        # Espera inteligente con jitter
        next_crash = api_status['crash']['next_allowed_time']
        next_slide = api_status['slide']['next_allowed_time']
        next_allowed = min(next_crash, next_slide)
        wait = max(0, next_allowed - time.time())
        if wait > 0:
            # Añadir un pequeño jitter (±10%) para evitar patrones fijos
            jitter = random.uniform(0.9, 1.1)
            time.sleep(wait * jitter)
        else:
            time.sleep(0.5)  # pausa mínima

except KeyboardInterrupt:
    print("\n⏹ Monitoreo detenido por el usuario.")
    stop_websocket.set()
    # Pequeña espera para que el servidor WebSocket termine
    time.sleep(1)
