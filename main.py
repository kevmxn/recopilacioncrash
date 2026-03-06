#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import time
import sqlite3
import json
import threading
import asyncio
import websockets
import random
import os
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ============================================
# CONFIGURACIÓN
# ============================================
API_CRASH = 'https://api-cs.casino.org/svc-evolution-game-events/api/stakecrash/latest'
API_SLIDE = 'https://api-cs.casino.org/svc-evolution-game-events/api/stakeslide/latest'

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; rv:109.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 17_1_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1'
]

DB_NAME = 'multipliers.db'

# ============================================
# BASE DE DATOS SQLITE
# ============================================
def init_db():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp_recepcion TEXT,
            api TEXT,
            event_id TEXT,
            maxMultiplier REAL,
            roundDuration INTEGER,
            startedAt TEXT
        )
    ''')
    # Crear índice para búsqueda rápida por api y timestamp
    c.execute('CREATE INDEX IF NOT EXISTS idx_api_time ON events(api, timestamp_recepcion)')
    conn.commit()
    conn.close()

def insert_event(api, event_id, maxMultiplier, roundDuration, startedAt):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('''
        INSERT INTO events (timestamp_recepcion, api, event_id, maxMultiplier, roundDuration, startedAt)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (datetime.now().isoformat(), api, event_id, maxMultiplier, roundDuration, startedAt))
    conn.commit()
    last_id = c.lastrowid
    conn.close()
    return last_id

def get_recent_events(limit=600):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('''
        SELECT timestamp_recepcion, api, event_id, maxMultiplier, roundDuration, startedAt
        FROM events
        ORDER BY timestamp_recepcion DESC
        LIMIT ?
    ''', (limit,))
    rows = c.fetchall()
    conn.close()
    # Convertir a lista de diccionarios para JSON
    events = []
    for row in rows:
        events.append({
            "timestamp_recepcion": row[0],
            "api": row[1],
            "event_id": row[2],
            "maxMultiplier": row[3],
            "roundDuration": row[4],
            "startedAt": row[5]
        })
    return events

# ============================================
# CONFIGURACIÓN DE SESIÓN CON REINTENTOS
# ============================================
def crear_sesion():
    sesion = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=['GET']
    )
    adapter = HTTPAdapter(max_retries=retry)
    sesion.mount('http://', adapter)
    sesion.mount('https://', adapter)
    return sesion

sesion = crear_sesion()

def consultar_api(url, api_nombre):
    headers = {'User-Agent': random.choice(USER_AGENTS)}
    try:
        resp = sesion.get(url, headers=headers, timeout=5)
        if resp.status_code == 200:
            return resp.json()
        else:
            print(f"⚠️ {api_nombre} - Código HTTP {resp.status_code}")
            return None
    except Exception as e:
        print(f"❌ {api_nombre} - Error: {e}")
        return None

# ============================================
# WEBSOCKET SERVER
# ============================================
connected_clients = set()
stop_websocket = threading.Event()
websocket_loop = None

async def _async_broadcast(message):
    if connected_clients:
        await asyncio.gather(
            *[client.send(message) for client in connected_clients],
            return_exceptions=True
        )

def broadcast(event_data):
    global websocket_loop
    if websocket_loop is None or not connected_clients:
        return
    message = json.dumps(event_data, default=str)
    asyncio.run_coroutine_threadsafe(_async_broadcast(message), websocket_loop)

async def websocket_handler(websocket):
    # Al conectarse, enviar historial de últimos 600 eventos
    try:
        history = get_recent_events(600)
        await websocket.send(json.dumps({"type": "history", "data": history}, default=str))
    except Exception as e:
        print(f"Error enviando historial: {e}")
    
    connected_clients.add(websocket)
    try:
        # Mantener conexión abierta, esperando mensajes (no esperamos nada del cliente)
        await websocket.wait_closed()
    finally:
        connected_clients.remove(websocket)

async def websocket_server():
    global websocket_loop
    port = int(os.environ.get('PORT', 8080))
    async with websockets.serve(websocket_handler, "0.0.0.0", port):
        print(f"✅ Servidor WebSocket escuchando en puerto {port}")
        # Mantener el servidor corriendo
        while not stop_websocket.is_set():
            await asyncio.sleep(1)

def start_websocket_server():
    global websocket_loop
    asyncio.set_event_loop(asyncio.new_event_loop())
    loop = asyncio.get_event_loop()
    websocket_loop = loop
    try:
        loop.run_until_complete(websocket_server())
    except Exception as e:
        print(f"Error en WebSocket: {e}")

# Iniciar hilo del WebSocket
websocket_thread = threading.Thread(target=start_websocket_server, daemon=True)
websocket_thread.start()

# Inicializar BD
init_db()

# ============================================
# BUCLE PRINCIPAL
# ============================================
print("🚀 Iniciando monitoreo cada 1 segundo. Presiona Ctrl+C para detener.")

crash_ids = set()
slide_ids = set()

try:
    while True:
        start_time = time.time()

        # Crash
        crash_data = consultar_api(API_CRASH, 'CRASH')
        if crash_data:
            api_id = crash_data.get('id')
            if api_id and api_id not in crash_ids:
                crash_ids.add(api_id)
                data_inner = crash_data.get('data', {})
                result = data_inner.get('result', {})
                max_mult = result.get('maxMultiplier')
                round_dur = result.get('roundDuration')
                started_at = data_inner.get('startedAt')

                # Insertar en BD
                insert_event('crash', api_id, max_mult, round_dur, started_at)
                print(f"✅ Crash nuevo: ID={api_id} maxMult={max_mult} started={started_at}")

                # Broadcast
                event = {
                    "type": "new",
                    "api": "crash",
                    "event_id": api_id,
                    "maxMultiplier": max_mult,
                    "roundDuration": round_dur,
                    "startedAt": started_at,
                    "timestamp": datetime.now().isoformat()
                }
                broadcast(event)

        # Slide
        slide_data = consultar_api(API_SLIDE, 'SLIDE')
        if slide_data:
            api_id = slide_data.get('id')
            if api_id and api_id not in slide_ids:
                slide_ids.add(api_id)
                data_inner = slide_data.get('data', {})
                result = data_inner.get('result', {})
                max_mult = result.get('maxMultiplier')
                round_dur = None
                started_at = data_inner.get('startedAt')

                insert_event('slide', api_id, max_mult, round_dur, started_at)
                print(f"✅ Slide nuevo: ID={api_id} maxMult={max_mult} started={started_at}")

                event = {
                    "type": "new",
                    "api": "slide",
                    "event_id": api_id,
                    "maxMultiplier": max_mult,
                    "roundDuration": None,
                    "startedAt": started_at,
                    "timestamp": datetime.now().isoformat()
                }
                broadcast(event)

        elapsed = time.time() - start_time
        sleep_time = max(0, 1.0 - elapsed)
        time.sleep(sleep_time)

except KeyboardInterrupt:
    print("\n⏹ Monitoreo detenido por el usuario.")
    stop_websocket.set()
    websocket_thread.join(timeout=2)

print("\n📊 Resumen:")
print(f"Crash IDs únicos: {len(crash_ids)}")
print(f"Slide IDs únicos: {len(slide_ids)}")

# Opcional: mostrar estadísticas de la BD
conn = sqlite3.connect(DB_NAME)
c = conn.cursor()
c.execute("SELECT COUNT(*) FROM events")
total = c.fetchone()[0]
print(f"Total registros en BD: {total}")
conn.close()