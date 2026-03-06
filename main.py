#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
import json
import random
import sqlite3
import threading
import asyncio
import websockets
import requests
from datetime import datetime, timezone
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
MAX_RECORDS_PER_API = 600  # Mantener últimos 600 registros por API

# ============================================
# CONFIGURACIÓN DE SESIÓN HTTP CON REINTENTOS
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

# ============================================
# BASE DE DATOS SQLITE
# ============================================
def init_db():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    # Tabla para crash
    c.execute('''CREATE TABLE IF NOT EXISTS crash (
                    id TEXT PRIMARY KEY,
                    maxMultiplier REAL,
                    roundDuration REAL,
                    startedAt TEXT,
                    timestamp_utc TEXT
                )''')
    # Tabla para slide
    c.execute('''CREATE TABLE IF NOT EXISTS slide (
                    id TEXT PRIMARY KEY,
                    maxMultiplier REAL,
                    roundDuration REAL,
                    startedAt TEXT,
                    timestamp_utc TEXT
                )''')
    conn.commit()
    conn.close()

def guardar_evento(api, data):
    """Guarda un evento en la base de datos y elimina los más antiguos si excede MAX_RECORDS_PER_API."""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    table = 'crash' if api == 'crash' else 'slide'
    # Insertar o reemplazar (si ya existe el ID, se actualiza)
    c.execute(f'''
        INSERT OR REPLACE INTO {table} (id, maxMultiplier, roundDuration, startedAt, timestamp_utc)
        VALUES (?, ?, ?, ?, ?)
    ''', (data['id'], data['maxMultiplier'], data.get('roundDuration'), data['startedAt'], data['timestamp_utc']))
    # Eliminar registros excedentes, manteniendo los más recientes por timestamp_utc
    c.execute(f'''
        DELETE FROM {table}
        WHERE id NOT IN (
            SELECT id FROM {table}
            ORDER BY timestamp_utc DESC
            LIMIT {MAX_RECORDS_PER_API}
        )
    ''')
    conn.commit()
    conn.close()

def obtener_ultimos_eventos(api, limite=MAX_RECORDS_PER_API):
    """Devuelve los últimos `limite` eventos de la API especificada, ordenados por timestamp_utc ascendente (más antiguos primero)."""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    table = 'crash' if api == 'crash' else 'slide'
    c.execute(f'''
        SELECT id, maxMultiplier, roundDuration, startedAt, timestamp_utc
        FROM {table}
        ORDER BY timestamp_utc ASC
        LIMIT ?
    ''', (limite,))
    rows = c.fetchall()
    conn.close()
    eventos = []
    for row in rows:
        eventos.append({
            'api': api,
            'id': row[0],
            'maxMultiplier': row[1],
            'roundDuration': row[2],
            'startedAt': row[3],
            'timestamp_utc': row[4]
        })
    return eventos

# ============================================
# FUNCIÓN DE CONSULTA A API
# ============================================
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
# SERVIDOR WEBSOCKET
# ============================================
connected_clients = set()
websocket_loop = None
stop_websocket = threading.Event()

async def websocket_handler(websocket):
    """Maneja una conexión de cliente: envía historial y lo agrega a la lista de difusión."""
    connected_clients.add(websocket)
    try:
        # Enviar historial de crash (últimos 600)
        crash_hist = obtener_ultimos_eventos('crash', MAX_RECORDS_PER_API)
        for ev in crash_hist:
            await websocket.send(json.dumps(ev, default=str))
        # Enviar historial de slide
        slide_hist = obtener_ultimos_eventos('slide', MAX_RECORDS_PER_API)
        for ev in slide_hist:
            await websocket.send(json.dumps(ev, default=str))
        # Mantener conexión abierta
        await websocket.wait_closed()
    finally:
        connected_clients.remove(websocket)

async def _async_broadcast(message):
    """Envía mensaje a todos los clientes conectados."""
    if connected_clients:
        await asyncio.gather(
            *[client.send(message) for client in connected_clients],
            return_exceptions=True
        )

def broadcast(event_data):
    """Envía un evento a todos los clientes (thread-safe)."""
    global websocket_loop
    if websocket_loop is None or not connected_clients:
        return
    message = json.dumps(event_data, default=str)
    asyncio.run_coroutine_threadsafe(_async_broadcast(message), websocket_loop)

async def websocket_server():
    global websocket_loop
    websocket_loop = asyncio.get_running_loop()
    port = int(os.environ.get('PORT', 8080))
    async with websockets.serve(websocket_handler, "0.0.0.0", port):
        print(f"✅ Servidor WebSocket escuchando en puerto {port}")
        # Mantener el servidor corriendo
        await asyncio.Future()  # Corre indefinidamente

def start_websocket_server():
    asyncio.run(websocket_server())

# ============================================
# BUCLE PRINCIPAL DE CONSULTAS
# ============================================
def main_loop():
    """Ejecuta las consultas cada segundo y guarda/broadcast cuando hay nuevos IDs."""
    crash_ids = set()
    slide_ids = set()

    # Cargar IDs existentes de la base de datos al iniciar
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('SELECT id FROM crash')
    crash_ids.update(row[0] for row in c.fetchall())
    c.execute('SELECT id FROM slide')
    slide_ids.update(row[0] for row in c.fetchall())
    conn.close()
    print(f"📦 IDs cargados desde DB: {len(crash_ids)} crash, {len(slide_ids)} slide")

    while not stop_websocket.is_set():
        start_time = time.time()

        # --- Consultar Crash ---
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
                timestamp_utc = datetime.now(timezone.utc).isoformat()

                evento = {
                    'api': 'crash',
                    'id': api_id,
                    'maxMultiplier': max_mult,
                    'roundDuration': round_dur,
                    'startedAt': started_at,
                    'timestamp_utc': timestamp_utc
                }
                # Guardar en SQLite
                guardar_evento('crash', evento)
                print(f"✅ Crash nuevo: ID={api_id} maxMult={max_mult}")
                # Broadcast
                broadcast(evento)

        # --- Consultar Slide ---
        slide_data = consultar_api(API_SLIDE, 'SLIDE')
        if slide_data:
            api_id = slide_data.get('id')
            if api_id and api_id not in slide_ids:
                slide_ids.add(api_id)
                data_inner = slide_data.get('data', {})
                result = data_inner.get('result', {})
                max_mult = result.get('maxMultiplier')
                started_at = data_inner.get('startedAt')
                timestamp_utc = datetime.now(timezone.utc).isoformat()

                evento = {
                    'api': 'slide',
                    'id': api_id,
                    'maxMultiplier': max_mult,
                    'roundDuration': None,
                    'startedAt': started_at,
                    'timestamp_utc': timestamp_utc
                }
                guardar_evento('slide', evento)
                print(f"✅ Slide nuevo: ID={api_id} maxMult={max_mult}")
                broadcast(evento)

        elapsed = time.time() - start_time
        sleep_time = max(0, 1.0 - elapsed)
        time.sleep(sleep_time)

# ============================================
# INICIO
# ============================================
if __name__ == "__main__":
    init_db()
    # Iniciar servidor WebSocket en un hilo
    ws_thread = threading.Thread(target=start_websocket_server, daemon=True)
    ws_thread.start()
    # Dar tiempo a que el WebSocket arranque
    time.sleep(1)
    try:
        main_loop()
    except KeyboardInterrupt:
        print("\n⏹ Deteniendo...")
        stop_websocket.set()
        ws_thread.join(timeout=2)
        print("Servidor detenido.")
