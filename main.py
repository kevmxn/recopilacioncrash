#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import time
import pandas as pd
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
import websocket  # websocket-client para el cliente Spaceman
import threading

# ============================================
# CONFIGURACIÓN
# ============================================
API_CRASH = 'https://api-cs.casino.org/svc-evolution-game-events/api/stakecrash/latest'
API_SLIDE = 'https://api-cs.casino.org/svc-evolution-game-events/api/stakeslide/latest'

# Configuración Spaceman (Pragmatic Play)
SPACEMAN_WS = 'wss://dga.pragmaticplaylive.net/ws'
SPACEMAN_CASINO_ID = 'ppcdk00000005349'
SPACEMAN_CURRENCY = 'BRL'
SPACEMAN_GAME_ID = 1301

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; rv:109.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 17_1_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1'
]

DB_FILE = 'eventos.db'  # Base de datos SQLite
MAX_HISTORY = 600       # Número máximo de eventos a guardar por API

# ============================================
# INICIALIZAR BASE DE DATOS
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
    # Índices para búsqueda rápida
    c.execute('CREATE INDEX IF NOT EXISTS idx_api ON eventos (api)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_timestamp ON eventos (timestamp_recepcion)')
    conn.commit()
    conn.close()

init_db()

# ============================================
# GUARDAR EVENTO EN BASE DE DATOS Y LIMITAR A 600 POR API
# ============================================
def guardar_evento(api, event_id, maxMultiplier, roundDuration, startedAt):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    timestamp = datetime.now().isoformat()
    c.execute('''INSERT INTO eventos (api, event_id, maxMultiplier, roundDuration, startedAt, timestamp_recepcion)
                 VALUES (?, ?, ?, ?, ?, ?)''',
              (api, event_id, maxMultiplier, roundDuration, startedAt, timestamp))
    conn.commit()
    
    # Eliminar eventos antiguos si se superan los 600 por API
    c.execute('''DELETE FROM eventos WHERE id IN (
                    SELECT id FROM eventos WHERE api = ? ORDER BY timestamp_recepcion DESC LIMIT -1 OFFSET ?
                )''', (api, MAX_HISTORY))
    conn.commit()
    conn.close()
    return timestamp

# ============================================
# OBTENER ÚLTIMOS EVENTOS (para historial)
# ============================================
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
# FUNCIÓN DE CONSULTA A APIs REST (Crash y Slide)
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
# WEBSOCKET SERVER (para clientes)
# ============================================
connected_clients = set()
websocket_loop = None
stop_websocket = threading.Event()

async def websocket_handler(websocket):
    """Maneja una conexión de cliente: envía historial y registra."""
    connected_clients.add(websocket)
    try:
        # Enviar historial de los últimos eventos de cada API
        for api in ['crash', 'slide', 'spaceman']:
            eventos = obtener_ultimos_eventos(api, MAX_HISTORY)
            if eventos:
                await websocket.send(json.dumps({
                    'tipo': 'historial',
                    'api': api,
                    'eventos': eventos
                }, default=str))
        # Mantener conexión
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

# Iniciar servidor WebSocket en hilo separado
threading.Thread(target=start_websocket_server, daemon=True).start()

# ============================================
# FUNCIÓN PARA ENVIAR EVENTO A TODOS LOS CLIENTES
# ============================================
async def _async_broadcast(message):
    if connected_clients:
        await asyncio.gather(
            *[client.send(message) for client in connected_clients],
            return_exceptions=True
        )

def broadcast(event_data):
    """Envía un evento a todos los clientes (thread-safe)."""
    if websocket_loop is None or not connected_clients:
        return
    message = json.dumps(event_data, default=str)
    asyncio.run_coroutine_threadsafe(_async_broadcast(message), websocket_loop)

# ============================================
# CLIENTE WEBSOCKET PARA SPACEMAN (Pragmatic Play)
# ============================================
def spaceman_client():
    """Se conecta al WebSocket de Pragmatic Play y reenvía eventos."""
    def on_message(ws, message):
        try:
            data = json.loads(message)
            # Verificar si es un resultado de juego
            if data.get('type') == 'gameResult' and 'gameResult' in data:
                for game in data['gameResult']:
                    # Extraer datos relevantes
                    event_id = str(game.get('id', int(time.time()*1000)))  # fallback a timestamp
                    maxMultiplier = float(game.get('result', 0))
                    startedAt = game.get('startTime', datetime.now().isoformat())
                    # Guardar en BD
                    timestamp = guardar_evento('spaceman', event_id, maxMultiplier, None, startedAt)
                    # Transmitir a clientes
                    broadcast({
                        'tipo': 'spaceman',
                        'id': event_id,
                        'maxMultiplier': maxMultiplier,
                        'roundDuration': None,
                        'startedAt': startedAt,
                        'timestamp_recepcion': timestamp
                    })
                    print(f"✅ Spaceman nuevo: ID={event_id} maxMult={maxMultiplier}")
        except Exception as e:
            print(f"Error procesando mensaje de Spaceman: {e}")

    def on_error(ws, error):
        print(f"❌ Spaceman WebSocket error: {error}")

    def on_close(ws, close_status_code, close_msg):
        print("🔌 Spaceman WebSocket cerrado. Reconectando en 5s...")
        time.sleep(5)
        spaceman_client()  # reconectar

    def on_open(ws):
        print("✅ Conectado a Spaceman (Pragmatic Play)")
        # Suscribirse
        subscribe_msg = {
            "type": "subscribe",
            "casinoId": SPACEMAN_CASINO_ID,
            "currency": SPACEMAN_CURRENCY,
            "key": [SPACEMAN_GAME_ID]
        }
        ws.send(json.dumps(subscribe_msg))

    # Configurar y conectar
    ws = websocket.WebSocketApp(SPACEMAN_WS,
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.run_forever()

# Iniciar cliente Spaceman en hilo separado
threading.Thread(target=spaceman_client, daemon=True).start()

# ============================================
# BUCLE PRINCIPAL (consultas a Crash y Slide)
# ============================================
print("🚀 Iniciando monitoreo de Crash y Slide cada 1 segundo. Presiona Ctrl+C para detener.")

crash_ids = set()
slide_ids = set()

try:
    while True:
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

                # Guardar en BD
                timestamp = guardar_evento('crash', api_id, max_mult, round_dur, started_at)

                # Enviar por WebSocket
                event = {
                    'tipo': 'crash',
                    'id': api_id,
                    'maxMultiplier': max_mult,
                    'roundDuration': round_dur,
                    'startedAt': started_at,
                    'timestamp_recepcion': timestamp
                }
                broadcast(event)
                print(f"✅ Crash nuevo: ID={api_id} maxMult={max_mult}")

        # --- Consultar Slide ---
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

                timestamp = guardar_evento('slide', api_id, max_mult, round_dur, started_at)

                event = {
                    'tipo': 'slide',
                    'id': api_id,
                    'maxMultiplier': max_mult,
                    'roundDuration': None,
                    'startedAt': started_at,
                    'timestamp_recepcion': timestamp
                }
                broadcast(event)
                print(f"✅ Slide nuevo: ID={api_id} maxMult={max_mult}")

        elapsed = time.time() - start_time
        sleep_time = max(0, 1.0 - elapsed)
        time.sleep(sleep_time)

except KeyboardInterrupt:
    print("\n⏹ Monitoreo detenido por el usuario.")
    stop_websocket.set()
