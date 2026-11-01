"""
PFO 3: Rediseño como Sistema Distribuido (Cliente-Servidor)
Módulo común

Programación sobre Redes - 1C 2026

Reúne lo que comparten el servidor, los workers y el cliente:
    - Configuración (direcciones, puertos, tiempos de espera).
    - Protocolo de mensajes: un objeto JSON por línea sobre TCP.
    - Acceso a la base de datos SQLite, reutilizando de la PFO 2 la tabla
      'usuarios' y el hasheo de contraseñas con werkzeug.
"""

import json
import sqlite3
import sys
from datetime import datetime

from werkzeug.security import check_password_hash, generate_password_hash

# ---------------------------------------------------------------------------
# Configuración general
# ---------------------------------------------------------------------------
HOST = "127.0.0.1"                 # Dirección local para todos los componentes
PUERTO_SERVIDOR = 6000             # Puerto donde escucha el servidor de tareas

# Workers disponibles (host, puerto). Para agregar uno nuevo basta sumarlo aquí
# y levantar un worker en ese puerto.
WORKERS = [(HOST, 6001), (HOST, 6002), (HOST, 6003)]

HILOS_POR_WORKER = 4               # Tamaño del pool de hilos de cada worker
TIMEOUT_CONEXION = 2               # Segundos para conectar con un worker
TIMEOUT_WORKER = 30                # Segundos máximos esperando un resultado
TIMEOUT_CLIENTE = 300              # Segundos de inactividad antes de cortar
MAX_LINEA = 65536                  # Tamaño máximo de un mensaje (64 KB)

DB_NAME = "tareas.db"              # Misma base que la PFO 2 (tabla 'usuarios')


# ---------------------------------------------------------------------------
# Consola
# ---------------------------------------------------------------------------
def configurar_consola():
    """
    Fuerza UTF-8 en la entrada y salida estándar para que las tildes y la ñ
    se vean bien en Git Bash. Si no se puede, el programa sigue igual.
    """
    for flujo in (sys.stdin, sys.stdout, sys.stderr):
        try:
            flujo.reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, ValueError):
            pass


# ---------------------------------------------------------------------------
# Protocolo: un JSON por línea
# ---------------------------------------------------------------------------
def enviar_json(archivo, objeto):
    """Serializa 'objeto' como JSON y lo envía terminado en salto de línea."""
    archivo.write(json.dumps(objeto, ensure_ascii=False) + "\n")
    archivo.flush()


def recibir_json(archivo):
    """
    Lee una línea y la convierte de JSON a objeto Python.
    Retorna None si el otro extremo cerró la conexión.
    Lanza ValueError si la línea no es un JSON válido.
    """
    linea = archivo.readline(MAX_LINEA)
    if not linea:
        return None
    return json.loads(linea)


# ---------------------------------------------------------------------------
# Base de datos (SQLite)
# ---------------------------------------------------------------------------
def obtener_conexion():
    """Abre una conexión a SQLite (timeout alto por si hay varios hilos)."""
    return sqlite3.connect(DB_NAME, timeout=10)


def inicializar_db():
    """
    Crea las tablas si no existen:
        usuarios -> igual que en la PFO 2 (id, usuario, password_hash)
        tareas   -> historial de las tareas procesadas por el sistema
    """
    conexion = obtener_conexion()
    try:
        conexion.execute("""
            CREATE TABLE IF NOT EXISTS usuarios (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                usuario TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL
            )
        """)
        conexion.execute("""
            CREATE TABLE IF NOT EXISTS tareas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                usuario TEXT NOT NULL,
                tipo TEXT NOT NULL,
                datos TEXT NOT NULL,
                resultado TEXT,
                worker TEXT,
                estado TEXT NOT NULL,
                fecha TEXT NOT NULL
            )
        """)
        conexion.commit()
    finally:
        conexion.close()


def crear_usuario(usuario, contrasena):
    """
    Registra un usuario guardando solo el hash de la contraseña.
    Retorna True si se creó, False si el usuario ya existía.
    """
    conexion = obtener_conexion()
    try:
        conexion.execute(
            "INSERT INTO usuarios (usuario, password_hash) VALUES (?, ?)",
            (usuario, generate_password_hash(contrasena)),
        )
        conexion.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conexion.close()


def verificar_usuario(usuario, contrasena):
    """Retorna True si el usuario existe y la contraseña coincide con el hash."""
    conexion = obtener_conexion()
    try:
        fila = conexion.execute(
            "SELECT password_hash FROM usuarios WHERE usuario = ?", (usuario,)
        ).fetchone()
    finally:
        conexion.close()

    return fila is not None and check_password_hash(fila[0], contrasena)


def guardar_tarea(usuario, tipo, datos, resultado, worker, estado):
    """Guarda una tarea en el historial y retorna su id."""
    conexion = obtener_conexion()
    try:
        cursor = conexion.execute(
            "INSERT INTO tareas (usuario, tipo, datos, resultado, worker, estado, fecha) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (usuario, tipo, json.dumps(datos, ensure_ascii=False), resultado,
             worker, estado, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        )
        conexion.commit()
        return cursor.lastrowid
    finally:
        conexion.close()


def obtener_historial(usuario, limite=10):
    """Devuelve las últimas tareas de un usuario (la más reciente primero)."""
    conexion = obtener_conexion()
    try:
        filas = conexion.execute(
            "SELECT id, tipo, datos, estado, worker, fecha FROM tareas "
            "WHERE usuario = ? ORDER BY id DESC LIMIT ?",
            (usuario, limite),
        ).fetchall()
    finally:
        conexion.close()

    return [
        {"id": f[0], "tipo": f[1], "datos": f[2], "estado": f[3],
         "worker": f[4], "fecha": f[5]}
        for f in filas
    ]
