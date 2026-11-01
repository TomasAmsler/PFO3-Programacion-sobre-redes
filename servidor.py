"""
PFO 3: Rediseño como Sistema Distribuido (Cliente-Servidor)
Servidor de tareas

Programación sobre Redes - 1C 2026

Recibe tareas de los clientes por socket TCP y las distribuye entre los
workers. Parte del servidor de la PFO 2: mantiene registro, login y la misma
tabla de usuarios con contraseñas hasheadas, pero ahora sobre sockets y con el
trabajo repartido entre varios servidores.

Funciones principales:
    - Atiende a cada cliente en su propio hilo (varios clientes a la vez).
    - Autentica al usuario (registro / login).
    - Reparte las tareas entre los workers en round-robin.
    - Si un worker no responde, reintenta con el siguiente (tolerancia a fallas).
    - Guarda cada tarea y su resultado en la base de datos.

Uso:
    python servidor.py
"""

import socket
import sqlite3
import threading

from comun import (HOST, PUERTO_SERVIDOR, TIMEOUT_CLIENTE, TIMEOUT_CONEXION,
                   TIMEOUT_WORKER, WORKERS, configurar_consola, crear_usuario,
                   enviar_json, guardar_tarea, inicializar_db, obtener_historial,
                   recibir_json, verificar_usuario)

# ---------------------------------------------------------------------------
# Balanceo de carga entre workers (round-robin)
# ---------------------------------------------------------------------------
_turno = 0                           # Contador de pedidos repartidos
_candado_turno = threading.Lock()    # Evita que dos hilos tomen el mismo turno


def ordenar_workers():
    """
    Devuelve la lista de workers empezando por el que le toca a este pedido.
    Así los pedidos se reparten en rotación y, si el primero falla, el resto
    queda como respaldo.
    """
    global _turno
    with _candado_turno:
        inicio = _turno % len(WORKERS)
        _turno += 1
    return WORKERS[inicio:] + WORKERS[:inicio]


def enviar_a_worker(worker, tarea):
    """Abre una conexión con un worker, le envía la tarea y espera el resultado."""
    host, puerto = worker
    with socket.create_connection((host, puerto), timeout=TIMEOUT_CONEXION) as conexion:
        conexion.settimeout(TIMEOUT_WORKER)
        with conexion.makefile("rw", encoding="utf-8", newline="\n") as archivo:
            enviar_json(archivo, tarea)
            respuesta = recibir_json(archivo)

    if respuesta is None:
        raise ConnectionError("el worker cerró la conexión sin responder")
    return respuesta


def distribuir_tarea(tarea):
    """
    Envía la tarea a un worker. Prueba en orden round-robin y salta a los que
    estén caídos. Si ninguno responde, informa el error.
    """
    for worker in ordenar_workers():
        try:
            return enviar_a_worker(worker, tarea)
        except (OSError, ValueError) as error:
            # OSError incluye conexión rechazada y timeouts; ValueError, JSON roto
            print(f"[SERVIDOR] Worker {worker[1]} no disponible ({error}). Probando otro...")

    return {"ok": False, "error": "No hay workers disponibles en este momento"}


# ---------------------------------------------------------------------------
# Acciones que puede pedir un cliente
# ---------------------------------------------------------------------------
def leer_credenciales(pedido):
    """Extrae usuario y contraseña del pedido, o (None, None) si son inválidos."""
    usuario = pedido.get("usuario")
    contrasena = pedido.get("contraseña")
    if not isinstance(usuario, str) or not isinstance(contrasena, str):
        return None, None
    if usuario.strip() == "" or contrasena == "":
        return None, None
    return usuario.strip(), contrasena


def accion_registro(pedido):
    """Registra un usuario nuevo (contraseña hasheada)."""
    usuario, contrasena = leer_credenciales(pedido)
    if usuario is None:
        return {"ok": False, "error": "Se requiere 'usuario' y 'contraseña'"}

    try:
        if not crear_usuario(usuario, contrasena):
            return {"ok": False, "error": "El usuario ya existe"}
    except sqlite3.Error as error:
        print(f"[DB] Error al registrar: {error}")
        return {"ok": False, "error": "Error interno de la base de datos"}

    return {"ok": True, "mensaje": f"Usuario '{usuario}' registrado correctamente"}


def accion_login(pedido):
    """Verifica las credenciales. Retorna (respuesta, usuario o None)."""
    usuario, contrasena = leer_credenciales(pedido)
    if usuario is None:
        return {"ok": False, "error": "Se requiere 'usuario' y 'contraseña'"}, None

    try:
        valido = verificar_usuario(usuario, contrasena)
    except sqlite3.Error as error:
        print(f"[DB] Error al verificar usuario: {error}")
        return {"ok": False, "error": "Error interno de la base de datos"}, None

    if not valido:
        # Mismo mensaje si el usuario no existe o la clave es incorrecta
        return {"ok": False, "error": "Usuario o contraseña incorrectos"}, None

    return {"ok": True, "mensaje": f"Bienvenido, {usuario}. Login exitoso"}, usuario


def accion_tarea(pedido, usuario):
    """Distribuye una tarea a los workers y guarda el resultado en la base."""
    if usuario is None:
        return {"ok": False, "error": "Debes iniciar sesión primero"}

    tipo = pedido.get("tipo")
    datos = pedido.get("datos")
    if not isinstance(tipo, str):
        return {"ok": False, "error": "Falta el campo 'tipo' de la tarea"}

    respuesta = distribuir_tarea({"tipo": tipo, "datos": datos})

    # Persistimos la tarea, tanto si salió bien como si falló
    try:
        respuesta["id_tarea"] = guardar_tarea(
            usuario, tipo, datos,
            respuesta.get("resultado"),
            respuesta.get("worker"),
            "completada" if respuesta.get("ok") else "error",
        )
    except sqlite3.Error as error:
        print(f"[DB] No se pudo guardar la tarea: {error}")

    return respuesta


def accion_historial(usuario):
    """Devuelve las últimas tareas del usuario."""
    if usuario is None:
        return {"ok": False, "error": "Debes iniciar sesión primero"}
    try:
        return {"ok": True, "historial": obtener_historial(usuario)}
    except sqlite3.Error as error:
        print(f"[DB] Error al leer el historial: {error}")
        return {"ok": False, "error": "Error interno de la base de datos"}


# ---------------------------------------------------------------------------
# Atención de clientes
# ---------------------------------------------------------------------------
def atender_cliente(conexion, direccion):
    """
    Corre en un hilo propio por cada cliente conectado. Lee pedidos JSON hasta
    que el cliente se va, manteniendo en la variable 'usuario' quién inició sesión.
    """
    ip = direccion[0]
    usuario = None
    print(f"[CONEXION] Cliente conectado desde {ip}:{direccion[1]}")

    try:
        conexion.settimeout(TIMEOUT_CLIENTE)
        with conexion, conexion.makefile("rw", encoding="utf-8", newline="\n") as archivo:
            while True:
                try:
                    pedido = recibir_json(archivo)
                except ValueError:
                    enviar_json(archivo, {"ok": False, "error": "JSON inválido"})
                    continue

                if pedido is None:
                    break
                if not isinstance(pedido, dict):
                    enviar_json(archivo, {"ok": False, "error": "El pedido debe ser un objeto JSON"})
                    continue

                accion = pedido.get("accion")

                if accion == "registro":
                    respuesta = accion_registro(pedido)
                elif accion == "login":
                    respuesta, usuario_logueado = accion_login(pedido)
                    if usuario_logueado is not None:
                        usuario = usuario_logueado
                elif accion == "tarea":
                    print(f"[TAREA] {usuario or ip} pide {pedido.get('tipo')!r}")
                    respuesta = accion_tarea(pedido, usuario)
                elif accion == "historial":
                    respuesta = accion_historial(usuario)
                elif accion == "salir":
                    enviar_json(archivo, {"ok": True, "mensaje": "Hasta luego"})
                    break
                else:
                    respuesta = {"ok": False, "error": f"Acción desconocida: {accion!r}"}

                enviar_json(archivo, respuesta)

    except (OSError, ValueError) as error:
        print(f"[CONEXION] Conexión con {ip} interrumpida: {error}")

    print(f"[CONEXION] Cliente {ip}:{direccion[1]} desconectado")


# ---------------------------------------------------------------------------
# Socket del servidor
# ---------------------------------------------------------------------------
def inicializar_socket():
    """Crea el socket TCP del servidor y lo deja escuchando. Maneja puerto ocupado."""
    try:
        # Configuración del socket TCP/IP (IPv4, orientado a conexión)
        servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        servidor.bind((HOST, PUERTO_SERVIDOR))
        servidor.listen(100)

        # Timeout corto en accept() para poder detectar Ctrl+C también en Windows
        servidor.settimeout(1.0)
        print(f"[SOCKET] Servidor escuchando en {HOST}:{PUERTO_SERVIDOR}")
        return servidor
    except OSError as error:
        print(f"[SOCKET] No se pudo abrir el puerto {PUERTO_SERVIDOR}: {error}")
        raise SystemExit(1)


def aceptar_conexiones(servidor):
    """Acepta clientes y lanza un hilo para atender a cada uno."""
    print("[SERVIDOR] Esperando clientes... (Ctrl+C para detener)")
    while True:
        try:
            conexion, direccion = servidor.accept()
        except socket.timeout:
            continue
        except KeyboardInterrupt:
            print("\n[SERVIDOR] Detenido por el usuario.")
            break

        hilo = threading.Thread(target=atender_cliente, args=(conexion, direccion), daemon=True)
        hilo.start()


def main():
    """Punto de entrada del servidor."""
    configurar_consola()

    try:
        inicializar_db()
        print("[DB] Base de datos lista.")
    except sqlite3.Error as error:
        print(f"[DB] No se pudo acceder a la base de datos: {error}")
        raise SystemExit(1)

    print(f"[SERVIDOR] Workers configurados: {[puerto for _, puerto in WORKERS]}")
    servidor = inicializar_socket()
    try:
        aceptar_conexiones(servidor)
    finally:
        servidor.close()


if __name__ == "__main__":
    main()
