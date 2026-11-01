"""
PFO 3: Rediseño como Sistema Distribuido (Cliente-Servidor)
Worker

Programación sobre Redes - 1C 2026

Un worker es un servidor TCP que recibe tareas del servidor principal, las
ejecuta y devuelve el resultado. Cada worker tiene su propio POOL DE HILOS:
el hilo principal solo acepta conexiones y delega cada tarea a un hilo libre
del pool, de modo que varias tareas se procesan en paralelo.

Uso:
    python worker.py <puerto> [cantidad_de_hilos]
    python worker.py 6001
"""

import math
import socket
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from comun import (HILOS_POR_WORKER, HOST, TIMEOUT_WORKER, configurar_consola,
                   enviar_json, recibir_json)


# ---------------------------------------------------------------------------
# Tareas que sabe ejecutar el worker
# ---------------------------------------------------------------------------
def validar_entero(valor, minimo, maximo, nombre):
    """Verifica que 'valor' sea un entero dentro del rango permitido."""
    if isinstance(valor, bool) or not isinstance(valor, int):
        raise ValueError(f"'{nombre}' debe ser un número entero")
    if not minimo <= valor <= maximo:
        raise ValueError(f"'{nombre}' debe estar entre {minimo} y {maximo}")
    return valor


def validar_texto(valor):
    """Verifica que 'valor' sea un texto de hasta 1000 caracteres."""
    if not isinstance(valor, str) or valor == "":
        raise ValueError("Se requiere un texto no vacío")
    if len(valor) > 1000:
        raise ValueError("El texto no puede superar los 1000 caracteres")
    return valor


def tarea_factorial(datos):
    """Factorial de n (0 a 1000)."""
    n = validar_entero(datos, 0, 1000, "n")
    return str(math.factorial(n))


def tarea_fibonacci(datos):
    """Número de Fibonacci en la posición n (0 a 5000), calculado de forma iterativa."""
    n = validar_entero(datos, 0, 5000, "n")
    a, b = 0, 1
    for _ in range(n):
        a, b = b, a + b
    return str(a)


def tarea_primos(datos):
    """Cuenta cuántos números primos hay hasta n (2 a 5.000.000) con criba."""
    n = validar_entero(datos, 2, 5_000_000, "n")
    criba = bytearray([1]) * (n + 1)
    criba[0] = criba[1] = 0
    for i in range(2, int(n ** 0.5) + 1):
        if criba[i]:
            criba[i * i::i] = bytearray(len(criba[i * i::i]))
    return f"Hay {sum(criba)} números primos hasta {n}"


def tarea_invertir(datos):
    """Devuelve el texto al revés."""
    return validar_texto(datos)[::-1]


def tarea_mayusculas(datos):
    """Devuelve el texto en mayúsculas."""
    return validar_texto(datos).upper()


def tarea_dormir(datos):
    """Simula un trabajo largo esperando n segundos (0 a 10)."""
    if isinstance(datos, bool) or not isinstance(datos, (int, float)):
        raise ValueError("'segundos' debe ser un número")
    if not 0 <= datos <= 10:
        raise ValueError("'segundos' debe estar entre 0 y 10")
    time.sleep(datos)
    return f"Trabajo simulado de {datos} s completado"


# Tabla de despacho: nombre de la tarea -> función que la resuelve
TAREAS = {
    "factorial": tarea_factorial,
    "fibonacci": tarea_fibonacci,
    "primos": tarea_primos,
    "invertir": tarea_invertir,
    "mayusculas": tarea_mayusculas,
    "dormir": tarea_dormir,
}


# ---------------------------------------------------------------------------
# Atención de pedidos
# ---------------------------------------------------------------------------
def procesar_pedido(pedido, nombre_worker):
    """
    Ejecuta la tarea pedida y arma la respuesta. Nunca lanza excepciones:
    cualquier error se informa dentro de la respuesta.
    """
    inicio = time.perf_counter()
    hilo = threading.current_thread().name

    try:
        if not isinstance(pedido, dict):
            raise ValueError("El pedido debe ser un objeto JSON")

        tipo = pedido.get("tipo")
        funcion = TAREAS.get(tipo)
        if funcion is None:
            raise ValueError(f"Tipo de tarea desconocido: {tipo!r}")

        resultado = funcion(pedido.get("datos"))
        respuesta = {"ok": True, "resultado": resultado}
    except ValueError as error:
        respuesta = {"ok": False, "error": str(error)}

    respuesta["worker"] = nombre_worker
    respuesta["hilo"] = hilo
    respuesta["tiempo"] = round(time.perf_counter() - inicio, 3)
    return respuesta


def atender_conexion(conexion, direccion, nombre_worker):
    """
    Corre en un hilo del pool: lee un pedido, lo procesa y responde.
    Cada conexión transporta una sola tarea.
    """
    try:
        conexion.settimeout(TIMEOUT_WORKER)
        with conexion, conexion.makefile("rw", encoding="utf-8", newline="\n") as archivo:
            try:
                pedido = recibir_json(archivo)
            except ValueError:
                enviar_json(archivo, {"ok": False, "error": "JSON inválido",
                                      "worker": nombre_worker})
                return

            if pedido is None:
                return

            hilo = threading.current_thread().name
            print(f"[{hilo}] Procesando {pedido.get('tipo')!r} de {direccion[0]}")
            respuesta = procesar_pedido(pedido, nombre_worker)
            enviar_json(archivo, respuesta)
            print(f"[{hilo}] Listo en {respuesta['tiempo']} s")
    except (OSError, ValueError) as error:
        print(f"[{nombre_worker}] Error atendiendo a {direccion[0]}: {error}")


# ---------------------------------------------------------------------------
# Socket del worker
# ---------------------------------------------------------------------------
def inicializar_socket(puerto):
    """Crea el socket TCP del worker y lo deja escuchando en el puerto."""
    try:
        # Configuración del socket TCP/IP (IPv4, orientado a conexión)
        servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        servidor.bind((HOST, puerto))
        servidor.listen(50)

        # Timeout corto en accept() para poder detectar Ctrl+C también en Windows
        servidor.settimeout(1.0)
        return servidor
    except OSError as error:
        print(f"[WORKER] No se pudo abrir el puerto {puerto}: {error}")
        raise SystemExit(1)


def ejecutar_worker(puerto, hilos):
    """Bucle principal: acepta conexiones y las delega al pool de hilos."""
    nombre_worker = f"worker-{puerto}"
    servidor = inicializar_socket(puerto)
    print(f"[{nombre_worker}] Escuchando en {HOST}:{puerto} con {hilos} hilos")

    # Pool de hilos: 'hilos' tareas pueden ejecutarse al mismo tiempo.
    # Si llegan más, esperan en la cola interna del pool.
    pool = ThreadPoolExecutor(max_workers=hilos, thread_name_prefix=f"w{puerto}-hilo")

    try:
        while True:
            try:
                conexion, direccion = servidor.accept()
            except socket.timeout:
                continue
            pool.submit(atender_conexion, conexion, direccion, nombre_worker)
    except KeyboardInterrupt:
        print(f"\n[{nombre_worker}] Detenido por el usuario.")
    finally:
        pool.shutdown(wait=False, cancel_futures=True)
        servidor.close()


def leer_argumentos():
    """Lee puerto y cantidad de hilos desde la línea de comandos."""
    if len(sys.argv) < 2:
        print("Uso: python worker.py <puerto> [cantidad_de_hilos]")
        raise SystemExit(1)
    try:
        puerto = int(sys.argv[1])
        hilos = int(sys.argv[2]) if len(sys.argv) > 2 else HILOS_POR_WORKER
    except ValueError:
        print("El puerto y la cantidad de hilos deben ser números enteros.")
        raise SystemExit(1)
    return puerto, max(1, hilos)


def main():
    """Punto de entrada del worker."""
    configurar_consola()
    puerto, hilos = leer_argumentos()
    ejecutar_worker(puerto, hilos)


if __name__ == "__main__":
    main()
