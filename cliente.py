"""
PFO 3: Rediseño como Sistema Distribuido (Cliente-Servidor)
Cliente de consola

Programación sobre Redes - 1C 2026

Se conecta al servidor por socket TCP, permite registrarse, iniciar sesión,
enviar tareas y recibir los resultados. Incluye un lote de prueba que lanza
varias tareas a la vez para ver cómo se reparten entre los workers.
"""

import socket
import threading
import time

from comun import (HOST, PUERTO_SERVIDOR, TIMEOUT_CLIENTE, configurar_consola,
                   enviar_json, recibir_json)

# Cantidad máxima de caracteres de un resultado que se muestran en pantalla
MAX_MOSTRAR = 200

# Menú de tareas: opción -> (tipo, pregunta al usuario, tipo de dato)
MENU_TAREAS = {
    "1": ("factorial", "Número n (0 a 1000): ", int),
    "2": ("fibonacci", "Posición n (0 a 5000): ", int),
    "3": ("primos", "Contar primos hasta n (2 a 5000000): ", int),
    "4": ("invertir", "Texto a invertir: ", str),
    "5": ("mayusculas", "Texto a pasar a mayúsculas: ", str),
    "6": ("dormir", "Segundos de trabajo simulado (0 a 10): ", float),
}


# ---------------------------------------------------------------------------
# Conexión con el servidor
# ---------------------------------------------------------------------------
def conectar():
    """
    Abre la conexión TCP con el servidor.
    Retorna (socket, archivo) o (None, None) si no se pudo conectar.
    """
    try:
        # Configuración del socket TCP/IP (IPv4, orientado a conexión)
        conexion = socket.create_connection((HOST, PUERTO_SERVIDOR), timeout=5)
        conexion.settimeout(TIMEOUT_CLIENTE)
        archivo = conexion.makefile("rw", encoding="utf-8", newline="\n")
        return conexion, archivo
    except OSError:
        print(f"[ERROR] No se pudo conectar a {HOST}:{PUERTO_SERVIDOR}. "
              "¿Está corriendo servidor.py?")
        return None, None


def consultar(archivo, pedido):
    """Envía un pedido y devuelve la respuesta del servidor (un diccionario)."""
    enviar_json(archivo, pedido)
    respuesta = recibir_json(archivo)
    if respuesta is None:
        raise ConnectionError("el servidor cerró la conexión")
    return respuesta


# ---------------------------------------------------------------------------
# Presentación de resultados
# ---------------------------------------------------------------------------
def acortar(texto):
    """Recorta textos muy largos (por ejemplo un factorial enorme) para mostrarlos."""
    texto = str(texto)
    if len(texto) <= MAX_MOSTRAR:
        return texto
    return f"{texto[:MAX_MOSTRAR]}... ({len(texto)} caracteres en total)"


def mostrar_respuesta(respuesta):
    """Imprime la respuesta del servidor en formato legible."""
    if not respuesta.get("ok"):
        print(f"[ERROR] {respuesta.get('error')}")
        return

    if "resultado" in respuesta:
        print(f"[RESULTADO] {acortar(respuesta['resultado'])}")
        print(f"            procesado por {respuesta.get('worker')} / "
              f"{respuesta.get('hilo')} en {respuesta.get('tiempo')} s "
              f"(tarea #{respuesta.get('id_tarea')})")
    else:
        print(f"[OK] {respuesta.get('mensaje')}")


# ---------------------------------------------------------------------------
# Opciones del menú
# ---------------------------------------------------------------------------
def pedir_credenciales():
    """Pide usuario y contraseña por consola."""
    usuario = input("Usuario: ").strip()
    contrasena = input("Contraseña: ")
    return usuario, contrasena


def opcion_registrar(archivo, sesion):
    """Opción 1: registrar un usuario."""
    usuario, contrasena = pedir_credenciales()
    respuesta = consultar(archivo, {"accion": "registro", "usuario": usuario,
                                    "contraseña": contrasena})
    mostrar_respuesta(respuesta)


def opcion_login(archivo, sesion):
    """Opción 2: iniciar sesión (guarda las credenciales para el lote de prueba)."""
    usuario, contrasena = pedir_credenciales()
    respuesta = consultar(archivo, {"accion": "login", "usuario": usuario,
                                    "contraseña": contrasena})
    mostrar_respuesta(respuesta)
    if respuesta.get("ok"):
        sesion["usuario"] = usuario
        sesion["contraseña"] = contrasena


def opcion_enviar_tarea(archivo, sesion):
    """Opción 3: elegir una tarea, enviarla y mostrar el resultado."""
    print("\nTareas disponibles:")
    for clave, (tipo, _, _) in MENU_TAREAS.items():
        print(f"  {clave}. {tipo}")

    eleccion = input("Elegí la tarea: ").strip()
    if eleccion not in MENU_TAREAS:
        print("Opción inválida.")
        return

    tipo, pregunta, convertir = MENU_TAREAS[eleccion]
    valor = input(pregunta)
    try:
        datos = convertir(valor)
    except ValueError:
        print("[ERROR] El valor ingresado no tiene el formato esperado.")
        return

    # Los segundos se envían como entero cuando no tienen decimales (2.0 -> 2)
    if isinstance(datos, float) and datos.is_integer():
        datos = int(datos)

    print("Enviando tarea al servidor...")
    mostrar_respuesta(consultar(archivo, {"accion": "tarea", "tipo": tipo, "datos": datos}))


def opcion_historial(archivo, sesion):
    """Opción 4: ver las últimas tareas del usuario."""
    respuesta = consultar(archivo, {"accion": "historial"})
    if not respuesta.get("ok"):
        mostrar_respuesta(respuesta)
        return

    historial = respuesta["historial"]
    if not historial:
        print("Todavía no enviaste tareas.")
        return

    print(f"\n{'ID':<5}{'Tarea':<12}{'Datos':<14}{'Estado':<12}{'Worker':<14}Fecha")
    for t in historial:
        print(f"{t['id']:<5}{t['tipo']:<12}{acortar(t['datos'])[:12]:<14}"
              f"{t['estado']:<12}{str(t['worker']):<14}{t['fecha']}")


# ---------------------------------------------------------------------------
# Lote de prueba: varias tareas en paralelo
# ---------------------------------------------------------------------------
def ejecutar_tarea_independiente(sesion, tipo, datos, resultados, posicion):
    """
    Corre en un hilo: abre su propia conexión, inicia sesión con las mismas
    credenciales, envía una tarea y guarda la respuesta en 'resultados'.
    """
    conexion, archivo = conectar()
    if conexion is None:
        resultados[posicion] = {"ok": False, "error": "sin conexión"}
        return
    try:
        with conexion, archivo:
            consultar(archivo, {"accion": "login", "usuario": sesion["usuario"],
                                "contraseña": sesion["contraseña"]})
            resultados[posicion] = consultar(archivo, {"accion": "tarea", "tipo": tipo,
                                                       "datos": datos})
    except (OSError, ValueError) as error:
        resultados[posicion] = {"ok": False, "error": str(error)}


def opcion_lote(archivo, sesion):
    """Opción 5: envía 6 tareas de 2 segundos a la vez y muestra cómo se repartieron."""
    if "usuario" not in sesion:
        print("[ERROR] Primero iniciá sesión (opción 2).")
        return

    cantidad = 6
    print(f"Enviando {cantidad} tareas de 2 segundos en paralelo...")
    resultados = [None] * cantidad
    hilos = [
        threading.Thread(target=ejecutar_tarea_independiente,
                         args=(sesion, "dormir", 2, resultados, i))
        for i in range(cantidad)
    ]

    inicio = time.perf_counter()
    for hilo in hilos:
        hilo.start()
    for hilo in hilos:
        hilo.join()
    total = time.perf_counter() - inicio

    print(f"\n{'#':<4}{'Worker':<14}{'Hilo':<20}{'Tiempo':<10}Estado")
    for i, r in enumerate(resultados, start=1):
        if r and r.get("ok"):
            print(f"{i:<4}{r['worker']:<14}{r['hilo']:<20}{r['tiempo']:<10}OK")
        else:
            print(f"{i:<4}{'-':<14}{'-':<20}{'-':<10}ERROR: {r and r.get('error')}")

    print(f"\nTiempo total: {total:.2f} s "
          f"(en serie habrían sido {cantidad * 2} s)")


# ---------------------------------------------------------------------------
# Programa principal
# ---------------------------------------------------------------------------
def mostrar_menu(sesion):
    """Imprime el menú principal."""
    quien = sesion.get("usuario", "sin sesión")
    print(f"\n=== Sistema distribuido de tareas ({quien}) ===")
    print("1. Registrar usuario")
    print("2. Iniciar sesión")
    print("3. Enviar una tarea")
    print("4. Ver mi historial")
    print("5. Lote de prueba (6 tareas en paralelo)")
    print("6. Salir")


def main():
    """Bucle principal del cliente."""
    configurar_consola()
    conexion, archivo = conectar()
    if conexion is None:
        return

    print(f"[CONEXION] Conectado al servidor {HOST}:{PUERTO_SERVIDOR}")
    sesion = {}
    opciones = {
        "1": opcion_registrar,
        "2": opcion_login,
        "3": opcion_enviar_tarea,
        "4": opcion_historial,
        "5": opcion_lote,
    }

    try:
        while True:
            mostrar_menu(sesion)
            eleccion = input("Elegí una opción: ").strip()

            if eleccion == "6":
                consultar(archivo, {"accion": "salir"})
                print("Hasta luego.")
                break
            elif eleccion in opciones:
                opciones[eleccion](archivo, sesion)
            else:
                print("Opción inválida.")
    except (OSError, ConnectionError) as error:
        print(f"[ERROR] Se perdió la conexión con el servidor: {error}")
    except KeyboardInterrupt:
        print("\n[CLIENTE] Interrumpido por el usuario.")
    finally:
        archivo.close()
        conexion.close()


if __name__ == "__main__":
    main()
