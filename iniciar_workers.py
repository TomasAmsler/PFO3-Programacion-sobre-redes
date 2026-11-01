"""
PFO 3: Rediseño como Sistema Distribuido (Cliente-Servidor)
Lanzador de workers

Programación sobre Redes - 1C 2026

Inicia, en una sola terminal, un proceso worker por cada puerto definido en
WORKERS (comun.py). Con Ctrl+C se detienen todos juntos.

Uso:
    python iniciar_workers.py
"""

import subprocess
import sys
import time

from comun import HILOS_POR_WORKER, WORKERS, configurar_consola


def lanzar_workers():
    """Abre un proceso 'python worker.py <puerto>' por cada worker configurado."""
    procesos = []
    for _, puerto in WORKERS:
        # '-u' desactiva el buffer para ver los mensajes de cada worker al instante
        proceso = subprocess.Popen(
            [sys.executable, "-u", "worker.py", str(puerto), str(HILOS_POR_WORKER)]
        )
        procesos.append(proceso)
    return procesos


def detener_workers(procesos):
    """Termina todos los procesos worker que sigan vivos."""
    for proceso in procesos:
        if proceso.poll() is None:
            proceso.terminate()
    for proceso in procesos:
        try:
            proceso.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proceso.kill()


def main():
    """Punto de entrada del lanzador."""
    configurar_consola()
    print(f"[LANZADOR] Iniciando {len(WORKERS)} workers... (Ctrl+C para detenerlos)")
    procesos = lanzar_workers()

    try:
        # Mantiene vivo el lanzador mientras haya algún worker corriendo
        while any(p.poll() is None for p in procesos):
            time.sleep(1)
        print("[LANZADOR] Todos los workers terminaron.")
    except KeyboardInterrupt:
        print("\n[LANZADOR] Deteniendo workers...")
    finally:
        detener_workers(procesos)
        print("[LANZADOR] Fin.")


if __name__ == "__main__":
    main()
