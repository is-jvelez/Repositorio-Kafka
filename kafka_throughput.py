import requests
import time
import random
from datetime import datetime

# ─────────────────────────────────────────
#  CONFIGURACIÓN
# ─────────────────────────────────────────
API_URL   = "http://localhost:5000/api/auth/register"
CANTIDAD  = 50   # mensajes en ráfaga
DELAY     = 0    # 0 = máxima velocidad

NOMBRES = [
    "Santiago", "Valentina", "Mateo", "Camila", "Sebastian",
    "Isabella", "Andres", "Gabriela", "Nicolas", "Daniela",
    "Felipe", "Natalia", "Alejandro", "Mariana", "Tomas",
    "Lucia", "Ricardo", "Fernanda", "Diego", "Catalina",
    "Miguel", "Sofia", "Esteban", "Paola", "Julian",
    "Karla", "Bruno", "Xiomara", "Fabian", "Renata",
    "Cristian", "Valeria", "Mauricio", "Jimena", "Rodrigo",
    "Adriana", "Gonzalo", "Vanessa", "Hector", "Lorena",
    "Cesar", "Monica", "Arturo", "Patricia", "Ernesto",
    "Carolina", "Ignacio", "Beatriz", "Raul", "Silvia",
]

APELLIDOS = [
    "Morales", "Torres", "Rios", "Herrera", "Vargas",
    "Castillo", "Mendoza", "Rojas", "Guerrero", "Perez",
    "Salazar", "Jimenez", "Bermudez", "Cardona", "Ospina",
    "Ramirez", "Gomez", "Lopez", "Martinez", "Sanchez",
    "Reyes", "Flores", "Diaz", "Cruz", "Vega",
    "Muñoz", "Aguilar", "Ortiz", "Gutierrez", "Chavez",
    "Ramos", "Romero", "Medina", "Alvarez", "Ruiz",
    "Suarez", "Molina", "Pena", "Soto", "Delgado",
    "Fuentes", "Ibarra", "Cabrera", "Nunez", "Estrada",
    "Cervantes", "Zavala", "Miranda", "Arias", "Campos",
]
# ─────────────────────────────────────────

def color(t, c): return f"\033[{c}m{t}\033[0m"
def verde(t):    return color(t, "92")
def rojo(t):     return color(t, "91")
def amarillo(t): return color(t, "93")
def cyan(t):     return color(t, "96")
def gris(t):     return color(t, "90")
def magenta(t):  return color(t, "95")

def generar_usuarios(cantidad):
    emails_usados = set()
    usuarios = []
    intentos = 0
    while len(usuarios) < cantidad and intentos < cantidad * 20:
        intentos += 1
        nombre   = random.choice(NOMBRES)
        apellido = random.choice(APELLIDOS)
        # agregar sufijo numérico para garantizar unicidad global
        sufijo   = random.randint(100, 999)
        email    = f"{nombre.lower()}.{apellido.lower()}{sufijo}@demo.com"
        if email not in emails_usados:
            emails_usados.add(email)
            usuarios.append({
                "nombre":   nombre,
                "apellido": apellido,
                "email":    email,
                "password": f"{nombre[0].upper()}{nombre.lower()[1:]}123!"
            })
    return usuarios

def registrar(u):
    payload = {
        "email":           u["email"],
        "password":        u["password"],
        "confirmPassword": u["password"],
    }
    try:
        resp = requests.post(API_URL, json=payload, timeout=10,
                             headers={"Content-Type": "application/json",
                                      "Accept": "application/json"})
        data = resp.json()
        if resp.status_code in (200, 201) and data.get("success"):
            return "ok"
        return "duplicado"
    except:
        return "error"

def barra_progreso(actual, total, ancho=40):
    pct     = actual / total
    llenos  = int(pct * ancho)
    barra   = "█" * llenos + "░" * (ancho - llenos)
    return f"[{barra}] {actual}/{total} ({pct*100:.0f}%)"

def main():
    print(cyan("""
╔══════════════════════════════════════════════╗
║     KAFKA DEMO — Throughput (ráfaga)         ║
║                                              ║
║  Abre Kafka UI → localhost:8090              ║
║  Topics → user-registered → Statistics      ║
║  Observa bytes/s producidos y consumidos     ║
╚══════════════════════════════════════════════╝
"""))

    usuarios = generar_usuarios(CANTIDAD)
    print(f"  Usuarios generados : {len(usuarios)}")
    print(f"  Delay entre c/u    : {DELAY}s (ráfaga máxima)")
    print(f"  API                : {API_URL}")
    print(f"\n  {amarillo('IMPORTANTE: Mientras corre, observa en Kafka UI:')}")
    print(f"  {amarillo('  → Topics → user-registered → pestaña Statistics')}")
    print(f"  {amarillo('  → Consumers → notification-group → Consumer Lag')}")
    input(f"\n  Presiona {cyan('Enter')} para iniciar la ráfaga...\n")

    resultados  = {"ok": 0, "duplicado": 0, "error": 0}
    inicio      = time.time()
    tiempos     = []

    for i, u in enumerate(usuarios, 1):
        t0       = time.time()
        resultado = registrar(u)
        t1       = time.time()
        tiempos.append(t1 - t0)
        resultados[resultado] += 1

        # barra de progreso en la misma línea
        icono = "✅" if resultado == "ok" else "⚠ " if resultado == "duplicado" else "❌"
        print(f"\r  {icono} {barra_progreso(i, len(usuarios))}  "
              f"{u['nombre']} {u['apellido']:<12}", end="", flush=True)

        if DELAY > 0:
            time.sleep(DELAY)

    duracion   = time.time() - inicio
    tps        = resultados["ok"] / duracion if duracion > 0 else 0
    avg_ms     = (sum(tiempos) / len(tiempos) * 1000) if tiempos else 0

    print(f"\n\n{cyan('═' * 60)}")
    print(cyan("  MÉTRICAS DE THROUGHPUT"))
    print(cyan('═' * 60))
    print(verde(f"  ✅  Registrados        : {resultados['ok']}"))
    print(amarillo(f"  ⚠   Rechazados         : {resultados['duplicado']}"))
    print(rojo(f"  ❌  Errores            : {resultados['error']}"))
    print(f"  ⏱   Duración total     : {duracion:.2f}s")
    print(magenta(f"  🚀  Throughput         : {tps:.1f} registros/segundo"))
    print(magenta(f"  📊  Latencia promedio  : {avg_ms:.0f}ms por request"))
    print(f"\n  {amarillo('En Kafka UI ahora deberías ver:')}")
    print(f"    • Statistics → pico de bytes producidos durante la ráfaga")
    print(f"    • Consumer Lag → subió durante la ráfaga y volvió a 0")
    print(f"    • Number of messages → incrementó en {resultados['ok']}")
    print(cyan('═' * 60))

if __name__ == "__main__":
    main()
