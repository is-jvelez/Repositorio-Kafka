import requests
import time
import random
from datetime import datetime

# ─────────────────────────────────────────
#  CONFIGURACIÓN
# ─────────────────────────────────────────
API_URL         = "http://localhost:5000/api/auth/register"
DELAY_SEGUNDOS  = 5
CANTIDAD        = 15   # cuántos usuarios generar
MOSTRAR_TOKENS  = False

NOMBRES = [
    "Santiago", "Valentina", "Mateo", "Camila", "Sebastian",
    "Isabella", "Andres", "Gabriela", "Nicolas", "Daniela",
    "Felipe", "Natalia", "Alejandro", "Mariana", "Tomas",
    "Lucia", "Ricardo", "Fernanda", "Diego", "Catalina",
    "Miguel", "Sofia", "Esteban", "Paola", "Julian",
]

APELLIDOS = [
    "Morales", "Torres", "Rios", "Herrera", "Vargas",
    "Castillo", "Mendoza", "Rojas", "Guerrero", "Perez",
    "Salazar", "Jimenez", "Bermudez", "Cardona", "Ospina",
    "Ramirez", "Gomez", "Lopez", "Martinez", "Sanchez",
    "Reyes", "Flores", "Diaz", "Cruz", "Vega",
]
# ─────────────────────────────────────────

def generar_usuarios(cantidad):
    """Combina nombres y apellidos aleatoriamente garantizando emails únicos."""
    emails_usados = set()
    usuarios = []
    intentos = 0
    while len(usuarios) < cantidad and intentos < cantidad * 10:
        intentos += 1
        nombre   = random.choice(NOMBRES)
        apellido = random.choice(APELLIDOS)
        email    = f"{nombre.lower()}.{apellido.lower()}@demo.com"
        if email not in emails_usados:
            emails_usados.add(email)
            usuarios.append({"nombre": nombre, "apellido": apellido})
    return usuarios

def construir_payload(nombre, apellido):
    return {
        "email":           f"{nombre.lower()}.{apellido.lower()}@demo.com",
        "password":        f"{nombre[0].upper()}{nombre.lower()[1:]}123!",
        "confirmPassword": f"{nombre[0].upper()}{nombre.lower()[1:]}123!",
    }

def color(texto, codigo): return f"\033[{codigo}m{texto}\033[0m"
def verde(t):    return color(t, "92")
def rojo(t):     return color(t, "91")
def amarillo(t): return color(t, "93")
def cyan(t):     return color(t, "96")
def gris(t):     return color(t, "90")
def separador(): print(gris("─" * 60))

def registrar(numero, nombre, apellido):
    payload = construir_payload(nombre, apellido)
    hora    = datetime.now().strftime("%H:%M:%S")

    print(f"\n{cyan(f'[{hora}]')} Intento #{numero} — {nombre} {apellido}")
    separador()
    print(f"  📧  Email    : {payload['email']}")
    print(f"  🔑  Password : {payload['password']}")

    try:
        resp = requests.post(API_URL, json=payload, timeout=10,
                             headers={"Content-Type": "application/json",
                                      "Accept": "application/json"})
        data = resp.json()

        if resp.status_code in (200, 201) and data.get("success"):
            user = data["data"]["user"]
            print(verde(f"\n  ✅  REGISTRADO — userId: {user['id']}"))
            print(f"  📨  Evento publicado en Kafka → topic: user-registered")
            if MOSTRAR_TOKENS:
                print(f"  🎫  JWT: {data['data']['accessToken'][:40]}...")
            print(amarillo(f"\n  👉  Kafka UI → Topics → user-registered → +1 mensaje"))
            print(amarillo(f"      Consumers → notification-group → Consumer Lag"))
            return "ok"

        elif not data.get("success"):
            errores = data.get("errors", [data.get("message", "Error desconocido")])
            print(rojo(f"\n  ❌  RECHAZADO: {errores}"))
            return "duplicado"

        else:
            print(rojo(f"\n  ❌  HTTP {resp.status_code}: {resp.text[:100]}"))
            return "error"

    except requests.exceptions.ConnectionError:
        print(rojo("\n  🔌  Sin conexión — ¿está corriendo docker compose up?"))
        return "error"
    except Exception as e:
        print(rojo(f"\n  💥  Error inesperado: {e}"))
        return "error"

def main():
    print(cyan("""
╔══════════════════════════════════════════╗
║        KAFKA DEMO — Flujo en tiempo real ║
║   Observa Kafka UI en localhost:8090     ║
╚══════════════════════════════════════════╝
"""))

    usuarios = generar_usuarios(CANTIDAD)

    print(f"  Usuarios a registrar : {len(usuarios)}")
    print(f"  Delay entre c/u      : {DELAY_SEGUNDOS}s")
    print(f"  API                  : {API_URL}")
    print(f"  Combinaciones únicas : nombres×apellidos aleatorios")
    print(f"\n  {amarillo('Abre Kafka UI → localhost:8090 antes de continuar')}")
    input(f"\n  Presiona {cyan('Enter')} para iniciar...\n")

    resultados = {"ok": 0, "duplicado": 0, "error": 0}

    for i, u in enumerate(usuarios, 1):
        resultado = registrar(i, u["nombre"], u["apellido"])
        resultados[resultado] += 1

        if i < len(usuarios):
            print(gris(f"\n  ⏱  Esperando {DELAY_SEGUNDOS}s — "
                       f"observa el Consumer Lag en Kafka UI..."))
            time.sleep(DELAY_SEGUNDOS)

    print(f"\n\n{cyan('═' * 60)}")
    print(cyan("  RESUMEN FINAL"))
    print(cyan('═' * 60))
    print(verde(f"  ✅  Registrados exitosamente : {resultados['ok']}"))
    print(amarillo(f"  ⚠   Duplicados/rechazados   : {resultados['duplicado']}"))
    print(rojo(f"  ❌  Errores                  : {resultados['error']}"))
    print(f"\n  {amarillo('Verifica en Kafka UI:')}")
    print(f"    • Topics → user-registered → Number of messages: {resultados['ok']}")
    print(f"    • Consumers → notification-group → Consumer Lag: 0")
    print(cyan('═' * 60))

if __name__ == "__main__":
    main()

# ─────────────────────────────────────────
#  Como ejecutarlo
#  C:\Users\jose.velez\AppData\Local\Programs\Python\Python310\python.exe kafka_demo.py
# ─────────────────────────────────────────
