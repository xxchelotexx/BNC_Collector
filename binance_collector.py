import requests
import json
import time
import threading
import sys
import os
from collections import defaultdict
from datetime import datetime, time as dt_time, timedelta, timezone

# 1. Importar herramientas de seguridad y MongoDB
from pymongo import MongoClient
from dotenv import load_dotenv

# Cargar variables desde el archivo .env
load_dotenv()

if sys.platform.startswith('win'):
    sys.stdout.reconfigure(encoding='utf-8')

# === CONFIGURACIÓN BINANCE ===
URL_API = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.60 Safari/537.36",
    "Referer": "https://p2p.binance.com/",
    "Content-Type": "application/json"
}

# === CONFIGURACIÓN MONGODB ATLAS ===
db_user = os.getenv("MONGO_USER")
db_pass = os.getenv("MONGO_PASS")
db_cluster = os.getenv("MONGO_CLUSTER")

MONGO_URI = f"mongodb+srv://{db_user}:{db_pass}@{db_cluster}/?retryWrites=true&w=majority"

# Definimos la zona horaria de Bolivia para los logs de consola y el cálculo del intervalo
UTC_BOLIVIA = timezone(timedelta(hours=-4))

try:
    client = MongoClient(MONGO_URI)
    db = client["Monitor_P2P_Bolivia"]
    collection = db["BNC_PRICE"]
    # Verificamos la conexión
    client.admin.command('ping')
    print("✅ Conexión exitosa a MongoDB Atlas (Binance).")
except Exception as e:
    print(f"❌ Error crítico de conexión a MongoDB: {e}")
    sys.exit(1)

# ----------------------------------------------------
# 💰 Función de procesamiento de datos
# ----------------------------------------------------
def procesar_datos(data, trade_type):
    agrupado = defaultdict(lambda: {
        "suma": 0.0, 
        "conteo": 0, 
        "min": float('inf'), 
        "max": 0.0, 
        "inmediato": 0.0
    })
    vol_total = 0.0
    
    for item in data:
        adv_data = item.get("adv", {})
        precio_str = adv_data.get("price")
        cantidad_str = adv_data.get("tradableQuantity")
        min_single_str = adv_data.get("minSingleTransAmount")
        max_single_str = adv_data.get("maxSingleTransAmount")
        
        if precio_str and cantidad_str:
            try:
                precio = float(precio_str)
                cantidad = float(cantidad_str)
                min_single = float(min_single_str) if min_single_str else 0.0
                max_single = float(max_single_str) if max_single_str else 0.0

                if precio == 0 or cantidad == 0: continue
                
                # Lógica de máximo calculado igual a Bybit
                ad_max_calculado = min(cantidad * precio, max_single) if max_single else cantidad * precio

                vol_total += cantidad
                entry = agrupado[precio]
                
                entry["suma"] += cantidad
                entry["conteo"] += 1
                
                if min_single < entry["min"]:
                    entry["min"] = min_single
                
                if ad_max_calculado > entry["max"]:
                    entry["max"] = ad_max_calculado
                
                entry["inmediato"] += ad_max_calculado / precio

            except ValueError:
                continue

    datos_agrupados_limpios = {}
    for p, valores in agrupado.items():
        if valores["min"] == float('inf'): valores["min"] = 0.0
        # Formateo de llave igual a Bybit para consistencia
        key_precio = f"{p:.2f}".replace(".", "_")
        datos_agrupados_limpios[key_precio] = valores

    return {
        "trade_type": trade_type,
        "vol_total": vol_total,
        "datos_agrupados": datos_agrupados_limpios
    }

# ----------------------------------------------------
# 📥 Función de obtención y guardado
# ----------------------------------------------------
def obtener_y_guardar_datos():
    ahora_bo = datetime.now(UTC_BOLIVIA)
    print(f"\n--- 📡 Iniciando recolección Binance: {ahora_bo.strftime('%H:%M:%S')} ---")
    
    resultados = []
    escenarios = [{"type": "BUY"}, {"type": "SELL"}]
    
    # Diccionario temporal para guardar los merchants por tipo de operación
    merchants_por_tipo = {"BUY": [], "SELL": []}

    for escenario in escenarios:
        trade_type = escenario["type"]
        items = []
        
        for page in range(1, 25):
            payload = {
                "fiat": "BOB", "page": page, "rows": 10, "tradeType": trade_type, 
                "asset": "USDT", "payTypes": [], "countries": [], "filterOptions": {}
            }
            
            try:
                response = requests.post(URL_API, headers=HEADERS, data=json.dumps(payload), timeout=10) 
                if response.status_code == 200:
                    data = response.json()
                    if data.get("success") and data.get("data"):
                        batch = data["data"]
                        items.extend(batch)
                        
                        # --- LÓGICA PARA MERCHANTS ---
                        for item in batch:
                            # Solo procesar si aún no tenemos 10 y es merchant
                            if len(merchants_por_tipo[trade_type]) < 10:
                                advertiser = item.get("advertiser", {})
                                adv_data = item.get("adv", {})
                                
                                if advertiser.get("userType") == "merchant":
                                    merchants_por_tipo[trade_type].append({
                                        "nickName": advertiser.get("nickName"),
                                        "price": adv_data.get("price"),
                                        "tradableQuantity": adv_data.get("tradableQuantity"),
                                        "minSingleTransAmount": adv_data.get("minSingleTransAmount"),
                                        "maxSingleTransAmount": adv_data.get("maxSingleTransAmount"),
                                        "advNo": adv_data.get("advNo") # Identificador del anuncio (opcional)
                                    })
                    else: break
                else: break
            except Exception as e:
                print(f"⚠️ Error API Binance {trade_type} p{page}: {e}")
                break

        resultados.append(procesar_datos(items, trade_type))
    
    # --- LA PARTE CRÍTICA: IGUALAR A BYBIT ---
    # Usamos la misma estructura que en tu código de Bybit
    documento = {
            "timestamp": datetime.now(timezone.utc),
            "exchange": "binance",
            "resultados": resultados,
            # Nuevas listas solicitadas
            "merchant_buy": merchants_por_tipo["BUY"],
            "merchant_sell": merchants_por_tipo["SELL"]
        }
        
    try:
        collection.insert_one(documento)
        print(f"✅ Datos de Binance guardados en MongoDB.")
    except Exception as e:
        print(f"❌ Error al insertar en Binance: {e}")

# ----------------------------------------------------
# ⏰ Función Worker
# ----------------------------------------------------
def worker():
    print("🚀 Recolector Binance iniciado.")
    
    while True:
        ahora_bo = datetime.now(UTC_BOLIVIA)
        # Intervalo: 10s día, 60s noche (puedes ajustarlo a 30s como en Bybit)
        intervalo = 10 if 6 <= ahora_bo.hour <= 23 else 60

        obtener_y_guardar_datos()
        time.sleep(intervalo)

if __name__ == '__main__':
    # Usamos un try/except simple para el hilo principal
    try:
        worker()
    except KeyboardInterrupt:
        print("\n🛑 Deteniendo el recolector de Binance...")