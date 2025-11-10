# -*- coding: utf-8 -*-
"""
Fantasy Full + Calendar + Google Sheets Uploader
Autor: ChatGPT
Fecha: 2025-11-07
"""

from playwright.sync_api import sync_playwright
import pandas as pd
import json
from datetime import datetime
import unicodedata
from difflib import get_close_matches
from dateutil.parser import parse as dateparse
import time

# --- CONFIGURACIÓN ---
URL_MERCADO = "https://www.futbolfantasy.com/analytics/laliga-fantasy/mercado"
ARCHIVO_JSON = "es.1.json"
CSV_SALIDA = "fantasy_mercado_completo.csv"
CREDENCIALES_JSON = "fantasyuploader-c620411f5c89.json"
GOOGLE_SHEET_ID = "1GVjWGTC8t4jyJQ_EPavbdAiZhvCCWlXXrPpKR_sYQ7Q"  # <-- cambia esto
NOMBRE_HOJA = "Mercado"
ESPERA_PAGINA = 0.5
# ----------------------

# --- FUNCIONES AUXILIARES ---
def normalizar(text):
    if not isinstance(text, str):
        return ""
    t = text.strip().lower()
    t = ''.join(c for c in unicodedata.normalize('NFD', t) if unicodedata.category(c) != 'Mn')
    t = ''.join(ch for ch in t if ch.isalnum() or ch.isspace())
    return " ".join(t.split())

STOP_WORDS = {"cf","fc","rcd","real","rc","c","de","la","el","club","ca","ud","sd","futbol","balompie","balompiee"}

def tokens_utiles(nombre):
    toks = [tok for tok in normalizar(nombre).split() if tok and tok not in STOP_WORDS]
    return set(toks)

# --- CARGAR CALENDARIO ---
with open(ARCHIVO_JSON, "r", encoding="utf-8") as f:
    calendario = json.load(f)

matches = calendario.get("matches", [])
hoy = datetime.now().date()

partidos_por_equipo = {}
for m in matches:
    fecha_raw = m.get("date")
    if not fecha_raw:
        continue
    try:
        fecha = dateparse(fecha_raw).date()
    except:
        continue
    if fecha >= hoy:
        t1 = m.get("team1", "").strip()
        t2 = m.get("team2", "").strip()
        if not t1 or not t2:
            continue
        partidos_por_equipo.setdefault(t1, []).append((fecha, t2))
        partidos_por_equipo.setdefault(t2, []).append((fecha, t1))

for k in list(partidos_por_equipo.keys()):
    partidos_por_equipo[k].sort(key=lambda x: x[0])

map_norm_to_originals = {}
for equipo in partidos_por_equipo.keys():
    norm = normalizar(equipo)
    map_norm_to_originals.setdefault(norm, []).append(equipo)

lista_equipos_original = list(partidos_por_equipo.keys())
lista_equipos_norm = list(map_norm_to_originals.keys())

def encontrar_proximos_rivales(equipo_input, n=3):
    if not isinstance(equipo_input, str) or equipo_input.strip() == "":
        return []
    norm_in = normalizar(equipo_input)
    if norm_in in map_norm_to_originals:
        orig = map_norm_to_originals[norm_in][0]
        return [r for _, r in partidos_por_equipo.get(orig, [])][:n]
    toks_in = tokens_utiles(equipo_input)
    if toks_in:
        mejor, mejor_score = None, 0
        for orig in lista_equipos_original:
            toks_orig = tokens_utiles(orig)
            inter = toks_in.intersection(toks_orig)
            score = len(inter) / max(len(toks_orig), 1)
            if score > mejor_score:
                mejor, mejor_score = orig, score
        if mejor and mejor_score >= 0.34:
            return [r for _, r in partidos_por_equipo.get(mejor, [])][:n]
    posibles = get_close_matches(norm_in, lista_equipos_norm, n=3, cutoff=0.6)
    if posibles:
        cand_norm = posibles[0]
        origs = map_norm_to_originals.get(cand_norm, [])
        if origs:
            return [r for _, r in partidos_por_equipo.get(origs[0], [])][:n]
    for orig in lista_equipos_original:
        if norm_in in normalizar(orig) or normalizar(orig) in norm_in:
            return [r for _, r in partidos_por_equipo.get(orig, [])][:n]
    return []

# --- SCRAPE FÚTBOL FANTASY ---
def extraer_mercado_playwright():
    datos = []
    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=True)
        pagina = navegador.new_page()
        pagina.goto(URL_MERCADO, timeout=0)
        pagina.wait_for_selector("div.elemento_jugador", timeout=20000)

        jugadores = pagina.query_selector_all("div.elemento_jugador")
        print(f"🟢 Se encontraron {len(jugadores)} jugadores en el mercado.")

        for j in jugadores:
            try:
                nombre = (j.get_attribute("data-nombre") or "").strip()
                posicion = (j.get_attribute("data-posicion") or "").strip()
                equipo_node = j.query_selector(".equipo span")
                equipo = equipo_node.inner_text().strip() if equipo_node else (j.get_attribute("data-equipo") or "").strip()
                valor = j.get_attribute("data-valor") or ""
                diferencia = j.get_attribute("data-diferencia1") or ""
                diferencia_pct = j.get_attribute("data-diferencia-pct1") or ""

                try:
                    val_int = int(valor)
                except:
                    val_int = ""
                try:
                    diff_int = int(diferencia) if diferencia != "" else ""
                except:
                    diff_int = ""

                datos.append({
                    "Jugador": nombre,
                    "Equipo": equipo,
                    "Posición": posicion,
                    "Valor_raw": val_int,
                    "Variación_raw": diff_int,
                    "Variación_pct_raw": float(diferencia_pct) if diferencia_pct else ""
                })
            except Exception as e:
                print("Error procesando jugador:", e)
                continue

        navegador.close()
    return datos

# --- SUBIDA A GOOGLE SHEETS ---
def subir_a_google_sheets(df):
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        import json, os

        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]

        # 🔹 En lugar de leer el archivo local:
        # creds = Credentials.from_service_account_file(CREDENCIALES_JSON, scopes=scopes)

        # 🔹 Cargar desde el secreto de GitHub Actions:
        creds_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT")
        creds_dict = json.loads(creds_json)
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)

        cliente = gspread.authorize(creds)
        spreadsheet = cliente.open_by_key(GOOGLE_SHEET_ID)
        try:
            hoja = spreadsheet.worksheet(NOMBRE_HOJA)
        except gspread.exceptions.WorksheetNotFound:
            hoja = spreadsheet.add_worksheet(title=NOMBRE_HOJA, rows="1000", cols="20")

        hoja.clear()
        hoja.update([df.columns.values.tolist()] + df.values.tolist())
        print(f"📤 Datos subidos correctamente a Google Sheets ({NOMBRE_HOJA})")
    except Exception as e:
        print("⚠️ Error al subir a Google Sheets:", e)

# --- MAIN ---
def main():
    print("🏁 Inicio:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    mercado = extraer_mercado_playwright()
    df = pd.DataFrame(mercado)
    if df.empty:
        print("❌ No se extrajeron jugadores.")
        return

    proximos = []
    for eq in df["Equipo"]:
        rivales = encontrar_proximos_rivales(eq, 3)
        proximos.append(" | ".join(rivales) if rivales else "N/A")
    df["Prox.Partidos"] = proximos
    df["Timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    df["Valor (€)"] = df["Valor_raw"].apply(lambda x: f"{int(x):,}".replace(",", ".") if x != "" else "")
    df["Variación (€)"] = df["Variación_raw"].apply(lambda x: f"{int(x):,}".replace(",", ".") if x != "" else "")
    df["Variación (%)"] = df["Variación_pct_raw"].apply(lambda x: f"{x:.2f}%" if x != "" else "")

    columnas = ["Jugador", "Equipo", "Posición", "Valor (€)", "Variación (€)", "Variación (%)", "Prox.Partidos", "Timestamp"]
    df_final = df[columnas]

    df_final.to_csv(CSV_SALIDA, index=False, encoding="utf-8-sig")
    print(f"💾 Guardado localmente: {CSV_SALIDA} ({len(df_final)} jugadores)")

    subir_a_google_sheets(df_final)

if __name__ == "__main__":
    main()
