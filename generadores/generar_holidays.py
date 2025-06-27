import os
import pandas as pd
import requests
from datetime import datetime, timedelta
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
import logging
import time
from calendar import monthrange
import json

# --- Configuración de Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# =============================================================================
# --------------------------- CONFIGURACIÓN GLOBAL ----------------------------
# =============================================================================

# --- Google Sheets ---
try:
    RUTA_CREDENCIALES = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS",
                                       "G:/Mi unidad/Proyecto_Data/1. pythonProject/Proyeccion_Demanda/forecast-459600-d8ffd029be68.json")
    SCOPE_GOOGLE = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
except Exception as e:
    logging.error(f"❌ Error al definir la ruta de credenciales: {e}")
    raise

SPREADSHEET_NAME = "1. Forecast_Diario"
WORKSHEET_NAME = "Holidays"

# --- Rango de Fechas ---
start_date = "2021-01-01"
end_date = datetime.today() + timedelta(days=14)
fechas = pd.date_range(start=start_date, end=end_date, freq="D")
anios = fechas.year.unique()


# =============================================================================
# ---------------------------- FUNCIONES MODULARES ----------------------------
# =============================================================================

def autorizar_gsheets():
    """Autoriza el acceso a Google Sheets usando un secreto en GitHub Actions o un archivo local."""
    try:
        # Intenta leer el secreto desde la variable de entorno de GitHub Actions
        gspread_creds_json = os.environ.get("GSPREAD_CREDENTIALS")

        if gspread_creds_json:
            logging.info("Usando credenciales desde GitHub Secrets.")
            # Cargar las credenciales desde el string JSON
            creds_dict = json.loads(gspread_creds_json)
            creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, SCOPE_GOOGLE)
        else:
            # Si no está en GitHub, busca el archivo local
            logging.info("Usando archivo de credenciales local.")
            local_path = "G:/Mi unidad/Proyecto_Data/1. pythonProject/Proyeccion_Demanda/forecast-459600-d8ffd029be68.json"
            creds = ServiceAccountCredentials.from_json_keyfile_name(local_path, SCOPE_GOOGLE)

        client = gspread.authorize(creds)
        logging.info("✅ Autorización con Google Sheets exitosa.")
        return client
    except Exception as e:
        logging.error(f"❌ Error al autorizar con Google Sheets: {e}")
        raise


def obtener_feriados_boostr(anios):
    """Obtiene los feriados para una lista de años desde la API de Boostr, con un sistema de reintentos."""
    all_dates = []
    logging.info(f"Consultando feriados desde Boostr para los años: {anios}")

    session = requests.Session()

    for anio in sorted(set(anios)):
        url = f"https://api.boostr.cl/holidays/{anio}.json"
        retries = 3

        for attempt in range(retries):
            try:
                headers = {'User-Agent': 'Mozilla/5.0'}
                resp = session.get(url, timeout=15, headers=headers)
                resp.raise_for_status()

                data = resp.json().get("data", [])
                all_dates.extend([item["date"] for item in data])

                logging.info(f"✅ Feriados para el año {anio} obtenidos exitosamente desde Boostr.")
                break

            except requests.exceptions.RequestException as e:
                logging.warning(f"⚠️ Intento {attempt + 1}/{retries} falló para el año {anio} en Boostr: {e}")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    logging.error(f"❌ No se pudo consultar el año {anio} desde Boostr después de {retries} intentos.")

    return pd.to_datetime(sorted(set(all_dates)))


def export_to_gsheets(df, spreadsheet, worksheet_name):
    """Limpia una hoja y la sobreescribe con el contenido de un DataFrame."""
    if df.empty:
        logging.warning("⚠️ El DataFrame de feriados está vacío, no se exportará nada.")
        return

    try:
        df['fecha'] = df['fecha'].dt.strftime('%Y-%m-%d')

        try:
            worksheet = spreadsheet.worksheet(worksheet_name)
            logging.info(f"Hoja '{worksheet_name}' encontrada. Limpiando contenido...")
            worksheet.clear()
        except gspread.exceptions.WorksheetNotFound:
            logging.info(f"Hoja '{worksheet_name}' no encontrada. Creándola...")
            worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows=len(df) + 1, cols=len(df.columns))

        set_with_dataframe(worksheet, df)
        logging.info(f"✅ Tabla '{worksheet_name}' exportada exitosamente con {len(df)} filas.")

    except Exception as e:
        logging.error(f"❌ Error al exportar a Google Sheets: {e}")


def main():
    """Función principal que orquesta la generación de la tabla de feriados."""
    logging.info(f"🚀 Iniciando el proceso para generar la tabla de feriados en '{WORKSHEET_NAME}'.")

    feriados_todos = obtener_feriados_boostr(anios)
    logging.info(f"Se encontraron {len(feriados_todos)} feriados en total.")

    # --- Clasificación de Eventos y Feriados ---
    irrenunciables_mmdd = {"01-01", "01-05", "18-09", "19-09", "25-12"}
    feriados_irrenunciables = {d for d in feriados_todos if d.strftime("%d-%m") in irrenunciables_mmdd}
    feriados_ordinarios = {d for d in feriados_todos if d not in feriados_irrenunciables and d.weekday() < 5}
    dias_previos_brutos = {d - timedelta(days=1) for d in feriados_todos}
    dia_anterior_feriado = {d for d in dias_previos_brutos if d not in feriados_todos and d.weekday() < 5}
    dia_anterior_navidad = {datetime(year, 12, 24) for year in anios}
    dia_anterior_ano_nuevo = {datetime(year, 12, 31) for year in anios}

    dias_de_la_madre = set()
    for year in anios:
        mayo = pd.date_range(start=f'{year}-05-01', end=f'{year}-05-31', freq='D')
        dias_de_la_madre.add(mayo[mayo.weekday == 6][1])
        dias_de_la_madre.add(mayo[mayo.weekday == 6][1] - timedelta(days=1))

    dias_del_padre = set()
    for year in anios:
        junio = pd.date_range(start=f'{year}-06-01', end=f'{year}-06-30', freq='D')
        dias_del_padre.add(junio[junio.weekday == 6][2])
        dias_del_padre.add(junio[junio.weekday == 6][2] - timedelta(days=1))

    dias_pago = set()
    for year in anios:
        for month in range(1, 13):
            last_day = monthrange(year, month)[1]
            dias_pago.add(datetime(year, month, last_day))
            dias_pago.add(datetime(year, month, last_day - 1))
            dias_pago.add(datetime(year, month, last_day - 2))

    periodos_vacaciones = [
        ("2021-01-01", "2021-02-28"), ("2021-07-12", "2021-07-23"),
        ("2022-01-01", "2022-02-28"), ("2022-07-11", "2022-07-22"),
        ("2023-01-01", "2023-02-28"), ("2023-07-03", "2023-07-14"),
        ("2024-01-01", "2024-02-29"), ("2024-06-24", "2024-07-05"),
        ("2025-01-01", "2025-02-28"), ("2025-07-07", "2025-07-18"),
        ("2026-01-01", "2026-02-28"),
    ]
    vacaciones_escolares = set()
    for start, end in periodos_vacaciones:
        vacaciones_escolares.update(pd.date_range(start=start, end=end, freq='D'))

    # --- Construcción del DataFrame ---
    logging.info("Construyendo el DataFrame final de feriados y eventos.")
    df_h = pd.DataFrame({"fecha": fechas})
    df_h['feriado_ordinario'] = df_h['fecha'].isin(feriados_ordinarios).astype(int)
    df_h['feriado_irrenunciable'] = df_h['fecha'].isin(feriados_irrenunciables).astype(int)
    df_h['dia_previo_feriado'] = df_h['fecha'].isin(dia_anterior_feriado).astype(int)
    df_h['dia_previo_navidad'] = df_h['fecha'].isin(dia_anterior_navidad).astype(int)
    df_h['dia_previo_ano_nuevo'] = df_h['fecha'].isin(dia_anterior_ano_nuevo).astype(int)
    df_h['dia_madre'] = df_h['fecha'].isin(dias_de_la_madre).astype(int)
    df_h['dia_padre'] = df_h['fecha'].isin(dias_del_padre).astype(int)
    df_h['dia_pago'] = df_h['fecha'].isin(dias_pago).astype(int)
    df_h['vacaciones_escolares'] = df_h['fecha'].isin(vacaciones_escolares).astype(int)

    # --- Exportación ---
    client = autorizar_gsheets()
    spreadsheet = client.open(SPREADSHEET_NAME)
    export_to_gsheets(df_h, spreadsheet, WORKSHEET_NAME)

    logging.info("🏁 Proceso de generación de feriados finalizado.")


if __name__ == "__main__":
    main()
