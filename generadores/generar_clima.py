import os
import pandas as pd
import requests
from datetime import datetime, timedelta
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
import logging
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
WORKSHEET_NAME = "TempHistorico"

# --- API, Ubicación y Umbrales de Clima ---
LATITUDE = -33.45
LONGITUDE = -70.67
UMBRAL_FRIO = 18.0
UMBRAL_CALUROSO = 25.0
UMBRAL_LLUVIA = 0.1

# --- Rango de Fechas ---
START_DATE = "2021-01-01"
FORECAST_DAYS = 14


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


def fetch_weather_data(historical_start_date, forecast_days):
    """Obtiene datos de temperatura y precipitación históricos y futuros de Open-Meteo."""
    today = datetime.today().date()
    historical_end_date = today - timedelta(days=1)
    df_hist, df_future = pd.DataFrame(), pd.DataFrame()

    logging.info(f"Obteniendo datos históricos de clima desde {historical_start_date}...")
    try:
        historical_url = (
            f"https://archive-api.open-meteo.com/v1/archive?latitude={LATITUDE}&longitude={LONGITUDE}"
            f"&start_date={historical_start_date}&end_date={historical_end_date.strftime('%Y-%m-%d')}"
            "&daily=temperature_2m_max,precipitation_sum&timezone=America/Santiago"
        )
        resp = requests.get(historical_url, timeout=30)
        resp.raise_for_status()
        data = resp.json().get("daily", {})
        if data and "time" in data and "temperature_2m_max" in data:
            df_hist = pd.DataFrame({
                'fecha': pd.to_datetime(data['time']),
                'temperatura_max_c': data['temperature_2m_max'],
                'precipitacion_mm': data.get('precipitation_sum', 0)
            })
            logging.info(f"✅ Se obtuvieron {len(df_hist)} registros históricos de clima.")
    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Error al consultar la API de archivo de clima: {e}")

    logging.info(f"Obteniendo pronóstico de clima para los próximos {forecast_days} días...")
    try:
        forecast_start_date = today
        forecast_end_date = today + timedelta(days=forecast_days)
        forecast_url = (
            f"https://api.open-meteo.com/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}"
            f"&start_date={forecast_start_date.strftime('%Y-%m-%d')}&end_date={forecast_end_date.strftime('%Y-%m-%d')}"
            "&daily=temperature_2m_max,precipitation_sum&timezone=America/Santiago"
        )
        resp = requests.get(forecast_url, timeout=20)
        resp.raise_for_status()
        data = resp.json().get("daily", {})
        if data and "time" in data and "temperature_2m_max" in data:
            df_future = pd.DataFrame({
                'fecha': pd.to_datetime(data['time']),
                'temperatura_max_c': data['temperature_2m_max'],
                'precipitacion_mm': data.get('precipitation_sum', 0)
            })
            logging.info(f"✅ Se obtuvieron {len(df_future)} días de pronóstico de clima.")
    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Error al consultar la API de pronóstico de clima: {e}")

    if not df_hist.empty or not df_future.empty:
        df_total = pd.concat([df_hist, df_future], ignore_index=True).dropna(subset=['fecha'])
        df_total['dia_frio'] = (df_total['temperatura_max_c'] <= UMBRAL_FRIO).astype(int)
        df_total['dia_caluroso'] = (df_total['temperatura_max_c'] > UMBRAL_CALUROSO).astype(int)
        df_total['dia_lluvioso'] = (df_total['precipitacion_mm'] > UMBRAL_LLUVIA).astype(int)
        df_total['frio_y_lluvioso'] = ((df_total['dia_frio'] == 1) & (df_total['dia_lluvioso'] == 1)).astype(int)
        df_final = df_total[
            ['fecha', 'temperatura_max_c', 'dia_frio', 'dia_caluroso', 'dia_lluvioso', 'frio_y_lluvioso']]
        logging.info(f"✅ Se procesaron los datos. Total de registros: {len(df_final)}.")
        return df_final
    else:
        logging.error("❌ No se pudo obtener ningún dato de clima.")
        return pd.DataFrame()


def export_to_gsheets(df, spreadsheet, worksheet_name):
    """Limpia una hoja y la sobreescribe con el contenido de un DataFrame."""
    if df.empty:
        logging.warning("⚠️ El DataFrame está vacío, no se exportará nada.")
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
    """Función principal que orquesta la generación de la tabla de clima."""
    logging.info(f"🚀 Iniciando el proceso para generar la tabla de clima en '{WORKSHEET_NAME}'.")

    client = autorizar_gsheets()
    spreadsheet = client.open(SPREADSHEET_NAME)

    df_weather = fetch_weather_data(START_DATE, FORECAST_DAYS)

    if not df_weather.empty:
        export_to_gsheets(df_weather, spreadsheet, WORKSHEET_NAME)

    logging.info("🏁 Proceso de generación de clima finalizado.")


if __name__ == "__main__":
    main()
