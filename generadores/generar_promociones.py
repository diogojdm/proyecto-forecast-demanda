import os
import pandas as pd
import numpy as np
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
WORKSHEET_NAME = "Promociones"

# --- Parámetros de Promoción ---
PROMOCIONES = [
    {
        "nombre_columna": "fuerza_promo_pastel_trozo",
        "fecha_inicio": "2025-05-01",
        "fecha_fin": None,
        "fuerza": 0.17
    },
]

# --- Rango de Fechas ---
START_DATE = "2021-01-01"
END_DATE = (datetime.today() + timedelta(days=14)).strftime("%Y-%m-%d")


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


def generar_tabla_promociones(start_date, end_date, promociones_config):
    """Crea un DataFrame con columnas para cada promoción definida."""
    logging.info("Generando la tabla de promociones...")

    fechas = pd.date_range(start=start_date, end=end_date, freq="D")
    df_promos = pd.DataFrame({'fecha': fechas})

    for promo in promociones_config:
        col_name = promo["nombre_columna"]
        start = pd.to_datetime(promo["fecha_inicio"])
        end = pd.to_datetime(promo["fecha_fin"]) if promo["fecha_fin"] else None
        fuerza = promo["fuerza"]

        mask = (df_promos['fecha'] >= start)
        if end:
            mask &= (df_promos['fecha'] <= end)

        df_promos[col_name] = np.where(mask, fuerza, 0)

        logging.info(f"Columna '{col_name}' creada con fuerza {fuerza}.")

    return df_promos


def export_to_gsheets(df, spreadsheet, worksheet_name):
    """Limpia una hoja y la sobreescribe con el contenido de un DataFrame."""
    if df.empty:
        logging.warning("⚠️ El DataFrame de promociones está vacío, no se exportará nada.")
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
    """Función principal que orquesta la generación de la tabla de promociones."""
    logging.info(f"🚀 Iniciando el proceso para generar la tabla de promociones en '{WORKSHEET_NAME}'.")

    client = autorizar_gsheets()
    spreadsheet = client.open(SPREADSHEET_NAME)

    df_promotions = generar_tabla_promociones(START_DATE, END_DATE, PROMOCIONES)

    if not df_promotions.empty:
        export_to_gsheets(df_promotions, spreadsheet, WORKSHEET_NAME)

    logging.info("🏁 Proceso de generación de promociones finalizado.")


if __name__ == "__main__":
    main()
