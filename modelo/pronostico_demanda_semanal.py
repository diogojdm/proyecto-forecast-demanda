import os
import pandas as pd
import numpy as np
from prophet import Prophet
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import logging
import json # ¡Nuevo! Importa la librería json

# --- Configuración de Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# =============================================================================
# --------------------------- CONFIGURACIÓN GLOBAL ----------------------------
# =============================================================================

# --- Google Sheets ---
SCOPE_GOOGLE = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]

# RUTA_CREDENCIALES ya NO es una ruta de archivo fija. Se leerá desde una variable de entorno.
SPREADSHEET_NAME = "1. Forecast_Semanal"
OUTPUT_SHEET_NAME = "Demanda Total"

# --- Archivos de Ventas ---
# ¡CAMBIO AQUÍ! La ruta ahora es relativa a la raíz del repositorio de GitHub.
# Asumiendo que tus CSV están en la carpeta 'data' dentro de la raíz del repositorio.
CARPETA_VENTAS = "data"

# --- Parámetros del Modelo y Fechas ---
FORECAST_PERIOD_WEEKS = 52
HISTORY_PERIOD_WEEKS = 8

# --- Umbral para pronóstico simplificado ---
MIN_WEEKS_FOR_PROPHET = 12
WEEKS_FOR_REPRESENTATIVENESS = 4

# --- Parámetros de Promoción ---
PROMO_START_DATE = pd.to_datetime("2025-05-01")
PROMO_CATEGORY = 'Pastel Trozo'

# --- Filtros de Datos ---
GRUPOS_INCLUIDOS = ['Delicias', 'Pastel Grande', 'Pastel Mediano', 'Pastel Trozo']
ORDENES_EXCLUIDAS = ['Good Meal']
FAMILIAS_EXCLUIDAS = ['Gift Box']


# =============================================================================
# ---------------------------- FUNCIONES MODULARES ----------------------------
# =============================================================================

# ¡CAMBIO AQUÍ! Nueva función para autorizar usando el JSON de credenciales directamente
def autorizar_gsheets_from_env(credentials_json_str, scope):
    """Autoriza el acceso a Google Sheets usando el JSON de credenciales desde una cadena."""
    try:
        creds_dict = json.loads(credentials_json_str)
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        logging.info("✅ Autorización con Google Sheets exitosa.")
        return client
    except Exception as e:
        logging.error(f"❌ Error al autorizar con Google Sheets: {e}")
        raise


def cargar_y_procesar_ventas(carpeta_ventas):
    """Carga, concatena y preprocesa los archivos de ventas, devolviendo dos niveles de agregación."""
    logging.info(f"Cargando archivos de ventas desde: {carpeta_ventas}")
    try:
        # Asegúrate de que la carpeta exista antes de listar archivos
        if not os.path.exists(carpeta_ventas):
            logging.error(f"La carpeta de ventas '{carpeta_ventas}' no existe. Asegúrate de que los CSV estén en la ubicación correcta en el repositorio.")
            return pd.DataFrame(), pd.DataFrame()

        files = [os.path.join(carpeta_ventas, f) for f in os.listdir(carpeta_ventas) if f.endswith('.csv')]
        if not files:
            logging.error("No se encontraron archivos .csv en la carpeta especificada.")
            return pd.DataFrame(), pd.DataFrame()

        df = pd.concat((pd.read_csv(f, on_bad_lines='skip') for f in files), ignore_index=True)
    except Exception as e:
        logging.error(f"Error al leer los archivos CSV: {e}")
        return pd.DataFrame(), pd.DataFrame()

    df['Business Date'] = pd.to_datetime(df['Business Date'], errors='coerce')
    df.dropna(subset=['Business Date'], inplace=True)

    mask = (
            df['Major Group Name'].isin(GRUPOS_INCLUIDOS) &
            ~df['Order Type Name'].isin(ORDENES_EXCLUIDAS) &
            ~df['Family Group Name'].isin(FAMILIAS_EXCLUIDAS)
    )
    df_filtered = df[mask].copy()
    df_filtered['ds'] = df_filtered['Business Date'] - pd.to_timedelta(df_filtered['Business Date'].dt.dayofweek,
                                                                       unit='D')

    # Agregación a nivel de Family Group (para Prophet)
    df_family_weekly = (
        df_filtered.groupby(['ds', 'Major Group Name', 'Family Group Name'])
        ['Sales Count'].sum().reset_index().rename(columns={'Sales Count': 'Venta Real'})
    )
    logging.info("Ventas agregadas a nivel de Family Group.")

    # Agregación a nivel de Menu Item (para representatividad y reporte final)
    df_item_weekly = (
        df_filtered.groupby(['ds', 'Major Group Name', 'Family Group Name', 'Menu Item Number', 'Menu Item Name'])
        ['Sales Count'].sum().reset_index().rename(columns={'Sales Count': 'Venta Real'})
    )
    logging.info("Ventas agregadas a nivel de Menu Item.")

    return df_family_weekly, df_item_weekly


def calcular_representatividad(df_item_weekly):
    """Calcula el % de representatividad de cada item dentro de su Family Group."""
    logging.info(f"Calculando representatividad de las últimas {WEEKS_FOR_REPRESENTATIVENESS} semanas...")
    if df_item_weekly.empty:
        return pd.DataFrame()

    # Filtrar las últimas N semanas de datos
    last_date = df_item_weekly['ds'].max()
    start_date = last_date - pd.Timedelta(weeks=WEEKS_FOR_REPRESENTATIVENESS - 1)
    recent_sales = df_item_weekly[df_item_weekly['ds'] >= start_date]

    # Calcular ventas totales por item y por familia en el período
    item_sales = recent_sales.groupby(['Family Group Name', 'Menu Item Number', 'Menu Item Name'])[
        'Venta Real'].sum().reset_index()
    family_sales = recent_sales.groupby('Family Group Name')['Venta Real'].sum().reset_index().rename(
        columns={'Venta Real': 'Venta Total Familia'})

    # Unir y calcular el porcentaje
    df_rep = pd.merge(item_sales, family_sales, on='Family Group Name')
    df_rep['Representatividad_%'] = (df_rep['Venta Real'] / df_rep['Venta Total Familia']) * 100

    # Manejar división por cero si una familia no tuvo ventas
    df_rep.fillna({'Representatividad_%': 0}, inplace=True)

    return df_rep[['Family Group Name', 'Menu Item Number', 'Menu Item Name', 'Representatividad_%']]


def entrenar_y_pronosticar(df_model):
    """Itera sobre cada Family Group, entrena un modelo Prophet o usa un promedio simple si hay pocos datos."""
    logging.info("Iniciando ciclo de entrenamiento y pronóstico por Family Group...")
    all_forecasts = []

    for (major_group, family_group), group in df_model.groupby(['Major Group Name', 'Family Group Name']):
        sales_history = group[group['Venta Real'] > 0]
        num_sales_weeks = len(sales_history)

        if num_sales_weeks < MIN_WEEKS_FOR_PROPHET:
            if num_sales_weeks < 1:
                logging.warning(f"⚠️ Family Group {family_group} omitido, sin historial de ventas.")
                continue
            logging.info(f"🔹 Usando promedio de últimas 3 semanas para nuevo Family Group: {family_group}")
            demand_avg = np.round(sales_history['Venta Real'].tail(3).mean())
            last_date = group['ds'].max()
            future_dates = pd.date_range(start=last_date, periods=FORECAST_PERIOD_WEEKS + 1, freq='W-MON')[1:]
            df_out = pd.DataFrame({'Fecha': future_dates})
            df_out['Demanda'] = demand_avg
            df_out['Peor Escenario'] = demand_avg
            df_out['Escenario Promedio'] = demand_avg
            df_out['Mejor Escenario'] = demand_avg

        else:
            try:
                df_prophet = group[['ds', 'Venta Real']].rename(columns={'Venta Real': 'y'})
                promo_start_week = PROMO_START_DATE - pd.to_timedelta(PROMO_START_DATE.dayofweek, unit='D')

                if major_group == PROMO_CATEGORY:
                    df_prophet['Promo'] = df_prophet['ds'].apply(lambda d: 1 if d >= promo_start_week else 0)
                else:
                    df_prophet['Promo'] = 0

                max_sale = df_prophet['y'].max()
                cap_limit = max_sale * 1.5
                df_prophet['cap'] = cap_limit
                model = Prophet(growth='logistic', seasonality_mode='additive', yearly_seasonality=True,
                                weekly_seasonality=False, daily_seasonality=False, changepoint_prior_scale=0.05)
                model.add_regressor('Promo')
                model.fit(df_prophet)
                future = model.make_future_dataframe(periods=FORECAST_PERIOD_WEEKS, freq='W-MON')
                future['cap'] = cap_limit

                if major_group == PROMO_CATEGORY:
                    future['Promo'] = future['ds'].apply(lambda d: 1 if d >= promo_start_week else 0)
                else:
                    future['Promo'] = 0

                forecast = model.predict(future)
                df_out = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].rename(columns={'ds': 'Fecha'})

                # --- CAMBIO REALIZADO: La Demanda ahora es siempre el Mejor Escenario ---
                df_out['Demanda'] = np.maximum(0, df_out['yhat_upper']).round()

                df_out['Peor Escenario'] = np.maximum(0, df_out['yhat_lower']).round()
                df_out['Escenario Promedio'] = np.maximum(0, df_out['yhat']).round()
                df_out['Mejor Escenario'] = np.maximum(0, df_out['yhat_upper']).round()
                logging.info(f"✅ Pronóstico con Prophet generado para: {family_group}")
            except Exception as e:
                logging.error(f"❌ Falló el pronóstico con Prophet para {family_group}: {e}")
                continue

        df_out['Major Group Name'] = major_group
        df_out['Family Group Name'] = family_group
        all_forecasts.append(df_out)

    return pd.concat(all_forecasts, ignore_index=True) if all_forecasts else pd.DataFrame()


def exportar_resultados(df_forecast_family, df_item_hist, spreadsheet):
    """Desglosa el pronóstico de familia a item y lo exporta a Google Sheets."""
    if df_forecast_family.empty:
        logging.warning("⚠️ No hay datos de pronóstico para exportar.")
        return

    # 1. Calcular representatividad
    df_rep = calcular_representatividad(df_item_hist)
    if df_rep.empty:
        logging.warning("⚠️ No se pudo calcular la representatividad. No se puede desglosar el pronóstico.")
        return

    # 2. Desglosar el pronóstico de familia a item
    logging.info("Desglosando pronóstico de familia a item...")
    df_exploded = pd.merge(df_forecast_family, df_rep, on='Family Group Name', how='left')

    # Calcular la demanda a nivel de item para todos los escenarios
    df_exploded['Demanda'] = (df_exploded['Mejor Escenario'] * df_exploded['Representatividad_%'] / 100).round()

    # 3. Unir con ventas reales históricas a nivel de item
    df_export = pd.merge(
        df_exploded,
        df_item_hist[['ds', 'Menu Item Number', 'Venta Real']],
        left_on=['Fecha', 'Menu Item Number'],
        right_on=['ds', 'Menu Item Number'],
        how='left'
    ).drop(columns='ds')

    # 4. Formatear y exportar
    hoy = pd.Timestamp.today().normalize()
    inicio_rango = hoy - pd.Timedelta(weeks=HISTORY_PERIOD_WEEKS)
    fin_rango = hoy + pd.Timedelta(weeks=FORECAST_PERIOD_WEEKS)
    df_export = df_export[(df_export['Fecha'] >= inicio_rango) & (df_export['Fecha'] <= fin_rango)]

    # --- CAMBIO REALIZADO: Se ajusta el orden y la selección de columnas ---
    column_order = [
        'Fecha', 'Major Group Name', 'Family Group Name', 'Menu Item Number', 'Menu Item Name',
        'Demanda', 'Venta Real'
    ]
    df_export = df_export.reindex(columns=column_order)

    try:
        worksheet = spreadsheet.worksheet(OUTPUT_SHEET_NAME)
        worksheet.clear()
        set_with_dataframe(worksheet, df_export, include_index=False, allow_formulas=False)
        logging.info(f"✅ {len(df_export)} filas exportadas correctamente a la hoja '{OUTPUT_SHEET_NAME}'.")
    except gspread.exceptions.WorksheetNotFound:
        logging.warning(f"La hoja '{OUTPUT_SHEET_NAME}' no existe. Creándola...")
        worksheet = spreadsheet.add_worksheet(title=OUTPUT_SHEET_NAME, rows=len(df_export) + 1, cols=len(column_order))
        set_with_dataframe(worksheet, df_export, include_index=False, allow_formulas=False)
        logging.info(f"✅ Hoja creada y datos exportados.")
    except Exception as e:
        logging.error(f"❌ Error al exportar a Google Sheets: {e}")


# =============================================================================
# ------------------------------ EJECUCIÓN PRINCIPAL --------------------------
# =============================================================================

def main():
    """Función principal que orquesta todo el proceso."""
    logging.info("🚀 Iniciando el proceso de pronóstico de demanda semanal.")

    # ¡CAMBIO AQUÍ! Obtener credenciales de Google Sheets desde la variable de entorno
    google_credentials_json = os.environ.get('GOOGLE_CREDENTIALS')
    if not google_credentials_json:
        logging.error("❌ La variable de entorno GOOGLE_CREDENTIALS no está configurada.")
        logging.error("Asegúrate de haber añadido el secreto GOOGLE_CREDENTIALS en tu repositorio de GitHub.")
        return

    try:
        client = autorizar_gsheets_from_env(google_credentials_json, SCOPE_GOOGLE)
        spreadsheet = client.open(SPREADSHEET_NAME)
    except Exception as e:
        logging.error(f"❌ No se pudo autorizar Google Sheets. Error: {e}")
        return

    df_family_weekly, df_item_weekly = cargar_y_procesar_ventas(CARPETA_VENTAS)

    if df_family_weekly.empty:
        logging.error("El proceso no puede continuar sin datos de ventas.")
        return

    df_forecasts = entrenar_y_pronosticar(df_family_weekly)

    exportar_resultados(df_forecasts, df_item_weekly, spreadsheet)

    logging.info("🏁 Proceso de pronóstico de demanda finalizado.")


if __name__ == "__main__":
    main()
