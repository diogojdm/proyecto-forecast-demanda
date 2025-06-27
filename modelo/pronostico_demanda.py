import os
import pandas as pd
import numpy as np
from prophet import Prophet
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import logging
import matplotlib.pyplot as plt
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
OUTPUT_SHEET_NAME = "Demanda Diaria por Tienda"
HOLIDAYS_SHEET_NAME = "Holidays"
TEMP_SHEET_NAME = "TempHistorico"
PROMO_SHEET_NAME = "Promociones" 

# --- Archivos de Ventas ---
CARPETA_VENTAS = r"G:\Mi unidad\Proyecto_Data\4. Data_base\1. Ventas"

# --- Parámetros del Modelo y Fechas ---
FORECAST_PERIOD_DAYS = 14
HISTORY_PERIOD_DAYS = 14

# --- Umbral para pronóstico simplificado ---
MIN_DAYS_FOR_PROPHET = 30
DAYS_FOR_REPRESENTATIVENESS = 28

# --- Filtros de Datos ---
GRUPOS_INCLUIDOS = ["Delicias", "Pastel Grande", "Pastel Mediano", "Pastel Trozo"]
ORDENES_EXCLUIDAS = ['Good Meal']
FAMILIas_EXCLUIDAS = ['Gift Box']


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
            creds = ServiceAccountCredentials.from_json_keyfile_name(RUTA_CREDENCIALES, SCOPE_GOOGLE)

        client = gspread.authorize(creds)
        logging.info("✅ Autorización con Google Sheets exitosa.")
        return client
    except Exception as e:
        logging.error(f"❌ Error al autorizar con Google Sheets: {e}")
        raise
        
def cargar_y_procesar_ventas(carpeta_ventas):
    """Carga, concatena y preprocesa los archivos de ventas a nivel diario."""
    logging.info(f"Cargando archivos de ventas desde: {carpeta_ventas}")
    try:
        files = [os.path.join(carpeta_ventas, f) for f in os.listdir(carpeta_ventas) if f.endswith('.csv')]
        if not files:
            logging.error("No se encontraron archivos .csv en la carpeta especificada.")
            return pd.DataFrame(), pd.DataFrame()

        df = pd.concat((pd.read_csv(f, on_bad_lines='skip') for f in files), ignore_index=True)
    except Exception as e:
        logging.error(f"Error al leer los archivos CSV: {e}")
        return pd.DataFrame(), pd.DataFrame()

    df['Business Date'] = pd.to_datetime(df['Business Date'], errors='coerce')
    df.dropna(subset=['Business Date', 'Location Name', 'Family Group Name', 'Menu Item Number'], inplace=True)
    
    mask = (
        df['Major Group Name'].isin(GRUPOS_INCLUIDOS) & 
        ~df['Order Type Name'].isin(ORDENES_EXCLUIDAS) &
        ~df['Family Group Name'].isin(FAMILIas_EXCLUIDAS)
    )
    df_filtered = df[mask].copy()
    
    df_filtered['ds'] = df_filtered['Business Date']

    df_location_family_daily = (
        df_filtered.groupby(['ds', 'Location Name', 'Major Group Name', 'Family Group Name'])
        ['Sales Count'].sum().reset_index().rename(columns={'Sales Count': 'Venta Real'})
    )
    logging.info("Ventas agregadas a nivel diario por Tienda y Family Group.")

    df_location_item_daily = (
        df_filtered.groupby(['ds', 'Location Name', 'Major Group Name', 'Family Group Name', 'Menu Item Number', 'Menu Item Name'])
        ['Sales Count'].sum().reset_index().rename(columns={'Sales Count': 'Venta Real'})
    )
    logging.info("Ventas agregadas a nivel diario por Tienda y Menu Item.")
    
    return df_location_family_daily, df_location_item_daily

def cargar_regresores_externos(spreadsheet):
    """Carga y combina las tablas de feriados, clima y promociones."""
    logging.info("Cargando variables externas (feriados, clima, promociones)...")
    
    df_regressors_list = []
    
    # Cargar Feriados, Días de Pago y Vacaciones
    try:
        holidays_ws = spreadsheet.worksheet(HOLIDAYS_SHEET_NAME)
        df_holidays = pd.DataFrame(holidays_ws.get_all_records())
        df_holidays['ds'] = pd.to_datetime(df_holidays['fecha'])
        df_regressors_list.append(df_holidays.drop(columns='fecha'))
    except Exception as e:
        logging.error(f"❌ No se pudo cargar la hoja '{HOLIDAYS_SHEET_NAME}': {e}")

    # Cargar Clima
    try:
        temp_ws = spreadsheet.worksheet(TEMP_SHEET_NAME)
        df_weather = pd.DataFrame(temp_ws.get_all_records())
        df_weather['ds'] = pd.to_datetime(df_weather['fecha'])
        df_regressors_list.append(df_weather.drop(columns='fecha'))
    except Exception as e:
        logging.error(f"❌ No se pudo cargar la hoja '{TEMP_SHEET_NAME}': {e}")
        
    # Cargar Promociones
    try:
        promo_ws = spreadsheet.worksheet(PROMO_SHEET_NAME)
        df_promos = pd.DataFrame(promo_ws.get_all_records())
        df_promos['ds'] = pd.to_datetime(df_promos['fecha'])
        df_regressors_list.append(df_promos.drop(columns='fecha'))
    except Exception as e:
        logging.error(f"❌ No se pudo cargar la hoja '{PROMO_SHEET_NAME}': {e}")
        
    if not df_regressors_list:
        logging.warning("⚠️ No se cargó ninguna tabla de variables externas.")
        return pd.DataFrame(), []
    
    from functools import reduce
    df_regressors = reduce(lambda left, right: pd.merge(left, right, on='ds', how='outer'), df_regressors_list)

    # Excluir columnas que no son regresores directos del modelo
    cols_to_exclude = ['ds', 'temperatura_max_c']
    regressor_cols = [col for col in df_regressors.columns if col not in cols_to_exclude]
    logging.info(f"✅ Regresores externos cargados: {regressor_cols}")
    
    return df_regressors, regressor_cols


def calcular_representatividad(df_location_item_daily):
    """Calcula el % de representatividad de cada item dentro de su Family Group, POR TIENDA."""
    logging.info(f"Calculando representatividad de los últimos {DAYS_FOR_REPRESENTATIVENESS} días por tienda...")
    if df_location_item_daily.empty:
        return pd.DataFrame()

    last_date = df_location_item_daily['ds'].max()
    start_date = last_date - pd.Timedelta(days=DAYS_FOR_REPRESENTATIVENESS - 1)
    recent_sales = df_location_item_daily[df_location_item_daily['ds'] >= start_date]

    item_sales = recent_sales.groupby(['Location Name', 'Family Group Name', 'Menu Item Number', 'Menu Item Name'])['Venta Real'].sum().reset_index()
    family_sales = recent_sales.groupby(['Location Name', 'Family Group Name'])['Venta Real'].sum().reset_index().rename(columns={'Venta Real': 'Venta Total Familia'})

    df_rep = pd.merge(item_sales, family_sales, on=['Location Name', 'Family Group Name'])
    df_rep['Representatividad_%'] = (df_rep['Venta Real'] / df_rep['Venta Total Familia']) * 100
    df_rep.fillna({'Representatividad_%': 0}, inplace=True)
    
    return df_rep[['Location Name', 'Family Group Name', 'Menu Item Number', 'Menu Item Name', 'Representatividad_%']]

def entrenar_y_pronosticar(df_model, df_regressors, regressor_cols):
    """Itera sobre cada combinación de Tienda-Familia, entrena un modelo Prophet o usa un promedio simple."""
    logging.info("Iniciando ciclo de entrenamiento y pronóstico diario por Tienda y Familia...")
    all_forecasts = []
    
    # Crear carpeta para guardar los gráficos
    plots_dir = 'plots'
    if not os.path.exists(plots_dir):
        os.makedirs(plots_dir)
    
    for (location, major_group, family_group), group in df_model.groupby(['Location Name', 'Major Group Name', 'Family Group Name']):
        sales_history = group[group['Venta Real'] > 0]
        num_sales_days = len(sales_history)

        if num_sales_days < MIN_DAYS_FOR_PROPHET:
            if num_sales_days < 1:
                logging.warning(f"⚠️ Combinación '{location} - {family_group}' omitida, sin historial.")
                continue
            logging.info(f"🔹 Usando promedio para '{location} - {family_group}' (poca data)")
            demand_avg = np.round(sales_history['Venta Real'].tail(7).mean())
            last_date = group['ds'].max()
            future_dates = pd.date_range(start=last_date, periods=FORECAST_PERIOD_DAYS + 1, freq='D')[1:]
            df_out = pd.DataFrame({'Fecha': future_dates})
            df_out['Demanda'] = demand_avg
            df_out['Peor Escenario'] = demand_avg
            df_out['Escenario Promedio'] = demand_avg
            df_out['Mejor Escenario'] = demand_avg
        
        else:
            try:
                df_prophet = group[['ds', 'Venta Real']].rename(columns={'Venta Real': 'y'})
                
                df_prophet = pd.merge(df_prophet, df_regressors, on='ds', how='left')
                df_prophet[regressor_cols] = df_prophet[regressor_cols].fillna(0)
                
                if 'fuerza_promo_pastel_trozo' in df_prophet.columns and major_group != 'Pastel Trozo':
                    df_prophet['fuerza_promo_pastel_trozo'] = 0

                max_sale = df_prophet['y'].max()
                cap_limit = max_sale * 2.5
                df_prophet['cap'] = cap_limit
                
                model = Prophet(growth='logistic', 
                                seasonality_mode='additive', 
                                yearly_seasonality=True,
                                weekly_seasonality=True, 
                                daily_seasonality=True, 
                                changepoint_prior_scale=0.05)
                
                for regressor in regressor_cols:
                    model.add_regressor(regressor)
                
                model.fit(df_prophet)
                future = model.make_future_dataframe(periods=FORECAST_PERIOD_DAYS, freq='D')
                future['cap'] = cap_limit
                
                future = pd.merge(future, df_regressors, on='ds', how='left')
                future[regressor_cols] = future[regressor_cols].fillna(0)
                
                if 'fuerza_promo_pastel_trozo' in future.columns and major_group != 'Pastel Trozo':
                    future['fuerza_promo_pastel_trozo'] = 0

                forecast = model.predict(future)
                
                # --- CORRECCIÓN DE LÓGICA ---
                # Se eliminó la lógica condicional de aquí. Ahora solo se calculan los 3 escenarios.
                df_out = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].rename(columns={'ds': 'Fecha'})
                
                df_out['Peor Escenario'] = np.maximum(0, df_out['yhat_lower']).round()
                df_out['Escenario Promedio'] = np.maximum(0, df_out['yhat']).round()
                df_out['Mejor Escenario'] = np.maximum(0, df_out['yhat_upper']).round()

                logging.info(f"✅ Pronóstico con Prophet generado para: '{location} - {family_group}'")

                try:
                    fig = model.plot_components(forecast)
                    safe_location = "".join(c for c in location if c.isalnum() or c in (' ', '_')).rstrip()
                    safe_family = "".join(c for c in family_group if c.isalnum() or c in (' ', '_')).rstrip()
                    plot_filename = os.path.join('plots', f"componentes_{safe_location}_{safe_family}.png".replace(" ", "_"))
                    fig.savefig(plot_filename)
                    plt.close(fig)
                    logging.info(f"📈 Gráfico de componentes guardado en: {plot_filename}")
                except Exception as plot_e:
                    logging.error(f"❌ No se pudo generar el gráfico de componentes para '{location} - {family_group}': {plot_e}")

            except Exception as e:
                logging.error(f"❌ Falló el pronóstico para '{location} - {family_group}': {e}")
                continue
        
        df_out['Location Name'] = location
        df_out['Family Group Name'] = family_group
        df_out['Major Group Name'] = group['Major Group Name'].iloc[0]
        all_forecasts.append(df_out)

    return pd.concat(all_forecasts, ignore_index=True) if all_forecasts else pd.DataFrame()

def exportar_resultados(df_forecast_family, df_item_hist, spreadsheet):
    """Desglosa el pronóstico de familia a item por tienda y lo exporta."""
    if df_forecast_family.empty:
        logging.warning("⚠️ No hay datos de pronóstico para exportar.")
        return

    df_rep = calcular_representatividad(df_item_hist)
    if df_rep.empty:
        logging.warning("⚠️ No se pudo calcular la representatividad.")
        return

    logging.info("Desglosando pronóstico de familia a item por tienda...")
    df_exploded = pd.merge(df_forecast_family, df_rep, on=['Location Name', 'Family Group Name'], how='left')
    
    # Desglosar todos los escenarios primero
    df_exploded['Peor Escenario'] = (df_exploded['Peor Escenario'] * df_exploded['Representatividad_%'] / 100).round()
    df_exploded['Escenario Promedio'] = (df_exploded['Escenario Promedio'] * df_exploded['Representatividad_%'] / 100).round()
    df_exploded['Mejor Escenario'] = (df_exploded['Mejor Escenario'] * df_exploded['Representatividad_%'] / 100).round()

    # --- CORRECCIÓN DE LÓGICA: Aplicar la lógica condicional al final, sobre los datos desglosados ---
    df_exploded['Demanda'] = np.where(
        df_exploded['Fecha'].dt.weekday <= 3, # Lunes (0) a Jueves (3)
        df_exploded['Escenario Promedio'],
        df_exploded['Mejor Escenario']
    )
    
    df_export = pd.merge(
        df_exploded, 
        df_item_hist[['ds', 'Location Name', 'Menu Item Number', 'Venta Real']],
        left_on=['Fecha', 'Location Name', 'Menu Item Number'],
        right_on=['ds', 'Location Name', 'Menu Item Number'],
        how='left'
    ).drop(columns='ds')

    hoy = pd.Timestamp.today().normalize()
    inicio_rango = hoy - pd.Timedelta(days=HISTORY_PERIOD_DAYS)
    fin_rango = hoy + pd.Timedelta(days=FORECAST_PERIOD_DAYS)
    df_export = df_export[(df_export['Fecha'] >= inicio_rango) & (df_export['Fecha'] <= fin_rango)]

    column_order = [
        'Fecha', 'Location Name', 'Major Group Name', 'Family Group Name', 'Menu Item Number', 'Menu Item Name',
        'Demanda', 'Venta Real', 'Peor Escenario', 'Escenario Promedio', 'Mejor Escenario'
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
    logging.info("🚀 Iniciando el proceso de pronóstico de demanda diaria por tienda y familia.")

    client = autorizar_gsheets()
    spreadsheet = client.open(SPREADSHEET_NAME)
    
    df_location_family_daily, df_location_item_daily = cargar_y_procesar_ventas(CARPETA_VENTAS)
    df_regressors, regressor_cols = cargar_regresores_externos(spreadsheet)

    if df_location_family_daily.empty:
        logging.error("El proceso no puede continuar sin datos de ventas.")
        return
    
    if df_regressors.empty:
        logging.warning("⚠️ No se cargaron datos de regresores externos. El pronóstico no los considerará.")

    df_forecasts = entrenar_y_pronosticar(df_location_family_daily, df_regressors, regressor_cols)

    exportar_resultados(df_forecasts, df_location_item_daily, spreadsheet)

    logging.info("🏁 Proceso de pronóstico de demanda finalizado.")

if __name__ == "__main__":
    main()
