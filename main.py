import logging
import sys
import os

# --- Configuración de Logging ---
# Asegura que los mensajes se muestren en la consola.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

# --- Añadir las carpetas del proyecto al path de Python ---
# Esto permite que main.py encuentre e importe los módulos en las subcarpetas.
try:
    project_root = os.path.dirname(os.path.abspath(__file__))
    sys.path.append(project_root)
    from generadores.generar_holidays import main as main_holidays
    from generadores.generar_clima import main as main_clima
    from generadores.generar_promociones import main as main_promociones
    from modelo.pronostico_demanda import main as main_forecast
except ImportError as e:
    logging.critical(f"❌ Error de importación. Asegúrate de que la estructura de carpetas es correcta y que los archivos __init__.py existen. Error: {e}")
    sys.exit(1)


def run_pipeline():
    """
    Ejecuta el pipeline completo de generación de datos y pronóstico en el orden correcto.
    """
    try:
        logging.info("======================================================================")
        logging.info("--- PASO 1/4: Iniciando generación de la tabla de Feriados y Eventos ---")
        main_holidays()
        logging.info("--- PASO 1/4: Tabla de Feriados y Eventos generada exitosamente ---\n")

        logging.info("======================================================================")
        logging.info("--- PASO 2/4: Iniciando generación de la tabla de Clima ---")
        main_clima()
        logging.info("--- PASO 2/4: Tabla de Clima generada exitosamente ---\n")

        logging.info("======================================================================")
        logging.info("--- PASO 3/4: Iniciando generación de la tabla de Promociones ---")
        main_promociones()
        logging.info("--- PASO 3/4: Tabla de Promociones generada exitosamente ---\n")

        logging.info("======================================================================")
        logging.info("--- PASO 4/4: Iniciando rutina de Pronóstico de Demanda ---")
        main_forecast()
        logging.info("--- PASO 4/4: Rutina de Pronóstico de Demanda finalizada exitosamente ---\n")

        logging.info("✅✅✅ PIPELINE COMPLETADO EXITOSAMENTE ✅✅✅")

    except Exception as e:
        logging.critical(f"❌❌❌ El pipeline falló en un paso crítico: {e}", exc_info=True)

if __name__ == "__main__":
    run_pipeline()
