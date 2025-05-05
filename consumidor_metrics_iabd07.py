import json
import time
from kafka import KafkaConsumer
from pymongo import MongoClient
from datetime import datetime

# Configuración de MongoDB Atlas
MONGO_URI = "mongodb+srv://IABD07:Yogui6861@cluster0.wxly5.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
DB_NAME = "big_data"
RAW_COLLECTION = "system_metrics_raw_iabd07"
KPI_COLLECTION = "system_metrics_kpis_iabd07"

# Conectar a MongoDB Atlas
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
raw_collection = db[RAW_COLLECTION]
kpi_collection = db[KPI_COLLECTION]

# Configuración Kafka
KAFKA_BROKER = "10.32.24.128:29092" 
KAFKA_TOPIC = "system-metrics-topic-iabd07"
GROUP_ID = "grupo_iabd07_id"

# Inicializar el consumidor
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    group_id=GROUP_ID,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# --- Función para calcular KPIs ---
def calcular_kpis(messages):
    # Convertir las timestamps de las métricas a datetime para poder hacer la resta
    metrics = messages  # La lista de métricas recibidas

    # Asegurarse de que las timestamps sean datetime
    timestamps = [datetime.fromisoformat(metric['timestamp_utc']) for metric in metrics]

    # Calcular los KPIs
    if len(metrics) > 1:
        # Promedio de cpu_percent
        avg_cpu = sum([metric['metrics']['cpu_percent'] for metric in metrics]) / len(metrics)
        # Promedio de memory_percent
        avg_memory = sum([metric['metrics']['memory_percent'] for metric in metrics]) / len(metrics)
        # Promedio de disk_io_mbps
        avg_disk_io = sum([metric['metrics']['disk_io_mbps'] for metric in metrics]) / len(metrics)
        # Promedio de network_mbps
        avg_network = sum([metric['metrics']['network_mbps'] for metric in metrics]) / len(metrics)
        # Suma de error_count
        total_errors = sum([metric['metrics']['error_count'] for metric in metrics])

        # Tasa de procesamiento (mensajes por segundo)
        duration = (timestamps[-1] - timestamps[0]).total_seconds()  # Diferencia de tiempo entre los primeros y últimos timestamp
        rate = len(metrics) / duration if duration > 0 else 0
    else:
        # Si solo hay un mensaje, establecer valores predeterminados
        avg_cpu = metrics[0]['metrics']['cpu_percent']
        avg_memory = metrics[0]['metrics']['memory_percent']
        avg_disk_io = metrics[0]['metrics']['disk_io_mbps']
        avg_network = metrics[0]['metrics']['network_mbps']
        total_errors = metrics[0]['metrics']['error_count']
        rate = 0

    # Construir el documento de KPI
    kpi_document = {
        "timestamp_utc": datetime.now().isoformat(),
        "duration_seconds": duration,
        "message_count": len(metrics),
        "average_cpu_percent": avg_cpu,
        "average_memory_percent": avg_memory,
        "average_disk_io_mbps": avg_disk_io,
        "average_network_mbps": avg_network,
        "total_error_count": total_errors,
        "rate_messages_per_second": rate
    }

    return kpi_document

# --- Consumidor Kafka ---
def procesar_mensajes():
    # Crear el consumidor de Kafka
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        group_id=GROUP_ID,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    messages = []

    print("Iniciando consumidor...")

    for message in consumer:
        # Agregar el mensaje a la lista
        messages.append(message.value)

        # Al recibir N=20 mensajes, calcular y almacenar los KPIs
        if len(messages) == 20:
            kpis = calcular_kpis(messages)

            # Insertar KPIs en la base de datos MongoDB
            kpi_collection.insert_one(kpis)
            print("KPIs calculados y almacenados:", kpis)

            # Limpiar la lista de mensajes para procesar los siguientes
            messages = []

        # Almacenar las métricas brutas en MongoDB
        raw_collection.insert_one(message.value)
        print(f"Métrica recibida y almacenada en raw: {message.value['server_id']}")

if __name__ == "__main__":
    procesar_mensajes()