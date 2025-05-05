import random
import time
from datetime import datetime, timezone
import uuid
import json
from kafka import KafkaProducer

# --- Configuración ---
KAFKA_BROKER = "10.32.24.128:29092"  # Dirección del broker Kafka
KAFKA_TOPIC = "system-metrics-topic-iabd07"  # Nombre del topic en Kafka
REPORTING_INTERVAL_SECONDS = 10  # Tiempo entre reportes completos de todos los servers
SERVER_IDS = ["web01", "web02", "db01", "app01", "cache01"]

# Conexión al Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,  # Dirección del broker Kafka
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serializador JSON
)

# --- Lógica de Generación de Métricas ---
if __name__ == "__main__":
    print("Iniciando simulación de generación de métricas...")
    print(f"Servidores simulados: {SERVER_IDS}")
    print(f"Intervalo de reporte: {REPORTING_INTERVAL_SECONDS} segundos")
    print("-" * 30)

    try:
        while True:
            print(f"\n{datetime.now()}: Generando reporte de métricas...")

            # Iterar sobre cada servidor para generar sus métricas
            for server_id in SERVER_IDS:
                # Generar métricas simuladas con fluctuaciones
                cpu_percent = random.uniform(5.0, 75.0)

                # Añadir un pico ocasional de CPU
                if random.random() < 0.1:  # 10% de probabilidad
                    cpu_percent = random.uniform(85.0, 98.0)

                memory_percent = random.uniform(20.0, 85.0)

                # Añadir un pico ocasional de memoria
                if random.random() < 0.05:  # 5% de probabilidad
                    memory_percent = random.uniform(90.0, 99.0)

                disk_io_mbps = random.uniform(0.1, 50.0)
                network_mbps = random.uniform(1.0, 100.0)

                # Errores deben ser poco frecuentes
                error_count = 0
                if random.random() < 0.08:  # 8% probabilidad de tener algún error
                    error_count = random.randint(1, 3)

                # Crear el diccionario del mensaje de métricas
                metric_message = {
                    "server_id": server_id,
                    "timestamp_utc": datetime.now(timezone.utc).isoformat(),  # Usar timezone.utc
                    "metrics": {
                        "cpu_percent": round(cpu_percent, 2),
                        "memory_percent": round(memory_percent, 2),
                        "disk_io_mbps": round(disk_io_mbps, 2),
                        "network_mbps": round(network_mbps, 2),
                        "error_count": error_count
                    },
                    "message_uuid": str(uuid.uuid4())  # Identificador único del mensaje
                }

                # Enviar el mensaje de métricas a Kafka
                producer.send(KAFKA_TOPIC, value=metric_message)

                # Imprimir para ver que la métrica ha sido generada y enviada
                print(f"Generado para {server_id}:")
                print(f" CPU: {metric_message['metrics']['cpu_percent']}%")
                print(f" Mem: {metric_message['metrics']['memory_percent']}%")
                print(f" Disk: {metric_message['metrics']['disk_io_mbps']} MB/s")
                print(f" Net: {metric_message['metrics']['network_mbps']} Mbps")
                print(f" Errors: {metric_message['metrics']['error_count']}")
                print("-" * 10)

            # Esperar antes de generar el siguiente reporte completo
            print(f"\nReporte completo generado. Esperando {REPORTING_INTERVAL_SECONDS} segundos...")
            time.sleep(REPORTING_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print("\nSimulación detenida por el usuario.")

    finally:
        # Cerrar la conexión con Kafka cuando se detenga la simulación
        producer.close()

# --- Fin de la Lógica de Generación ---
