from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import json

def create_producer(ip_port):
    """
    Crea un productor de Kafka.
    
    :param ip_port: Dirección IP y puerto del servidor Kafka en formato 'IP:PORT'
    :return: Instancia de KafkaProducer
    """
    try:
        producer = KafkaProducer(
            bootstrap_servers=[ip_port],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        return producer
    except KafkaError as e:
        print(f"Error al crear el productor: {e}")
        return None

def send_kafka(ip_port, topic, message):
    """
    Envía un mensaje a un tópico de Kafka.
    
    :param ip_port: Dirección IP y puerto del servidor Kafka en formato 'IP:PORT'
    :param topic: Nombre del tópico
    :param message: Mensaje a enviar
    """
    producer = create_producer(ip_port)
    if not producer:
        return

    try:
        future = producer.send(topic, message)
        result = future.get(timeout=10)
        producer.flush()  # Asegura que todos los mensajes en el buffer se envíen
    except KafkaError as e:
        print(f"Error al enviar el mensaje: {e}")
    finally:
        producer.close()

def create_consumer_pos(ip_port, topic, grupo):
    """
    Crea un consumidor de Kafka.
    
    :param ip_port: Dirección IP y puerto del servidor Kafka en formato 'IP:PORT'
    :param topic: Nombre del tópico
    :param group_id: ID del grupo de consumidores (opcional)
    :return: Instancia de KafkaConsumer
    """
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[ip_port],
            enable_auto_commit=True,
            group_id=grupo
        )
        return consumer
    except KafkaError as e:
        print(f"Error al crear el consumidor: {e}")
        return None
    
    
def create_consumer(ip_port, topic):
    """
    Crea un consumidor de Kafka.
    
    :param ip_port: Dirección IP y puerto del servidor Kafka en formato 'IP:PORT'
    :param topic: Nombre del tópico
    :param group_id: ID del grupo de consumidores (opcional)
    :return: Instancia de KafkaConsumer
    """
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[ip_port],
            enable_auto_commit=True
        )
        return consumer
    except KafkaError as e:
        print(f"Error al crear el consumidor: {e}")
        return None
    
def receive_kafka(ip_port, topic):
    """
    Recibe mensajes de un tópico de Kafka.
    
    :param ip_port: Dirección IP y puerto del servidor Kafka en formato 'IP:PORT'
    :param topic: Nombre del tópico
    :param group_id: ID del grupo de consumidores (opcional)
    """
    consumer = create_consumer(ip_port, topic)

    if not consumer:
        return
    
    try:
        for message in consumer:
            return message.value
    except KafkaError as e:
        print(f"Error al recibir mensajes: {e}")
    finally:
        consumer.close()


def receive_kafka_pos(ip_port, topic, group_id):
    """
    Recibe mensajes de un tópico de Kafka.
    
    :param ip_port: Dirección IP y puerto del servidor Kafka en formato 'IP:PORT'
    :param topic: Nombre del tópico
    :param group_id: ID del grupo de consumidores (opcional)
    """
    consumer = create_consumer_pos(ip_port, topic, group_id)

    if not consumer:
        return
    
    try:
        for message in consumer:
            return message.value
    except KafkaError as e:
        print(f"Error al recibir mensajes: {e}")
    finally:
        consumer.close()
