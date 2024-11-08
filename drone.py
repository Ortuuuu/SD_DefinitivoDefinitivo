from kafka import KafkaProducer, KafkaConsumer
import json
import sys
import time
from AD_aux import *
import socket
import getpass
import ssl
import hashlib
import requests
import threading
import random

CERT_FILE = 'certs/certificado.crt'
IP_ENGINE = sys.argv[1]
PORT_ENGINE = int(sys.argv[2])
IP_BROKER = sys.argv[3]
PORT_BROKER = int(sys.argv[4])
DIRECCION_KAFKA = f"{IP_BROKER}:{PORT_BROKER}"
IP_REGISTRY = sys.argv[5]
PORT_REGISTRY = int(sys.argv[6])
DRONE_ID = None
FORMAT = 'utf-8'
DRON_FILE = "engineDDBB.json"

def figuras(dron, idDron):
    print("Soy el dron:", dron["alias"], "esperando coordenadas...")
    try:
        fig = receive_kafka(DIRECCION_KAFKA, 'figura')
        fig = fig.decode('utf-8')
        fig = json.loads(fig)
        print(fig["Nombre"])

        for pos in fig["Drones"]:
            positions = pos["POS"].split(",")
            if pos["ID"] == int(idDron):
                print("Soy el drone:", dron["alias"], "y me muevo a la posición:", positions)
                if receive_kafka(DIRECCION_KAFKA, 'comenzar'):
                    print("Comenzando movimiento del dron", dron["alias"])
                    move_dron(dron, positions)
    except Exception as e:
        print(f"Error en figuras: {e}")

def move_dron_inicial(dron, posiciones):
    posiciones = int(posiciones[0]), int(posiciones[1])

    while 0 != dron["x"] or 0 != dron["y"]:
        try:
            if posiciones[1] < dron["y"]:
                dron["y"] -= 1
            elif posiciones[0] < dron["x"]:
                dron["x"] -= 1
            elif posiciones[0] > dron["x"]:
                dron["x"] += 1
            elif posiciones[1] > dron["y"]:
                dron["y"] += 1
            print("Dron:", dron["alias"], "en la posición:", dron["x"], ",", dron["y"])
            time.sleep(1)
            send_kafka(DIRECCION_KAFKA, 'posiciones', str(dron["id"]) + "," + str(dron["x"]) + "," + str(dron["y"]))
        except Exception as e:
            print(f"Error en move_dron_inicial: {e}")
            break
    print("Dron:", dron["alias"], "ha vuelto a la base")
    if receive_kafka(DIRECCION_KAFKA, 'siguiente'):
        preparado(dron)

def move_dron(dron, posiciones):
    posiciones = int(posiciones[0]), int(posiciones[1])

    while posiciones[0] != dron["x"] or posiciones[1] != dron["y"]:
        try:
            if posiciones[0] > dron["x"]:
                dron["x"] += 1
            elif posiciones[0] < dron["x"]:
                dron["x"] -= 1
            elif posiciones[1] > dron["y"]:
                dron["y"] += 1
            elif posiciones[1] < dron["y"]:
                dron["y"] -= 1
            print("Dron:", dron["alias"], "en la posición:", dron["x"], ",", dron["y"])
            time.sleep(1)
            send_kafka(DIRECCION_KAFKA, 'posiciones', str(dron["id"]) + "," + str(dron["x"]) + "," + str(dron["y"]))
        except Exception as e:
            print(f"Error en move_dron: {e}")
            break
    print("Dron:", dron["alias"], "ha llegado a su destino")
    send_kafka(DIRECCION_KAFKA, 'posiciones', str(dron["id"]) + "," + str(99) + "," + str(99))

    try:
        opt = receive_kafka(DIRECCION_KAFKA, 'volver')
        opt = opt.decode('utf-8')
        opt = opt.replace('"', '')

        if opt == "s" or opt == "S":
            print("Volver a la posición inicial")
            move_dron_inicial(dron, [0, 0])
        elif opt == "n" or opt == "N":
            print("Realizando siguiente figura directamente")
            preparado(dron)
        else:
            return
    except Exception as e:
        print(f"Error al recibir opción de volver: {e}")

def preparado(dron):
    print("Ready" + str(dron["id"]))
    time.sleep(int(dron["id"]))
    print(int(dron["id"]))
    send_kafka(DIRECCION_KAFKA, 'confirmar', "Ready" + str(dron["id"]))

    try:
        figuras(dron, dron["id"])
    except Exception as e:
        print(f"Error en preparado: {e}")

def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

def registrar_dron_via_socket():
    context = ssl.create_default_context()
    context.load_verify_locations(cafile=CERT_FILE)
    context.check_hostname = False

    alias = input("Ingrese un alias para su dron: ")
    password = getpass.getpass("Ingrese una contraseña para su dron: ")
    hashed_password = hash_password(password)

    datos_dron = {
        'alias': alias,
        'password': hashed_password
    }

    try:
        with socket.create_connection((IP_REGISTRY, PORT_REGISTRY)) as sock:
            with context.wrap_socket(sock, server_hostname=IP_REGISTRY) as ssock:
                try:
                    ssock.send(json.dumps(datos_dron).encode())
                    respuesta = ssock.recv(1024).decode()
                    datos_registro = json.loads(respuesta)
                    print(f"Dron registrado con ID: {datos_registro['id']} y Alias: {datos_registro['alias']}")
                    return datos_registro
                except Exception as e:
                    print(f"Error durante el registro: {e}")
                    main()
    except Exception as ex:
        print("El servidor de registro no se encuentra disponible en estos momentos. Inténtelo de nuevo más tarde.")
        print(f"Exception: {ex}")
        main()

def solicitar_nuevo_token_via_rest():
    alias = input("Ingrese su alias: ")
    password = getpass.getpass("Ingrese su contraseña: ")
    hashed_password = hash_password(password)
    url = f"https://{IP_REGISTRY}:{PORT_REGISTRY+1}/autenticar"

    datos = {
        "alias": alias,
        "password": hashed_password
    }

    try:
        response = requests.post(url, json=datos, verify=False)
        print("Respuesta del servidor:", response.json())
        main()
    except Exception as e:
        print(f"Error solicitando nuevo token: {e}")
        main()

def validar_dron_con_engine():
    alias = input("Ingrese el alias del dron: ")
    password = getpass.getpass("Ingtese la contraseña del dron: ")
    hashed_password = hash_password(password)
    token = getToken(alias, hashed_password)

    try:
        with socket.create_connection((IP_ENGINE, PORT_ENGINE + 5)) as sock:
            if token == None:
                print("No existe token del dron, solicite uno nuevo")
                main()
            else:
                sock.send(token.encode(FORMAT))
                respuesta = sock.recv(1024).decode(FORMAT)

                if respuesta == "Validado":
                    print("Dron validado y listo para iniciar vuelo.")
                    return True
                else:
                    print("Validación fallida: Token no válido.")
                    return False

    except Exception as e:
        print(f"Error al validar el dron con el engine: {e}")
        return False

def enviar_posiciones():
    global DRONE_ID
    producer = KafkaProducer(bootstrap_servers=f'{IP_BROKER}:{PORT_BROKER}')
    topic = 'posiciones'

    while True:
        try:
            x = input("Ingrese la coordenada X: ")
            y = input("Ingrese la coordenada Y: ")
            posicion = f'{DRONE_ID},{x},{y}'
            future = producer.send(topic, value=posicion.encode(FORMAT))
            future.get(timeout=10)  # Bloquear hasta que se envíe el mensaje
            print(f"Posición enviada: {posicion}")
        except KafkaError as e:
            print(f"Error enviando posición: {e}")
            break

def getToken(alias, hashed_password):
    context = ssl.create_default_context()
    context.load_verify_locations(cafile=CERT_FILE)
    context.check_hostname = False

    datos_dron = {
        'alias': alias,
        'password': hashed_password
    }

    try:
        with socket.create_connection((IP_ENGINE, PORT_ENGINE)) as sock:
            with context.wrap_socket(sock, server_hostname=IP_ENGINE) as ssock:
                try:
                    ssock.send(json.dumps(datos_dron).encode())
                    respuesta = ssock.recv(1024).decode(FORMAT)

                    if respuesta == "None":
                        return None
                    elif respuesta == "False":
                        return False
                    else:
                        return respuesta
                except Exception as e:
                    print(f"Error durante la autenticación: {e}")
                    main()
    except Exception as ex:
        print("El servidor de autenticación no se encuentra disponible en estos momentos. Inténtelo de nuevo más tarde.")
        print(f"Exception: {ex}")
        main()

def opt3():
    alias = input("Ingrese el alias del dron: ")
    password = hash_password(getpass.getpass("Ingrese la contraseña: "))
    hashed_password = hash_password(password)
    idDron = getToken(alias, hashed_password)
    print(idDron)

    if idDron is None:
        print("No se encontró ningún dron con ese alias y contraseña o el dron no tiene un token.")
        main()

    elif idDron is False:
        print("Alias o contraseña incorrectos.")
        main()
    else:
        print(f"Dron validado con ID: {idDron}.")
        try:
            print("dir_kafka: ", DIRECCION_KAFKA)

            result = receive_kafka(DIRECCION_KAFKA, 'Engine')
            result = result.decode('utf-8')
            result = json.loads(result)
            for dron in result:
                if dron["id"] == int(idDron):
                    preparado(dron)
        except Exception as e:
            print(f"Error en opt3: {e}")

def main():
    global DRONE_ID
    option = 0

    while option != 4:
        print("=== Bienvenido al sistema de gestión de drones ===")
        print("1. Registrar dron vía Socket")
        print("2. Solicitar nuevo token vía REST")
        print("3. Validar dron con Engine")
        print("4. Enviar posiciones")
        option = input("Seleccione una opción: ")

        if option == "1":
            registrar_dron_via_socket()
        elif option == "2":
            solicitar_nuevo_token_via_rest()
        elif option == "3":
            opt3()
        elif option == "4":
            "Salir del programa"
            break
        else:
            print("Opción no válida. Intente nuevamente.")

if __name__ == "__main__":
    main()

