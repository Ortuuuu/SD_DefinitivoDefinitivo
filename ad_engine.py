import sys
import threading
import time
import json
import queue
import os
import requests
import ssl
import socket
from datetime import datetime
from AD_aux import *

FORMAT = 'utf-8'
DRON_FILE = 'engineDDBB.json'
IP_KAFKA = sys.argv[2]
IP_SOCKET = sys.argv[4]
PORT_KAFKA = int(sys.argv[3])
PORT_SOCKET = int(sys.argv[1])
DIRECCION_KAFKA = f"{IP_KAFKA}:{PORT_KAFKA}"
FILS = 20
COLS = 20
NUM_PREPARADOS = 0
NUM_TERMINADOS = 0
CERT_FILE = 'certs/certificado.crt'
CERT_KEY = 'certs/clavePrivada.key'
API_KEY = "1fc17bb6f61efe5e3ce57fa93e9cbdc8"
AUDIT_LOG_FILE_ENGINE = 'logs/engine.json'
alMain = False
figura = 'FIGURA.json'

mapa = [['-' for _ in range(COLS)] for _ in range(FILS)]
exit_flag = False
posiciones_queue = queue.Queue()
start_flag = threading.Event()

if not os.path.exists('logs'):
    os.makedirs('logs')
    
if not os.path.exists(AUDIT_LOG_FILE_ENGINE):
    with open(AUDIT_LOG_FILE_ENGINE, 'w') as log:
        json.dump([], log)

def registrar_evento_engine(evento):
    with open(AUDIT_LOG_FILE_ENGINE, 'r+') as log:
        logs = json.load(log)
        logs.append(evento)
        log.seek(0)
        json.dump(logs, log, indent=4)

def obtener_temperatura(ciudad):
    url = f"http://api.openweathermap.org/data/2.5/weather?q={ciudad}&appid={API_KEY}&units=metric"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        temperatura = data['main']['temp']
        registrar_evento_engine({
            'fecha_hora': datetime.now().isoformat(),
            'accion': 'obtener_temperatura',
            'descripcion': f'Temperatura obtenida para la ciudad {ciudad}: {temperatura}°C'
        })
        return temperatura
    else:
        registrar_evento_engine({
            'fecha_hora': datetime.now().isoformat(),
            'accion': 'error_obtener_temperatura',
            'descripcion': f'Error al obtener la temperatura para la ciudad {ciudad}'
        })
        return None


def read_drones_file(file_path=DRON_FILE):
    with open(file_path, 'r') as file:
        return json.load(file)

def write_drones_file(drones, file_path=DRON_FILE):
    with open(file_path, 'w') as file:
        json.dump(drones, file, indent=5)

def update_map():
    global mapa
    global posiciones_queue
    global start_flag
    global NUM_TERMINADOS
    global NUM_PREPARADOS
    global alMain
    NUM_TERMINADOS = 0
    global exit_flag
    while True:
        try:
            posiciones = receive_kafka_pos(DIRECCION_KAFKA, 'posiciones', "4")
            for pos in posiciones.decode(FORMAT).split('\n'):
                posiciones_queue.put(pos)  
            
            posiciones = posiciones.decode(FORMAT).split(',')
            if len(posiciones) == 3:
                dron_id = posiciones[0].replace('"', '')
                x = int(posiciones[1])
                y = int(posiciones[2].replace('"', ''))

                drones = read_drones_file()
                for drone in drones:
                    if drone['id'] == int(dron_id):
                        drone['x'] = x
                        drone['y'] = y
                        break
                write_drones_file(drones)
                
                if x == 99 or y == 99:
                    if alMain:
                        return
                    NUM_TERMINADOS += 1
                    drones = read_drones_file()
                    print(NUM_PREPARADOS)
                    if NUM_TERMINADOS == NUM_PREPARADOS:
                        for drone in drones:
                            if drone['id'] == int(dron_id):
                                drone['x'] = x
                                drone['y'] = y
                            for i in range(FILS):
                                for j in range(COLS):
                                    if i == x and j == y:
                                        mapa[i][j] = str(dron_id)
                        volver = input("Todos los drones han llegado a su destino, volver a la posición inicial? (s/n): ")
                        if volver == "s":
                            print("Volviendo a la posición inicial")
                            send_kafka(DIRECCION_KAFKA, 'volver', 's')
                        elif volver == "n":
                            print("Realizando siguiente figura directamente")
                            send_kafka(DIRECCION_KAFKA, 'volver', 'n')
                            return
                    continue
                
                print(NUM_TERMINADOS)
                
                if NUM_TERMINADOS != 0:
                    if x == 0 and y == 0:
                        if alMain:
                            return
                        NUM_TERMINADOS -= 1
                        if NUM_TERMINADOS == 0:
                            print("Todos los drones han vuelto a la base")
                            time.sleep(2)
                            send_kafka(DIRECCION_KAFKA, 'siguiente', 's')
                            return
                        continue

                for i in range(FILS):
                    for j in range(COLS):
                        if mapa[i][j] == str(dron_id):
                            mapa[i][j] = '-'
                mapa[x][y] = str(dron_id)
                
                print_map(mapa)
        except Exception as e:
            print(f"Error actualizando mapa: {e}")
    print("Fin de la actualización del mapa")

def print_map(mapa):
    for row in mapa:
        print(' '.join(row))
    print("\n")

def hacer(drones_data, num_drones, figura):
    join_show(drones_data, num_drones, drones_data)
    print("seguimos")
    send_kafka(DIRECCION_KAFKA, 'figura', figura)                      
    time.sleep(1)
    send_kafka(DIRECCION_KAFKA, 'comenzar', 's')     
    update_map()

def join_show(drones, num_drones, drones_data):
    global NUM_PREPARADOS
    NUM_PREPARADOS = 0
    city = input("Ciudad en la que se realizará el espectáculo: ")
    send_kafka(DIRECCION_KAFKA, 'Engine', drones_data)
    var = obtener_temperatura(city)
    registrar_evento_engine({
        'fecha_hora': datetime.now().isoformat(),
        'accion': 'temperatura_obtenida',
        'descripcion': f'Temperatura en {city}: {var}°C'
    })
    if(var <= 0):
        print("CONDICIONES CLIMÁTICAS ADVERSAS, SE CANCELA EL ESPECTÁCULO")
        registrar_evento_engine({
            'fecha_hora': datetime.now().isoformat(),
            'accion': 'espectaculo_cancelado',
            'descripcion': 'Condiciones climaticas adversas'
        })
    else:
        nuevos = False
        while True:
            send_kafka(DIRECCION_KAFKA, 'Engine', drones)
            registrar_evento_engine({
                'fecha_hora': datetime.now().isoformat(),
                'accion': 'esperando_confirmacion',
                'descripcion': 'Esperando confirmacion de drones'
            })
            print("Esperando confirmación de drones...")
            opcion = receive_kafka(DIRECCION_KAFKA, 'confirmar')
            opcion = opcion.decode(FORMAT).replace('"', '')
            for drone in drones:
                if opcion == f"Ready{drone['id']}":
                    print(f"Drone {drone['alias']} está listo")
                    NUM_PREPARADOS += 1
                    registrar_evento_engine({
                        'fecha_hora': datetime.now().isoformat(),
                        'accion': 'drone_listo',
                        'descripcion': f'Drone {drone["alias"]} esta listo'
                    })
                    print(f"{NUM_PREPARADOS} drones listos")
                    if num_drones == int(drone["id"]) or nuevos:
                        if NUM_PREPARADOS == num_drones:
                            print("Todos los drones están listos")
                            time.sleep(2)
                            send_kafka(DIRECCION_KAFKA, 'comenzar', 's')
                            registrar_evento_engine({
                                'fecha_hora': datetime.now().isoformat(),
                                'accion': 'comenzar_espectaculo',
                                'descripcion': 'Todos los drones están listos, comenzando espectaculo'
                            })
                            return
                        else:
                            continuar = input("¿Desea continuar con los drones que están listos? (s/n): ")
                            if continuar.lower() == "s":
                                print("Continuando con los drones que están listos")
                                registrar_evento_engine({
                                    'fecha_hora': datetime.now().isoformat(),
                                    'accion': 'continuar_drones_listos',
                                    'descripcion': 'Continuando con los drones que estan listos'
                                })
                                return
                            else:
                                print("Esperando a que los drones restantes se preparen...")
                                nuevos = True
                                continue
                        break

# En la función handle_drone_auth
def handle_drone_auth(connection, address):
    try:
        registrar_evento_engine({
            'fecha_hora': datetime.now().isoformat(),
            'accion': 'conexion_establecida',
            'descripcion': f'Conexion establecida con {address}'
        })
        data = connection.recv(1024).decode(FORMAT)
        if not data:
            registrar_evento_engine({
                'fecha_hora': datetime.now().isoformat(),
                'accion': 'error_autenticacion',
                'descripcion': 'No se recibieron datos'
            })
            return
        datos_dron = json.loads(data)
        alias = datos_dron['alias']
        password = datos_dron['password']

        with open(DRON_FILE, 'r') as file:
            drones_data = json.load(file)
            for drone in drones_data:
                if drone["alias"] == alias and drone["password"] == password:
                    if drone["token"] is None:
                        connection.send("None".encode(FORMAT))
                    else:
                        connection.send(str(drone["id"]).encode(FORMAT))
                    registrar_evento_engine({
                        'fecha_hora': datetime.now().isoformat(),
                        'accion': 'autenticacion_exitosa',
                        'descripcion': f'Autenticacion exitosa para {alias}'
                    })
                    return
            print("DEBUG_MSG: Enviando 'False'")
            connection.send("False".encode(FORMAT))
            registrar_evento_engine({
                'fecha_hora': datetime.now().isoformat(),
                'accion': 'autenticacion_fallida',
                'descripcion': f'Fallo de autenticacion para {alias}'
            })
    except Exception as e:
        print(f"Error al manejar la autenticación del dron: {e}")
        registrar_evento_engine({
            'fecha_hora': datetime.now().isoformat(),
            'accion': 'error_autenticacion',
            'descripcion': f'Error al manejar la autenticacin del dron: {e}'
        })
    finally:
        try:
            connection.close()
            print("\nSelecciona una opcion del menu: ")
            registrar_evento_engine({
                'fecha_hora': datetime.now().isoformat(),
                'accion': 'conexion_cerrada',
                'descripcion': 'Conexion cerrada'
            })
        except Exception as e:
            print(f"Error al cerrar la conexión: {e}")
            registrar_evento_engine({
                'fecha_hora': datetime.now().isoformat(),
                'accion': 'error_cerrar_conexion',
                'descripcion': f'Error al cerrar la conexion: {e}'
            })


def start_socket_server():
    context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    context.load_cert_chain(certfile=CERT_FILE, keyfile=CERT_KEY)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.bind((IP_SOCKET, PORT_SOCKET))
        server_socket.listen(5)

        while True:
            try:
                connection, address = server_socket.accept()
                secure_connection = context.wrap_socket(connection, server_side=True)
                threading.Thread(target=handle_drone_auth, args=(secure_connection, address)).start()
            except Exception as e:
                print(f"Error al aceptar la conexión: {e}")

def iniciar():
    global NUM_PREPARADOS
    global exit_flag
    num_drones = 0
    drones_file = DRON_FILE

    figura = input('Fichero con la figura a dibujar: ')    

    try:
        with open(drones_file, 'r') as file:
            drones_data = json.load(file)
    except FileNotFoundError:
        print(f"El archivo {drones_file} no se pudo encontrar.")
        return 1
    except Exception as e:
        print(f"Ocurrió un error al leer el archivo: {e}")
        return 1
        
    while True:
        try:
            with open(figura, 'r') as file:
                figura_data = json.load(file)
                figuras = figura_data.get('figuras', [])
                for figura in figuras:
                    dronesFig = figura.get('Drones', [])
                    num_drones = len(dronesFig)
                    hacer(drones_data, num_drones, figura)
                break
        except FileNotFoundError:
            print(f"El archivo {figura} no se pudo encontrar.")
            figura = input('Fichero con la figura a dibujar: ')
    print("NUM DRONES: ", num_drones)

    
    return 0

def main(): 
    socket_thread = threading.Thread(target=start_socket_server)
    socket_thread.start()
    global alMain

    while True:
        try:
            opcion = input("Elija una opción: \n1. Reanudar ejecución\n2. Ver datos drones\n3. Iniciar Figuras\n4. Salir")
            if opcion == '1':
                sigue = input("¿Desea continuar con la ejecución? (s/n): ")
                if sigue == "s":
                    alMain = True
                    update_map()
                    print_map(mapa)
            elif opcion == '2':
                print(json.dumps(read_drones_file(), indent=4))
            elif opcion == '3':
                iniciar()
            elif opcion == '4':
                exit_flag = True
                break
        except Exception as e:
            print(f"Error en la opción del menú: {e}")

    socket_thread.join()

if __name__ == "__main__":
    sys.exit(main())