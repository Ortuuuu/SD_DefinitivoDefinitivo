import sys
import socket
import ssl
import json
import threading
import os
import uuid
import time
import hashlib
from flask import Flask, request, jsonify
from datetime import datetime

# Configuraciones
HOST = '0.0.0.0'
PORT = int(sys.argv[1])
CERT_FILE = 'certs/certificado.crt'
KEY_FILE = 'certs/clavePrivada.key'
DB_FILE = 'engineDDBB.json'
AUDIT_LOG_FILE = 'logs/registry.json'
TOKEN_EXPIRATION = 20  # segundos

if not os.path.exists('logs'):
    os.makedirs('logs')

if not os.path.exists(DB_FILE):
    with open(DB_FILE, 'w') as db:
        json.dump([], db)

if not os.path.exists(AUDIT_LOG_FILE):
    with open(AUDIT_LOG_FILE, 'w') as log:
        json.dump([], log)

# Recuperar último ID utilizado
def recuperar_ultimo_id():
    with open(DB_FILE, 'r') as db:
        data = json.load(db)
        if data:
            return max(dron['id'] for dron in data)
        else:
            return 0

ultimo_id = recuperar_ultimo_id()

def generar_id_unico():
    global ultimo_id
    ultimo_id += 1
    return ultimo_id

def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

def registrar_evento(evento):
    with open(AUDIT_LOG_FILE, 'r+') as log:
        logs = json.load(log)
        logs.append(evento)
        log.seek(0)
        json.dump(logs, log, indent=4)

def registrar_dron(conn, address):
    print(f"Conexión establecida con {address}")
    registrar_evento({
        'fecha_hora': datetime.now().isoformat(),
        'ip': address[0],
        'accion': 'conexion_establecida',
        'descripcion': f'Conexion establecida con {address}'
    })
    try:
        datos = conn.recv(1024).decode()
        datos_dron = json.loads(datos)
        
        # Generar ID único y token
        dron_id = generar_id_unico()
        token = uuid.uuid4().hex
        datos_dron['id'] = dron_id
        datos_dron['token'] = token
        datos_dron['x'] = 0
        datos_dron['y'] = 0
        datos_dron['token_expiry'] = time.time() + TOKEN_EXPIRATION
        
        # Hashear la contraseña
        datos_dron['password'] = hash_password(datos_dron['password'])
        
        # Guardar en la base de datos
        with open(DB_FILE, 'r+') as db:
            data = json.load(db)
            data.append(datos_dron)
            db.seek(0)
            json.dump(data, db, indent=4)
        
        # Enviar respuesta al dron
        respuesta = {
            'id': dron_id,
            'alias': datos_dron['alias']
        }
        conn.send(json.dumps(respuesta).encode())
        
        # Registrar evento
        registrar_evento({
            'fecha_hora': datetime.now().isoformat(),
            'ip': address[0],
            'accion': 'registro',
            'descripcion': f'Dron registrado con alias {datos_dron["alias"]}'
        })
    except Exception as e:
        print(f"Error registrando dron: {e}")
        registrar_evento({
            'fecha_hora': datetime.now().isoformat(),
            'ip': address[0],
            'accion': 'error_registro',
            'descripcion': f'Error registrando dron: {e}'
        })
    finally:
        conn.close()

def limpiar_tokens_expirados():
    while True:
        with open(DB_FILE, 'r+') as db:
            data = json.load(db)
            tiempo_actual = time.time()
            for dron in data:
                if 'token_expiry' in dron and dron['token_expiry'] and dron['token_expiry'] <= tiempo_actual:
                    registrar_evento({
                        'fecha_hora': datetime.now().isoformat(),
                        'ip': 'registry',
                        'accion': 'token_expirado',
                        'descripcion': f'Token expirado para el dron con alias {dron["alias"]}'
                    })
                    dron['token'] = None
                    dron['token_expiry'] = None
            db.seek(0)
            db.truncate()
            json.dump(data, db, indent=4)
        time.sleep(1)  # Verificar cada segundo

def inicializar_token_expiry():
    with open(DB_FILE, 'r+') as db:
        data = json.load(db)
        for dron in data:
            if 'token_expiry' not in dron:
                dron['token_expiry'] = None
        db.seek(0)
        json.dump(data, db, indent=4)

# Iniciar API REST
app = Flask(__name__)
@app.route('/autenticar', methods=['POST'])
def autenticar():
    alias = request.json.get('alias')
    password = request.json.get('password')
    if not alias or not password:
        return jsonify({'error': 'Alias y contraseña son requeridos'}), 400
    
    hashed_password = hash_password(password)
    
    with open(DB_FILE, 'r+') as db:
        data = json.load(db)
        dron = next((d for d in data if d['alias'] == alias and d['password'] == hashed_password), None)
        if dron:
            # Generar nuevo token y actualizar
            token = uuid.uuid4().hex
            dron['token'] = token
            dron['token_expiry'] = time.time() + TOKEN_EXPIRATION
            db.seek(0)
            json.dump(data, db, indent=4)
            
            # Registrar evento de autenticación exitosa
            registrar_evento({
                'fecha_hora': datetime.now().isoformat(),
                'ip': request.remote_addr,
                'accion': 'renovacion_token_exitosa',
                'descripcion': f'Dron con alias {alias} autenticado correctamente'
            })
            
            return jsonify({'token': token})
        else:
            # Registrar evento de autenticación fallida
            registrar_evento({
                'fecha_hora': datetime.now().isoformat(),
                'ip': request.remote_addr,
                'accion': 'autenticacion_fallida',
                'descripcion': f'Fallo de autenticación para el alias {alias}'
            })
            return jsonify({'error': 'Alias o contraseña incorrectos'}), 401

def iniciar_api():
    context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    context.load_cert_chain(certfile=CERT_FILE, keyfile=KEY_FILE)
    app.run(host=HOST, port=PORT+1, ssl_context=context)

def main():
    if len(sys.argv) != 2:
        print("Uso: ad_registry.py <Puerto_Registry>")
        sys.exit(1)
    
    inicializar_token_expiry()
    threading.Thread(target=iniciar_api).start()
    threading.Thread(target=limpiar_tokens_expirados).start()

    context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    context.load_cert_chain(certfile=CERT_FILE, keyfile=KEY_FILE)
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0) as sock:
        sock.bind((HOST, PORT))
        sock.listen(10)
        print(f"AD_Registry escuchando en {HOST}:{PORT}")
        
        while True:
            conn, addr = sock.accept()
            conn = context.wrap_socket(conn, server_side=True)
            threading.Thread(target=registrar_dron, args=(conn, addr)).start()

if __name__ == '__main__':
    main()
