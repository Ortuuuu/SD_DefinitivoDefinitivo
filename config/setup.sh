#!/bin/bash

echo "Configurando el entorno del proyecto Art with Drones..."

# Actualiza el índice de paquetes
echo "Actualizando el índice de paquetes..."
sudo apt update -y
sudo apt upgrade -y

# Instalación de Python y pip
echo "Instalando Python y pip..."
sudo apt install -y python3 python3-venv python3-pip

# Configuración del entorno virtual de Python
echo "Configurando el entorno virtual de Python..."
python3 -m venv SD_AwD
cd SD_AwD

# Creación de la esgructura de directorios
echo "Creando estructura de directorios y situando archivos en sus respectivos directorios..."
mkdir certs
mkdir logs
mkdir public
mv ../{certificado.crt,clavePrivada.key} certs/
mv ../{drone.py,ad_registry.py,ad_engine.py,AD_aux.py,FIGURA.json, requirements.txt} .

# Instalación de dependencias de Python
source bin/activate
echo "Instalando dependencias de Python en el entorno virtual..."
pip install -r requirements.txt

# Mensaje de finalización
echo "Configuración del entorno del proyecto Art with Drones completada."

# Incompleto no es 100 por cien funcional

