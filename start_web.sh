#!/bin/bash

REPO_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
VENV_PATH="$REPO_DIR/venv"
REQ_FILE="$REPO_DIR/requirements.txt"

echo "--- Aumentando particion /tmp a 3 GB  ---"
sudo mount -o remount,size=3G /tmp

echo "--- Iniciando proceso en $REPO_DIR ---"

# Crear el entorno virtual si no existe
if [ ! -d "$VENV_PATH" ]; then
    echo "[+] Creando entorno virtual..."
    python3 -m venv "$VENV_PATH"
fi

# Activar el entorno virtual
echo "[+] Activando entorno virtual..."
source "$VENV_PATH/bin/activate"

# Instalar/Actualizar dependencias
if [ -f "$REQ_FILE" ]; then
    echo "[+] Instalando dependencias desde requirements.txt..."
    pip install --upgrade pip
    pip install -r "$REQ_FILE"
else
    echo "[!] Advertencia: No se encontró requirements.txt"
fi

# Ejecutar la aplicación
echo "[+] Iniciando servidor web..."
# Reemplaza 'main.py' por el nombre de tu archivo principal
python run.py

