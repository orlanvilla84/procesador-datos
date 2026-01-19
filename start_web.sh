#!/bin/bash

# 1. Definición de rutas y nombres
# Obtiene la ruta absoluta de la carpeta donde reside este script
REPO_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
# Extrae el nombre de la carpeta (repositorio)
REPO_NAME=$(basename "$REPO_DIR")
VENV_PATH="$REPO_DIR/venv"
REQ_FILE="$REPO_DIR/requirements.txt"

# 2. Configuración de espacio temporal local
# Creamos una carpeta temporal dentro del disco principal para evitar el error 'Errno 122'
TMP_INSTALL_DIR="$REPO_DIR/tmp_pip"
mkdir -p "$TMP_INSTALL_DIR"
export TMPDIR="$TMP_INSTALL_DIR"

echo "--- Iniciando proceso en el repositorio: $REPO_NAME ---"
echo "--- Ruta: $REPO_DIR ---"

# 3. Gestión del Entorno Virtual
if [ ! -d "$VENV_PATH" ]; then
    echo "[+] Creando entorno virtual..."
    python3 -m venv "$VENV_PATH"
fi

# 4. Activación e Instalación de Dependencias
echo "[+] Activando entorno virtual..."
source "$VENV_PATH/bin/activate"

if [ -f "$REQ_FILE" ]; then
    echo "[+] Instalando/Actualizando dependencias..."
    # Usamos una carpeta de caché local para asegurar que haya espacio suficiente
    pip install --upgrade pip
    pip install --cache-dir "$REPO_DIR/.pip_cache" -r "$REQ_FILE"
else
    echo "[!] Advertencia: No se encontró requirements.txt"
fi

# 5. Limpieza de archivos temporales de instalación
rm -rf "$TMP_INSTALL_DIR"

# 6. Ejecución de la Aplicación
echo "[+] Iniciando servidor web (run.py)..."
# Ejecutamos usando la ruta absoluta del python del venv para mayor estabilidad
"$VENV_PATH/bin/python" "$REPO_DIR/run.py"
