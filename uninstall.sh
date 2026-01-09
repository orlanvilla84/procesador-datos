#!/bin/bash

# Copiar script a las una particion
REPO_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
REPO_NAME=$(basename "$REPO_DIR")

echo "Deteniendo servicios"
sudo systemctl disable validador_mallas_web.service
sudo systemctl stop validador_mallas_web.service
sudo rm /etc/systemd/system/validador_mallas_web.service
sudo systemctl daemon-reload

echo "Borrando carpeta principal"
sudo rm -rf /opt/"$REPO_NAME"

echo "Desintalacion terminada. Para instalar de nuevo ejecuta el script install.sh"
