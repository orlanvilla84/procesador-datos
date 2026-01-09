#!/bin/bash

# Gestionando permisos
chmod u+x run.py
chmod u+x start_web.sh
chmod u+x uninstall.sh
chmod u+x stop_service.sh
chmod u+x start_service.sh

# Copiar script a las una particion
REPO_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
REPO_NAME=$(basename "$REPO_DIR")

echo "Copiando directorio a la ruta /opt/$REPO_NAME"
sudo cp -r "$REPO_DIR" /opt/
sudo chown "$USER":root /opt/"$REPO_NAME" --recursive

echo "Creando servicio de linux para aprovisionar pagina web de gestion de mallas..."
sudo cp validador_mallas_web.service /etc/systemd/system/validador_mallas_web.service 

sudo systemctl daemon-reload
sudo systemctl enable validador_mallas_web.service
sudo systemctl start validador_mallas_web.service

echo "Servicio instalado correctamente."
