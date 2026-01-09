#!/bin/bash

# Copiar script a las una particion
REPO_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
REPO_NAME=$(basename "$REPO_DIR")

echo "Deteniendo servicios"
sudo systemctl start validador_mallas_web.service

echo "Servicio detenido correctamente"
