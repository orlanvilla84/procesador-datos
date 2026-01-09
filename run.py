from app import app

if __name__ == '__main__':
    # Usamos el puerto 8080 que es el estándar para Codespaces
    app.run(debug=True, host='0.0.0.0', port=8080)