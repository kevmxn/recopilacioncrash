# Usa Python 3.10 oficial como imagen base
FROM python:3.10-slim

# Establece el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copia el archivo de dependencias primero (para aprovechar caché de Docker)
COPY requirements.txt .

# Instala las dependencias
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copia el resto del código de la aplicación
COPY . .

# Expone el puerto que usa tu aplicación (8080)
EXPOSE 8080

# Comando para ejecutar la aplicación
CMD ["python", "main.py"]