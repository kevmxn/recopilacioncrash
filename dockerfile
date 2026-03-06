FROM python:3.10-slim

WORKDIR /app

# Actualizar pip e instalar dependencias
COPY requirements.txt .
RUN pip install --upgrade pip setuptools wheel && \
    pip install -r requirements.txt

COPY . .

EXPOSE 8080

CMD ["python", "main.py"]
