# Usar una imagen base de Python
FROM python:3.9

# Establecer el directorio de trabajo en /app
WORKDIR /app

# Copiar solo el archivo que quieres incluir en Docker
COPY / /app

RUN pip install --no-cache-dir -r requirements.txt

# Comando para ejecutar tu archivo cuando el contenedor se inicie
CMD ["python", "recomendacion.py"]
