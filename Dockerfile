# Use a base image with Python pre-installed
FROM python:3.8-slim

# Set the working directory in the container
WORKDIR /app

# Copy the source code to the container
COPY src/ /app

# Install required packages
RUN pip install --no-cache-dir psycopg2-binary numpy pandas scipy flask sqlalchemy

# Expose the port the app runs on
EXPOSE 80

# Define the command to run your application
CMD ["python", "conexion.py"]