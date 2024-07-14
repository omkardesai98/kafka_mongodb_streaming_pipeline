# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy .env file to the working directory in the container
COPY .env /app/.env

# Set environment variables (optional, can be overridden at runtime)
ENV MONGO_DB_PASSWORD=omkar@123

# Command to run the application
CMD ["python", "Consumer_code.py"]

