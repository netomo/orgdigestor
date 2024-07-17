# Organization Digestor Project

Welcome to the Organization Digestor Project!
This project is designed to process and digest lots of data efficiently.
Using Python, Django, Celery, and Docker,
it offers a robust solution for handling asynchronous tasks and data processing.

## Project Structure

The project is structured as follows:

- `docker-compose.yml`: Defines the services, networks, and volumes for the project. It includes services for 
  - PostgreSQL
  - RabbitMQ
  - Django application (`digestor-app`)
  - Celery workers
  - Celery Flower for monitoring.
- `Dockerfile`: Instructions for building the Docker image for the Django application (including migrations), Celery worker, and Celery Flower.
- `orgdigestor/`: Contains the Django application including models, views, serializers, and tasks for processing organization data.
- `orgdigestor/tasks.py`: Defines Celery tasks for processing CSV files and updating the database with organization information.

## Prerequisites

Before you start, ensure you have Docker and Docker Compose installed on your system.

## Running the Project

To get the project up and running, follow these steps:

1. **Clone the Repository**

   First, clone this repository to your local machine using Git:

   ```bash
   git clone git@github.com:netomo/orgdigestor.git organization-digestor
   cd organization-digestor
    ```
   
2. **Build and start the Docker Containers**

    ```bash
    docker-compose up --build
    ```
    Maybe the first time you run the command, rabbitmq will take a little longer to start, so you may need to run the command again.

3. **Accessing the Application**

    Once the containers are up and running, you can access the Django application at `http://localhost:8000/api/schema/swagger-ui/`.

4. **Uploading a CSV File**
    
    Explore the `POST /api/orgdigestor/organizations/digest/` endpoint to upload a CSV file and start processing the data.

5. **Monitoring Celery Tasks**
    
   You can monitor the Celery tasks using Flower at `http://localhost:5555`.
