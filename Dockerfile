FROM python:3.12
LABEL authors="netomo"

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Install Poetry
RUN pip install poetry

# Set work directory
WORKDIR /app

# Install dependencies
COPY pyproject.toml poetry.lock /app/
RUN poetry config virtualenvs.create false && poetry install --no-root

# Copy the rest of the project
COPY . /app/
