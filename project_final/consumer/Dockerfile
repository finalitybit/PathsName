# consumer/Dockerfile
FROM python:3.11-slim

# Установка системных зависимостей и локали
RUN apt-get update && apt-get install -y \
    build-essential \
    libssl-dev \
    libffi-dev \
    python3-dev \
    locales \
    && rm -rf /var/lib/apt/lists/* \
    && sed -i '/ru_RU.UTF-8/s/^# //g' /etc/locale.gen \
    && locale-gen

ENV LANG ru_RU.UTF-8
ENV LANGUAGE ru_RU:ru
ENV LC_ALL ru_RU.UTF-8
ENV TZ=Europe/Moscow

# Установка рабочей директории
WORKDIR /app/consumer

# Копирование файлов проекта
COPY consumer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копирование исходного кода и конфигурации
COPY consumer/consumer.py /app/consumer/
COPY .env .
COPY data app/data/

# Создание директорий для логов и данных
RUN mkdir -p /app/logs /app/data
RUN python -m spacy download en_core_web_sm

# Команда для запуска через скрипт
CMD ["python3", "/app/consumer/consumer.py"]