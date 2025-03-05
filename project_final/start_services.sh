#!/bin/bash

# Функция для логирования
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1"
}

# Функция проверки статуса контейнера
check_container() {
    local container=$1
    local max_attempts=30
    local attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        if docker-compose ps $container | grep -q "Up"; then
            log "$container is up and running"
            return 0
        fi
        log "Waiting for $container... (attempt $attempt/$max_attempts)"
        sleep 2
        attempt=$((attempt + 1))
    done
    
    log "ERROR: $container failed to start properly"
    return 1
}

# Создаем необходимые директории
log "Creating directories..."
mkdir -p logs data

# Проверяем наличие docker-compose
if ! command -v docker-compose &> /dev/null; then
    log "ERROR: docker-compose is not installed"
    exit 1
fi

# Останавливаем все существующие контейнеры
log "Stopping any existing containers..."
docker-compose down

# Запускаем MongoDB
log "Starting MongoDB..."
docker-compose up -d mongodb
check_container mongodb || exit 1

# Запускаем Mongo Express
log "Starting Mongo Express..."
docker-compose up -d mongo-express
check_container mongo-express || exit 1

# Запускаем RabbitMQ
log "Starting RabbitMQ..."
docker-compose up -d rabbitmq
check_container rabbitmq || exit 1

# Запускаем Redis
log "Starting Redis..."
docker-compose up -d redis
check_container redis || exit 1

# Запускаем Telegram бота
log "Starting Telegram bot..."
docker-compose up -d telegram_bot
check_container telegram_bot || exit 1

# Запускаем Consumer
log "Starting Consumer..."
docker-compose up -d consumer
check_container consumer || exit 1

# Показываем статус всех контейнеров
log "All services started. Checking status..."
docker-compose ps

# Показываем логи в случае ошибок
log "Checking for any errors in logs..."
docker-compose logs --tail=50
