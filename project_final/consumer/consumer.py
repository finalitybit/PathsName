#!/usr/bin/env python3

import os
import sys
import json
import logging
import pika
import re
import time
from typing import Dict, List, Optional, Any
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from dotenv import load_dotenv
import colorlog
import pandas as pd
from datetime import datetime
from pymorphy3 import MorphAnalyzer
from natasha import Doc, MorphVocab, NewsEmbedding, NewsMorphTagger, NewsNERTagger
import locale
from natasha import Segmenter, MorphVocab, NewsEmbedding, NewsMorphTagger, NewsNERTagger, Doc
import nltk
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
import spacy

# Загрузка переменных окружения
load_dotenv()

# Настройка логирования
log_directory = os.path.abspath(os.getenv("LOG_DIRECTORY", "./logs"))
if not os.path.exists(log_directory):
    os.makedirs(log_directory, exist_ok=True)

# Настройка корневого логгера
logging.basicConfig(level=logging.INFO)

# Создание логгера для модуля
logger = logging.getLogger('consumer')
logger.setLevel(logging.INFO)

# Очистка существующих обработчиков
logger.handlers.clear()

# Настройка форматирования для консоли
console_handler = colorlog.StreamHandler()
console_formatter = colorlog.ColoredFormatter(
    '%(log_color)s%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    log_colors={
        'DEBUG': 'cyan',
        'INFO': 'green',
        'WARNING': 'yellow',
        'ERROR': 'red',
        'CRITICAL': 'red,bg_white'
    }
)
console_handler.setFormatter(console_formatter)
console_handler.setLevel(logging.INFO)
logger.addHandler(console_handler)

# Настройка файлового логирования
file_handler = logging.FileHandler(f"{log_directory}/consumer.log", mode='a', encoding='utf-8')
file_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s')
file_handler.setFormatter(file_formatter)
file_handler.setLevel(logging.INFO)
logger.addHandler(file_handler)

# Отключение передачи логов родительским логгерам
logger.propagate = False

logger.debug("Логирование настроено")

# Устанавливаем русскую локаль
try:
    locale.setlocale(locale.LC_ALL, 'ru_RU.UTF-8')
    logger.debug("Русская локаль установлена")
except locale.Error:
    logger.warning("Не удалось установить русскую локаль")

# Инициализация моделей Natasha
try:
    segmenter = Segmenter()
    morph_vocab = MorphVocab()
    emb = NewsEmbedding()
    morph_tagger = NewsMorphTagger(emb)
    ner_tagger = NewsNERTagger(emb)
    logger.debug("Модели Natasha инициализированы")
except Exception as e:
    logger.error(f"Ошибка инициализации Natasha: {e}")

# Инициализация NLTK и spaCy для английского
try:
    nltk.download('punkt', quiet=True)
    nltk.download('wordnet', quiet=True)
    logger.debug("Ресурсы NLTK загружены")
except Exception as e:
    logger.error(f"Ошибка загрузки NLTK ресурсов: {e}")

try:
    nlp_en = spacy.load("en_core_web_sm")
    logger.debug("Модель spaCy для английского загружена")
except OSError as e:
    logger.error(f"Модель spaCy для английского не найдена: {e}. Установите: python -m spacy download en_core_web_sm")
    sys.exit(1)
lemmatizer = WordNetLemmatizer()

# Переменные окружения
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", 5672))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "admin")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "rabbitpass")
RABBITMQ_VHOST = os.getenv("RABBITMQ_VHOST", "/")
QUEUE_NAME = os.getenv("QUEUE_NAME", "path_queue")
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://admin:mongopass@mongodb:27017/paths_db?authSource=admin")
MONGODB_DB = os.getenv("MONGODB_DB", "paths_db")
MONGODB_COLLECTION = os.getenv("MONGODB_COLLECTION", "paths")
XLSX_PATH = os.getenv("XLSX_PATH", "/data/paths.xlsx")
PATHS_COLUMN = os.getenv("PATHS_COLUMN", "Path")

logger.debug(f"Переменные окружения: XLSX_PATH={XLSX_PATH}, MONGODB_URI={MONGODB_URI}")

# Вспомогательные функции для обработки текста
def normalize_text(text: str) -> str:
    """Нормализация текста."""
    if not text:
        return ""
    normalized = re.sub(r'[^\w\s]', ' ', text.lower())
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    return normalized

def tokenize_text(text: str) -> List[str]:
    """Разбиение текста на токены."""
    if not text:
        return []
    tokens = re.split(r'[\s_\-]+', text.lower())
    return [token for token in tokens if token and len(token) > 2]

def contains_cyrillic(text: str) -> bool:
    """Проверяет, содержит ли текст кириллические символы."""
    for char in text:
        if 'а' <= char.lower() <= 'я':
            return True
    return False

# Функция для нормализации путей
def normalize_path(path: str) -> str:
    """Преобразует Windows-путь в Unix-путь с сохранением всех допустимых символов."""
    if not path:
        return ""
    
    try:
        path = path.strip()
        logger.debug(f"Исходный путь: {path}")
        
        if ':' in path:
            path = path.split(':', 1)[1]
            logger.debug(f"После удаления буквы диска: {path}")
        path = path.replace('\\', '/')
        logger.debug(f"После замены слешей: {path}")
        
        while '//' in path:
            path = path.replace('//', '/')
            
        if not path.startswith('/'):
            path = '/' + path
        logger.debug(f"Нормализованный путь: {path}")
            
        invalid_chars = [c for c in path if ord(c) < 32 or c in '<>"|*?']
        if invalid_chars:
            logger.warning(f"Путь содержит недопустимые символы {invalid_chars}: {path}")
            return ""
        
        return path
        
    except Exception as e:
        logger.error(f"Ошибка нормализации пути: {str(e)}")
        return ""

# Класс для работы с MongoDB
class DatabaseHandler:
    def __init__(self, uri: str, db_name: str, collection_name: str):
        self.uri = uri
        self.db_name = db_name
        self.collection_name = collection_name
        self.client: Optional[MongoClient] = None
        self.db: Optional[Database] = None
        self.collection: Optional[Collection] = None
        self.morph = MorphAnalyzer()
        self.emb = NewsEmbedding()
        self.morph_tagger = NewsMorphTagger(self.emb)
        self.ner_tagger = NewsNERTagger(self.emb)
        self.morph_vocab = MorphVocab()
        self.segmenter = segmenter
        self.morph_vocab = morph_vocab
        self.morph_tagger = morph_tagger
        self.ner_tagger = ner_tagger
        self.nlp_en = nlp_en  # spaCy для английского
        self.lemmatizer = lemmatizer  # NLTK lemmatizer
        logger.debug("Инициализация DatabaseHandler начата")
        self.connect()

    def connect(self) -> bool:
        """Подключение к MongoDB."""
        try:
            time.sleep(10)
            self.client = MongoClient(
                self.uri,
                serverSelectionTimeoutMS=15000,
                socketTimeoutMS=15000,
                connectTimeoutMS=15000,
                retryWrites=True,
                retryReads=True,
                connect=False
            )
            self.client.server_info()
            self.db = self.client[self.db_name]
            self.collection = self.db[self.collection_name]
            self.collection.create_index([("path", 1)])
            self.collection.create_index([("bank", 1)])
            self.collection.create_index([("vendor", 1)])
            self.collection.create_index([("equipment", 1)])
            self.collection.create_index([("path_parts", 1)])
            self.collection.create_index([("indexed_parts.text", 1)])
            self.collection.create_index([("keywords", 1)])
            self.collection.create_index([("normalized_text", 1)])
            self.collection.create_index([("lemmas", 1)])
            self.collection.create_index([("entities.normal", 1)])
            self.collection.create_index([("path", "text"), ("metadata", "text"),
                                         ("keywords", "text"), ("normalized_text", "text")],
                                        default_language="russian")
            logger.info(f"Успешное подключение к MongoDB: {self.uri}")
            return True
        except Exception as e:
            logger.error(f"Ошибка подключения к MongoDB: {e}")
            return False

    def ensure_connected(self) -> bool:
        """Проверка и восстановление соединения."""
        if self.client is None:
            logger.warning("Клиент MongoDB не инициализирован")
            return self.connect()
        try:
            self.client.server_info()
            return True
        except Exception as e:
            logger.warning(f"Потеряно соединение с MongoDB: {e}. Попытка переподключения...")
            return self.connect()

    def add_path(self, path: str) -> bool:
        """Добавление пути с улучшенной обработкой."""
        if not self.ensure_connected():
            logger.error("Нет подключения к MongoDB перед добавлением пути")
            return False
        if not path:
            logger.warning("Пустой путь для добавления")
            return False

        try:
            normalized_path = normalize_path(path)
            if not normalized_path:
                logger.warning("Нормализованный путь пуст")
                return False

            if any(ord(c) < 32 for c in normalized_path):
                logger.warning(f"Недопустимые управляющие символы в пути: {path}")
                return False

            if normalized_path != path:
                logger.debug(f"Нормализация пути: {path} -> {normalized_path}")

            existing = self.collection.find_one({"path": normalized_path})
            metadata = self._enhance_metadata(normalized_path)
            if metadata is None:
                logger.warning(f"Не удалось создать метаданные для пути: {path}")
                return False

            if existing:
                update_result = self.collection.update_one(
                    {"path": normalized_path},
                    {"$set": metadata}
                )
                success = update_result.modified_count > 0
            else:
                insert_result = self.collection.insert_one(metadata)
                success = insert_result.inserted_id is not None

            if success:
                logger.info(f"{'Обновлен' if existing else 'Добавлен'} путь: {normalized_path}")
            else:
                logger.warning(f"Не удалось {'обновить' if existing else 'добавить'} путь: {normalized_path}")

            return success
        except Exception as e:
            logger.error(f"Ошибка при добавлении пути {path}: {str(e)}")
            return False

    def remove_path(self, path: str) -> bool:
        """Удаление пути из базы данных."""
        if not self.ensure_connected():
            logger.error("Нет подключения к MongoDB перед удалением пути")
            return False
        try:
            result = self.collection.delete_one({"path": path})
            if result.deleted_count > 0:
                logger.info(f"Путь удален из базы данных: {path}")
                return True
            logger.warning(f"Путь не найден в базе данных: {path}")
            return False
        except Exception as e:
            logger.error(f"Ошибка при удалении пути: {e}")
            return False

    def search_paths(self, query_params: Dict[str, str]) -> List[Dict[str, Any]]:
        """Улучшенный поиск с проверками."""
        if not self.ensure_connected():
            logger.error("Нет подключения к БД при поиске")
            return []

        if not query_params:
            logger.warning("Пустые параметры поиска")
            return []

        try:
            sanitized_params = {k: v for k, v in query_params.items() if v is not None and isinstance(v, (str, list))}
            if not sanitized_params:
                logger.warning("Нет валидных параметров поиска после санитизации")
                return []

            query = {}
            if "bank" in sanitized_params:
                bank = str(sanitized_params["bank"]).strip()
                if bank:
                    query["bank"] = {"$regex": bank, "$options": "i"}

            if "vendor" in sanitized_params:
                query["vendor"] = {"$regex": sanitized_params["vendor"], "$options": "i"}

            if "equipment" in sanitized_params:
                query["equipment"] = {"$regex": sanitized_params["equipment"], "$options": "i"}

            if "text" in sanitized_params:
                search_text = str(sanitized_params["text"]).strip()
                if not search_text:
                    logger.warning("Пустой поисковый текст")
                    return []

                if contains_cyrillic(search_text):
                    doc = Doc(search_text)
                    doc.segment(self.segmenter)
                    if doc.tokens:
                        doc.tag_morph(self.morph_tagger)
                        doc.tag_ner(self.ner_tagger)
                    search_terms = {'lemmas': [], 'entities': []}
                    try:
                        search_terms['lemmas'] = [
                            self.morph.parse(token.text)[0].normal_form
                            for token in doc.tokens if token and len(token.text) > 2
                        ]
                    except Exception as e:
                        logger.warning(f"Ошибка при лемматизации русского текста: {e}")

                    try:
                        if doc.spans:
                            search_terms['entities'] = [span.normal for span in doc.spans if span and span.normal]
                    except Exception as e:
                        logger.warning(f"Ошибка при извлечении сущностей русского текста: {e}")
                else:
                    doc = self.nlp_en(search_text)
                    search_terms = {'lemmas': [], 'entities': []}
                    try:
                        search_terms['lemmas'] = [
                            self.lemmatizer.lemmatize(token)
                            for token in word_tokenize(search_text) if token and len(token) > 2
                        ]
                    except Exception as e:
                        logger.warning(f"Ошибка при лемматизации английского текста: {e}")

                    try:
                        search_terms['entities'] = [ent.text for ent in doc.ents]
                    except Exception as e:
                        logger.warning(f"Ошибка при извлечении сущностей английского текста: {e}")

                text_conditions = [
                    {"path": {"$regex": search_text, "$options": "i"}},
                    {"normalized_text": {"$regex": normalize_text(search_text), "$options": "i"}}
                ]
                if search_terms['lemmas']:
                    text_conditions.append({"lemmas": {"$in": search_terms['lemmas']}})
                if search_terms['entities']:
                    text_conditions.append({"entities.normal": {"$in": search_terms['entities']}})

                for token in tokenize_text(search_text):
                    norm_token = normalize_text(token)
                    lemma = self.morph.parse(token)[0].normal_form if contains_cyrillic(token) else self.lemmatizer.lemmatize(token)
                    token_condition = {
                        "$or": [
                            {"path": {"$regex": token, "$options": "i"}},
                            {"metadata": {"$regex": token, "$options": "i"}},
                            {"keywords": {"$in": [token.lower()]}},
                            {"path_parts": {"$in": [re.compile(token, re.IGNORECASE)]}},
                            {"indexed_parts.text": {"$regex": token, "$options": "i"}},
                            {"normalized_text": {"$regex": norm_token, "$options": "i"}},
                            {"lemmas": lemma}
                        ]
                    }
                    text_conditions.append(token_condition)

                query["$or"] = text_conditions

            results = list(self.collection.find(query).limit(50))
            logger.debug(f"Найдено {len(results)} результатов по запросу: {sanitized_params}")
            valid_results = [r for r in results if isinstance(r, dict) and "path" in r]
            return valid_results
        except Exception as e:
            logger.error(f"Ошибка при поиске: {str(e)}")
            return []

    def _enhance_metadata(self, path: str) -> Optional[dict]:
        """Улучшенный парсинг пути с поддержкой русского и английского."""
        if not path:
            logger.warning("Пустой путь для обработки метаданных")
            return None

        try:
            base_metadata = self.parse_path(path)
            if not base_metadata:
                logger.warning(f"Не удалось получить базовые метаданные для: {path}")
                return None

            if contains_cyrillic(path):
                try:
                    doc = Doc(path)
                    doc.segment(self.segmenter)
                    if doc.tokens:
                        doc.tag_morph(self.morph_tagger)
                        doc.tag_ner(self.ner_tagger)
                    else:
                        logger.debug(f"Сегментация не нашла токенов для пути: {path}")
                except Exception as e:
                    logger.error(f"Ошибка NLP обработки русского текста: {e}")
                    return base_metadata

                entities = []
                try:
                    if hasattr(doc, 'spans') and doc.spans is not None:
                        for span in doc.spans:
                            if span and hasattr(span, 'normalize'):
                                span.normalize(self.morph_vocab)
                                entities.append({
                                    "type": getattr(span, 'type', 'unknown'),
                                    "text": getattr(span, 'text', ''),
                                    "normal": getattr(span, 'normal', '')
                                })
                    else:
                        logger.debug(f"NER не нашел сущностей для пути: {path}")
                except Exception as e:
                    logger.warning(f"Ошибка при обработке сущностей русского текста: {e}")

                lemmas = []
                try:
                    parts = [part for part in re.split(r'[/\\]', path) if part]
                    for word in parts:
                        if word and len(word) > 2:
                            parsed = self.morph.parse(word)
                            if parsed and len(parsed) > 0:
                                lemmas.append(parsed[0].normal_form)
                except Exception as e:
                    logger.warning(f"Ошибка при лемматизации русского текста: {e}")
            else:
                try:
                    tokens = word_tokenize(path)
                    lemmas = [self.lemmatizer.lemmatize(token) for token in tokens if token and len(token) > 2]
                    doc = self.nlp_en(path)
                    entities = [
                        {
                            "type": ent.label_,
                            "text": ent.text,
                            "normal": ent.text
                        }
                        for ent in doc.ents
                    ]
                except Exception as e:
                    logger.error(f"Ошибка NLP обработки английского текста: {e}")
                    return base_metadata

            base_metadata.update({
                "entities": entities,
                "lemmas": lemmas
            })
            return base_metadata
        except Exception as e:
            logger.error(f"Ошибка при обработке метаданных: {str(e)}")
            return None

    def parse_path(self, path: str) -> Dict[str, Any]:
        """Парсинг пути для извлечения метаданных."""
        normalized_path = path.replace('\\', '/')
        parts = normalized_path.split('/')
        indexed_parts = [
            {"text": part, "index": i, "type": "directory" if i < len(parts) - 1 else "file"}
            for i, part in enumerate(parts) if part
        ]
        if indexed_parts and indexed_parts[-1]["type"] == "file":
            name_without_ext = os.path.splitext(indexed_parts[-1]["text"])[0]
            if name_without_ext != indexed_parts[-1]["text"]:
                indexed_parts.append({"text": name_without_ext, "index": len(parts) - 1, "type": "filename_without_ext"})

        metadata = {
            "bank": "", "vendor": "", "equipment": "", "metadata": "",
            "path": path, "path_parts": parts, "indexed_parts": indexed_parts,
            "created_at": datetime.now(), "keywords": [], "normalized_text": ""
        }
        path_lower = path.lower()
        banks = ["втб", "сбербанк", "альфа", "газпромбанк", "т-банк", "мкб", "рсхб", "sberbank", "vtb", "alfa"]
        vendors = ["grg", "wincor", "ncr", "diebold", "oki", "nautilus"]
        equipment_types = ["банкомат", "терминал", "оборудование", "проект", "atm", "terminal"]

        for bank in banks:
            if bank in path_lower:
                metadata["bank"] = bank
                break
        for vendor in vendors:
            if vendor in path_lower:
                metadata["vendor"] = vendor
                break
        for eq_type in equipment_types:
            if eq_type in path_lower:
                metadata["equipment"] = eq_type
                break

        filename = os.path.basename(path)
        name_without_ext = os.path.splitext(filename)[0]
        metadata["metadata"] = name_without_ext
        keywords = set()
        for part in parts:
            if len(part) > 2:
                keywords.add(part.lower())
                keywords.update(word.lower() for word in re.split(r'[\s_\-]+', part) if len(word) > 2)
        if name_without_ext:
            keywords.add(name_without_ext.lower())
            keywords.update(word.lower() for word in re.split(r'[\s_\-]+', name_without_ext) if len(word) > 2)
        metadata["keywords"] = list(keywords)
        normalized_text_parts = [normalize_text(part) for part in parts] + [normalize_text(name_without_ext)]
        metadata["normalized_text"] = " ".join(filter(None, normalized_text_parts))
        return metadata

    def load_from_xlsx(self, xlsx_path: str) -> int:
        """Загрузка путей из XLSX-файла."""
        if not self.ensure_connected():
            logger.error("Нет подключения к MongoDB перед загрузкой XLSX")
            return 0
            
        try:
            if not os.path.isfile(xlsx_path):
                logger.error(f"XLSX-файл не найден: {xlsx_path}")
                return 0
                
            if not os.access(xlsx_path, os.R_OK):
                logger.error(f"Нет прав на чтение файла: {xlsx_path}")
                return 0

            logger.info(f"Начало загрузки из файла: {xlsx_path}")
            df = pd.read_excel(xlsx_path, engine='openpyxl', header=None)
            if df.empty:
                logger.warning("XLSX файл пуст")
                return 0

            paths_column = df.iloc[:, 0]
            if paths_column.empty:
                logger.warning("Колонка с путями пуста")
                return 0

            count = 0
            for path in paths_column:
                if pd.notna(path) and str(path).strip():
                    original_path = str(path).strip()
                    if not original_path.startswith('#'):
                        logger.debug(f"Обрабатываем путь: {original_path}")
                        normalized_path = normalize_path(original_path)
                        if not normalized_path:
                            logger.warning(f"Путь отклонен после нормализации: {original_path}")
                            continue
                        if self.add_path(normalized_path):
                            count += 1
                        else:
                            logger.warning(f"Не удалось добавить путь: {normalized_path}")
                            
            logger.info(f"Загружено {count} путей из XLSX-файла")
            return count
        except Exception as e:
            logger.error(f"Ошибка при загрузке из XLSX: {e}")
            return 0

    def close(self):
        """Закрытие соединения с MongoDB."""
        if self.client:
            self.client.close()
            logger.info("Соединение с MongoDB закрыто")

# Инициализация MongoDB
logger.debug("Инициализация DatabaseHandler")
try:
    db_handler = DatabaseHandler(MONGODB_URI, MONGODB_DB, MONGODB_COLLECTION)
except Exception as e:
    logger.error(f"Ошибка при создании DatabaseHandler: {e}")
    sys.exit(1)

# Обработка сообщений из RabbitMQ
def callback(ch, method, properties, body):
    try:
        message = json.loads(body.decode())
        command = message.get("command")
        response = {"status": "error", "message": "Unknown command"}

        if command == "ADD":
            path = message.get("path")
            if path and db_handler.add_path(path):
                response = {"status": "success"}

        elif command == "REMOVE":
            path = message.get("path")
            if path and db_handler.remove_path(path):
                response = {"status": "success"}
            else:
                response = {"status": "error", "message": "Path not found"}

        elif command == "QUERY":
            query_type = message.get("type")
            query_value = message.get("value")
            if query_type and query_value:
                results = db_handler.search_paths({query_type: query_value})
                response = {"status": "success", "results": [r["path"] for r in results]}

        elif command == "ENHANCED_QUERY":
            query_type = message.get("type")
            query_value = message.get("value")
            normalized = message.get("normalized", "")
            expanded = message.get("expanded", [])
            if query_type and query_value:
                query_params = {"type": query_type, "value": query_value, "normalized": normalized, "expanded": expanded}
                results = db_handler.search_paths({query_type: query_value})
                response = {"status": "success", "results": [r["path"] for r in results]}

        if ch.is_open:
            ch.basic_publish(
                exchange='',
                routing_key=properties.reply_to,
                properties=pika.BasicProperties(correlation_id=properties.correlation_id),
                body=json.dumps(response).encode()
            )
        ch.basic_ack(delivery_tag=method.delivery_tag)
        logger.info(f"Обработано: {command}, ответ: {response.get('status')}")
    except Exception as e:
        logger.error(f"Ошибка обработки сообщения RabbitMQ: {e}")
        error_response = {"status": "error", "message": str(e)}
        if ch.is_open:
            ch.basic_publish(
                exchange='',
                routing_key=properties.reply_to,
                properties=pika.BasicProperties(correlation_id=properties.correlation_id),
                body=json.dumps(error_response).encode()
            )
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

# Настройка и запуск RabbitMQ
def start_consuming():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    time.sleep(15)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        virtual_host=RABBITMQ_VHOST,
        credentials=credentials,
        heartbeat=600,
        socket_timeout=15,
        retry_delay=5,
        connection_attempts=5
    )

    while True:
        try:
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            channel.queue_declare(queue=QUEUE_NAME, durable=True)
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)
            logger.info(f"Запуск потребления из очереди {QUEUE_NAME}")
            channel.start_consuming()
        except Exception as e:
            logger.error(f"Ошибка RabbitMQ: {e}")
            time.sleep(5)

if __name__ == "__main__":
    logger.info("Запуск consumer.py")
    try:
        count = db_handler.load_from_xlsx(XLSX_PATH)
        logger.info(f"Загружено {count} путей из XLSX")
        start_consuming()
    except KeyboardInterrupt:
        logger.info("Получен сигнал прерывания, завершение работы")
    except Exception as e:
        logger.critical(f"Критическая ошибка: {e}", exc_info=True)
    finally:
        logger.info("Завершение работы consumer.py")
        db_handler.close()