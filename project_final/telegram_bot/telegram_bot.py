#!/usr/bin/env python3

import os
import sys
import json
import uuid
import logging
import asyncio
import signal
from typing import Dict, Optional, Any
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton
from dotenv import load_dotenv
import colorlog
from pymorphy3 import MorphAnalyzer
from redis.asyncio import Redis
import aio_pika

# Загрузка переменных окружения
load_dotenv()

# Настройка логирования
log_directory = os.getenv("LOG_DIRECTORY", "./logs")
if not os.path.exists(log_directory):
    os.makedirs(log_directory, exist_ok=True)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

console_handler = colorlog.StreamHandler()
console_handler.setFormatter(colorlog.ColoredFormatter(
    '%(log_color)s%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    log_colors={'DEBUG': 'cyan', 'INFO': 'green', 'WARNING': 'yellow', 'ERROR': 'red'}
))
logger.addHandler(console_handler)

file_handler = logging.FileHandler(f"{log_directory}/telegram_bot.log")
file_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(name)s'))
logger.addHandler(file_handler)

# Проверяем настройки
logger.debug("Это отладочное сообщение при инициализации логгера")

# Переменные окружения
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", 5672))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "admin")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "rabbitpass")
RABBITMQ_VHOST = os.getenv("RABBITMQ_VHOST", "/")
QUEUE_NAME = os.getenv("QUEUE_NAME", "path_queue")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

if not BOT_TOKEN:
    logger.critical("BOT_TOKEN не задан!")
    sys.exit(1)

ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
ALLOWED_USERS_STR = os.getenv("ALLOWED_USERS", "")
try:
    ALLOWED_USERS = [int(id.strip()) for id in ALLOWED_USERS_STR.split(",") if id.strip() and not id.strip().startswith('#')]
except ValueError as e:
    logger.error(f"Ошибка в формате ALLOWED_USERS: {e}")
    ALLOWED_USERS = []

if ADMIN_ID and ADMIN_ID not in ALLOWED_USERS:
    ALLOWED_USERS.append(ADMIN_ID)

# Инициализация бота
storage = MemoryStorage()
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=storage)

# Класс для NLP обработки
class QueryProcessor:
    def __init__(self):
        self.morph = MorphAnalyzer()
        self.redis = None
        self.redis_connected = False

    async def connect_redis(self):
        if not self.redis_connected:
            try:
                self.redis = await Redis.from_url(REDIS_URL)
                self.redis_connected = True
                logger.debug("Подключено к Redis")
            except Exception as e:
                logger.error(f"Ошибка подключения к Redis: {e}")
                self.redis_connected = False

    async def normalize_query(self, text: str) -> str:
        """Нормализация запроса с кэшированием."""
        await self.connect_redis()
        cache_key = f"norm:{text}"
        if self.redis_connected:
            try:
                if cached := await self.redis.get(cache_key):
                    return cached.decode()
            except Exception as e:
                logger.warning(f"Ошибка получения из кэша: {e}")

        words = text.lower().split()
        normalized = [self.morph.parse(word)[0].normal_form for word in words]
        result = " ".join(normalized)
        if self.redis_connected:
            try:
                await self.redis.setex(cache_key, 3600, result)
            except Exception as e:
                logger.warning(f"Ошибка сохранения в кэш: {e}")
        return result

    async def expand_query(self, text: str) -> list:
        """Расширение запроса с формами слов."""
        expanded = set()
        for word in text.split():
            parsed = self.morph.parse(word)[0]
            expanded.update({f.word for f in parsed.lexeme})
            expanded.add(parsed.normal_form)
        return list(expanded)

# Класс для работы с RabbitMQ
class RabbitClient:
    def __init__(self):
        self.connection = None
        self.channel = None

    async def _connect(self):
        try:
            self.connection = await aio_pika.connect_robust(
                host=RABBITMQ_HOST,
                port=RABBITMQ_PORT,
                login=RABBITMQ_USER,
                password=RABBITMQ_PASS,
                virtualhost=RABBITMQ_VHOST
            )
            self.channel = await self.connection.channel()
            await self.channel.declare_queue(QUEUE_NAME, durable=True)
            logger.debug("Подключено к RabbitMQ")
        except Exception as e:
            logger.error(f"Ошибка подключения к RabbitMQ: {e}")

    async def call(self, command: str, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            if not self.connection or self.connection.is_closed:
                await self._connect()
                if not self.connection or self.connection.is_closed:
                    return None
                
            message = {"command": command, **data}
            correlation_id = str(uuid.uuid4())
            
            reply_queue = await self.channel.declare_queue("", exclusive=True)
            
            await self.channel.default_exchange.publish(
                aio_pika.Message(
                    body=json.dumps(message).encode(),
                    reply_to=reply_queue.name,
                    correlation_id=correlation_id,
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                ),
                routing_key=QUEUE_NAME
            )
            
            try:
                async with asyncio.timeout(10):  # 10 секунд таймаут
                    async with reply_queue.iterator() as queue_iter:
                        async for message in queue_iter:
                            async with message.process():
                                if message.correlation_id == correlation_id:
                                    return json.loads(message.body.decode())
            except asyncio.TimeoutError:
                logger.warning(f"Таймаут ожидания ответа от RabbitMQ для команды {command}")
                return None
                
        except Exception as e:
            logger.error(f"Ошибка отправки в RabbitMQ: {e}")
            return None

    async def close(self):
        if self.connection and not self.connection.is_closed:
            await self.connection.close()
            logger.debug("Соединение с RabbitMQ закрыто")

rabbit_client = RabbitClient()
query_processor = QueryProcessor()

# Состояния FSM
class BotStates(StatesGroup):
    waiting_for_path = State()
    waiting_for_path_to_remove = State()
    waiting_for_search_query = State()
    waiting_for_bank = State()
    waiting_for_vendor = State()
    waiting_for_equipment = State()

# Клавиатуры
def get_main_keyboard() -> ReplyKeyboardMarkup:
    buttons = [
        [KeyboardButton(text="🔍 Поиск")],
        [KeyboardButton(text="➕ Добавить путь"), KeyboardButton(text="➖ Удалить путь")],
        [KeyboardButton(text="🔎 Расширенный поиск")],
        [KeyboardButton(text="ℹ️ Помощь")]
    ]
    keyboard = ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)
    logger.debug("Клавиатура создана успешно")
    return keyboard

  
def get_advanced_search_keyboard() -> ReplyKeyboardMarkup:
    buttons = [
        [KeyboardButton(text="🏦 Поиск по банку")],
        [KeyboardButton(text="🏭 Поиск по вендору")],
        [KeyboardButton(text="🔧 Поиск по оборудованию")],
        [KeyboardButton(text="↩️ Назад")]
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)

# Проверка доступа
def is_user_allowed(user_id: int) -> bool:
    allowed = user_id in ALLOWED_USERS or user_id == ADMIN_ID
    if not allowed:
        logger.warning(f"Доступ запрещен для user_id {user_id}")
    return allowed

# Обработчики
@dp.message(Command("start"))
async def cmd_start(message: Message):
    user_id = message.from_user.id
    if not is_user_allowed(user_id):
        await message.answer("⛔ У вас нет доступа.")
        return
    await message.answer(
        f"👋 Привет, {message.from_user.first_name}!\nЯ бот для работы с путями.",
        reply_markup=get_main_keyboard()
    )
    logger.debug(f"Старт для пользователя {user_id}")

@dp.message(Command("help"))
@dp.message(F.text == "ℹ️ Помощь")
async def cmd_help(message: Message, state: FSMContext):
    if not is_user_allowed(message.from_user.id):
        await message.answer("⛔ У вас нет доступа.")
        return
    help_text = (
        "🤖 Справка\n\n"
        "Команды:\n"
        "• /start - Запуск\n"
        "• /help - Справка\n\n"
        "Поиск:\n"
        "• 🔍 Поиск по тексту\n"
        "• 🔎 Расширенный поиск (банк/вендор/оборудование)\n\n"
        "Для админа:\n"
        "• ➕ Добавить путь\n"
        "• ➖ Удалить путь"
    )
    await message.answer(help_text, parse_mode="HTML", reply_markup=get_main_keyboard())
    await state.clear()

@dp.message(F.text == "🔍 Поиск")
async def cmd_search(message: Message, state: FSMContext):
    if not is_user_allowed(message.from_user.id):
        await message.answer("⛔ У вас нет доступа.")
        return
    await state.set_state(BotStates.waiting_for_search_query)
    buttons = [[KeyboardButton(text="↩️ Отмена")]]
    await message.answer("🔍 Введите текст:", reply_markup=ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True))

@dp.message(BotStates.waiting_for_search_query)
async def process_search_query(message: Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "↩️ Отмена":
        await state.clear()
        await message.answer("❌ Поиск отменен.", reply_markup=get_main_keyboard())
        return

    query_text = message.text.strip()
    if not query_text:
        await message.answer("⚠️ Запрос не может быть пустым.")
        return

    processing_msg = await message.answer("🔄 Обрабатываю запрос...")
    try:
        normalized = await query_processor.normalize_query(query_text)
        expanded = await query_processor.expand_query(normalized)
        response = await rabbit_client.call("ENHANCED_QUERY", {
            "type": "text",
            "value": query_text,
            "normalized": normalized,
            "expanded": expanded,
            "user_id": str(user_id)
        })
        await bot.delete_message(chat_id=message.chat.id, message_id=processing_msg.message_id)
        if response and response.get("status") == "success":
            results = response.get("results", [])
            if results:
                result_text = f"🔍 Найдено {len(results)} результатов:\n\n"
                for i, path in enumerate(results[:15], 1):
                    result_text += f"{i}. {path}\n"
                await message.answer(result_text, reply_markup=get_main_keyboard())
            else:
                await message.answer("😕 Ничего не найдено.", reply_markup=get_main_keyboard())
        else:
            await message.answer("⚠️ Ошибка при поиске.", reply_markup=get_main_keyboard())
    except Exception as e:
        logger.error(f"Ошибка при поиске: {e}")
        await message.answer("⚠️ Произошла ошибка.", reply_markup=get_main_keyboard())
    await state.clear()

@dp.message(F.text == "🔎 Расширенный поиск")
async def cmd_advanced_search(message: Message, state: FSMContext):
    if not is_user_allowed(message.from_user.id):
        await message.answer("⛔ У вас нет доступа.")
        return
    await message.answer("🔎 Выберите тип поиска:", reply_markup=get_advanced_search_keyboard())

@dp.message(F.text == "🏦 Поиск по банку")
async def cmd_search_by_bank(message: Message, state: FSMContext):
    if not is_user_allowed(message.from_user.id):
        await message.answer("⛔ У вас нет доступа.")
        return
    await state.set_state(BotStates.waiting_for_bank)
    buttons = [[KeyboardButton(text="↩️ Отмена")]]
    await message.answer("🏦 Введите название банка:", reply_markup=ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True))

@dp.message(BotStates.waiting_for_bank)
async def process_bank_search(message: Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "↩️ Отмена":
        await state.clear()
        await message.answer("❌ Поиск отменен.", reply_markup=get_main_keyboard())
        return

    bank_query = message.text.strip()
    if not bank_query:
        await message.answer("⚠️ Название банка не может быть пустым.")
        return

    processing_msg = await message.answer("🔄 Обрабатываю запрос...")
    try:
        normalized = await query_processor.normalize_query(bank_query)
        response = await rabbit_client.call("ENHANCED_QUERY", {
            "type": "bank",
            "value": bank_query,
            "normalized": normalized,
            "user_id": str(user_id)
        })
        await bot.delete_message(chat_id=message.chat.id, message_id=processing_msg.message_id)
        if response and response.get("status") == "success":
            results = response.get("results", [])
            if results:
                result_text = f"🔍 Найдено {len(results)} результатов по банку '{bank_query}':\n\n"
                for i, path in enumerate(results[:15], 1):
                    result_text += f"{i}. {path}\n"
                await message.answer(result_text, reply_markup=get_main_keyboard())
            else:
                await message.answer("😕 Ничего не найдено.", reply_markup=get_main_keyboard())
        else:
            await message.answer("⚠️ Ошибка при поиске.", reply_markup=get_main_keyboard())
    except Exception as e:
        logger.error(f"Ошибка при поиске по банку: {e}")
        await message.answer("⚠️ Произошла ошибка.", reply_markup=get_main_keyboard())
    await state.clear()

@dp.message(F.text == "↩️ Назад")
async def cmd_back(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Вернулись в главное меню", reply_markup=get_main_keyboard())

# Добавляем обработчики сигналов
def signal_handler():
    logger.debug("Получен сигнал завершения, закрываем соединения...")
    asyncio.create_task(shutdown())

async def shutdown():
    await rabbit_client.close()
    await bot.session.close()
    # Завершаем все задачи
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    logger.debug("Соединения закрыты, завершение работы")

async def main():
    # Инициализируем подключения
    try:
        await rabbit_client._connect()
    except Exception as e:
        logger.error(f"Ошибка при инициализации подключений: {e}")
    
    # Запускаем бота
    try:
        logger.debug("Запуск бота...")
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"Ошибка при запуске бота: {e}")
    finally:
        # Закрываем соединения при завершении
        await rabbit_client.close()

if __name__ == "__main__":
    try:
        # Регистрируем обработчики сигналов
        for sig in (signal.SIGINT, signal.SIGTERM):
            asyncio.get_event_loop().add_signal_handler(sig, signal_handler)
        
        # Запускаем основную функцию
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.debug("Получен сигнал прерывания, завершение работы")
    except Exception as e:
        logger.error(f"Необработанное исключение: {e}")