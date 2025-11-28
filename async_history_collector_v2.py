# async_history_collector_v2.py
"""
Асинхронный клиент Alpha Domain с разделением пулов соединений.
Версия 2.0 с интеграцией 'rich' для логирования и вывода.
"""

import asyncio
import csv
import os
import logging
import traceback
from datetime import datetime, timedelta, timezone
from typing import List, Optional
from dataclasses import dataclass

# Библиотеки Alpha и Rich
import alpha_domain_pyclient as ng
from rich.logging import RichHandler
from rich.table import Table
from rich.panel import Panel
from rich.prompt import Prompt, Confirm
from rich.console import Console

# Импорт конфигураций
try:
    import config
    from config import (
        PoolZone, ConnectionConfig, PoolConfig,
        RUNTIME_CONFIG, HISTORIAN_CONFIG, POOL_CONFIG,
        LOG_BASE_PATH, SUBSCRIPTION_DURATION_SEC, HISTORY_READ_HOURS_AGO
    )
except ImportError:
    print("Ошибка: Не найден файл config.py. Пожалуйста, создайте его.")
    exit(1)


# ============================================================================
# НАСТРОЙКА ЛОГИРОВАНИЯ
# ============================================================================

# 1. Создаем главный console-объект, ОТКЛЮЧИВ цвета
console = Console(no_color=True)

# 2. Настраиваем логгер, чтобы он использовал этот монохромный console-объект
logging.basicConfig(
    level="INFO",
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(console=console, rich_tracebacks=True, show_path=False)]
)

log = logging.getLogger("rich")
# 'console' уже определен выше


# ============================================================================
# ЛОГИРОВАНИЕ
# ============================================================================

class AsyncDataLogger:
    """Асинхронный логгер данных с записью в CSV"""
    
    def __init__(self, base_path: str = "./logs"):
        os.makedirs(base_path, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.data_file = os.path.join(base_path, f"alpha_async_data_{timestamp}.csv")
        self._lock = asyncio.Lock()
        self._initialize_file()
        
    def _initialize_file(self):
        """Инициализация CSV файла"""
        try:
            with open(self.data_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'Time', 'Zone', 'ItemName', 'Value', 
                    'Quality', 'Operation', 'Status', 'LocalTime', 'ConnectionId'
                ])
            log.info(f"Файл логов данных CSV: {self.data_file}")
        except OSError as e:
            log.error(f"Не удалось создать CSV-лог: {e}")
            self.data_file = None
    
    async def write_data(
        self, 
        zone: PoolZone,
        item_name: str, 
        value: any, 
        quality: int,
        operation: str, 
        status: str,
        local_time: Optional[str] = None,
        connection_id: Optional[str] = None
    ):
        """Асинхронная запись данных"""
        if not self.data_file:
            return # Не пишем, если файл не был создан

        async with self._lock:
            try:
                with open(self.data_file, 'a', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow([
                        datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
                        zone.value,
                        item_name,
                        value,
                        quality,
                        operation,
                        status,
                        local_time or "",
                        connection_id or ""
                    ])
            except Exception as e:
                log.warning(f"Ошибка записи в CSV-лог: {e}")


class AlphaLogger(ng.ILogger):
    """
    Логгер для Alpha Domain SDK.
    Перенаправляет сообщения SDK в основной логгер 'rich'.
    """
    
    def __init__(self, zone: PoolZone):
        self.zone = zone
        
    def report(self, msg_type, msg):
        zone_str = f"[{self.zone.value} SDK]"
        if msg_type == ng.LogMsgType.ERROR:
            log.error(f"{zone_str} {msg}")
        elif msg_type == ng.LogMsgType.WARNING:
            log.warning(f"{zone_str} {msg}")
        # Игнорируем INFO сообщения от SDK, чтобы не засорять лог


# ============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================================

def convert_alpha_time_to_local(alpha_datetime) -> datetime:
    """Конвертация времени Alpha.Server в локальное (MSK)"""
    try:
        python_dt = alpha_datetime.to_datetime()
        if python_dt.tzinfo is None:
            utc_dt = python_dt.replace(tzinfo=timezone.utc)
        else:
            utc_dt = python_dt
        
        # Предполагаем MSK, можно вынести в конфиг
        local_timezone = timezone(timedelta(hours=3))
        local_dt = utc_dt.astimezone(local_timezone)
        return local_dt
    except Exception as e:
        log.warning(f"Ошибка конвертации времени: {e}")
        return alpha_datetime.to_datetime()


def create_alpha_datetime_with_timezone(dt: datetime):
    """Создание Alpha DateTime с учётом часового пояса"""
    try:
        # Предполагаем MSK, можно вынести в конфиг
        if dt.tzinfo is None:
            local_timezone = timezone(timedelta(hours=3))
            dt_with_tz = dt.replace(tzinfo=local_timezone)
        else:
            dt_with_tz = dt
        
        utc_dt = dt_with_tz.astimezone(timezone.utc)
        # Alpha DateTime ожидает naive datetime в UTC
        return ng.DateTime(utc_dt.replace(tzinfo=None))
    except Exception as e:
        log.warning(f"Ошибка создания Alpha DateTime: {e}")
        return ng.DateTime(dt)


def get_user_input_tags() -> List[str]:
    """Интерактивный ввод имён тегов с использованием Rich"""
    console.print("\n" + "="*60)
    console.print(" ВВОД ИМЁН ТЕГОВ")
    console.print("="*60)
    
    console.print("\nВведите имена тегов для тестирования.")
    console.print("Примеры: PLC1.Tag1, Device.Sensor.Temperature")
    console.print("Можно вводить через запятую или каждый с новой строки.")
    console.print("Для завершения ввода нажмите Enter на пустой строке.")
    
    tags = []
    
    first_input = Prompt.ask(
        "Введите теги (через запятую или Enter для построчного ввода)"
    ).strip()
    
    if first_input:
        tags = [tag.strip() for tag in first_input.split(',') if tag.strip()]
    else:
        log.info("Включен построчный ввод. Пустая строка для завершения.")
        while True:
            tag = Prompt.ask("  Тег").strip()
            if not tag:
                break
            tags.append(tag)
    
    if tags:
        log.info(f"Будут использованы теги ({len(tags)}):")
        for i, tag in enumerate(tags, 1):
            console.print(f"  {i}. {tag}")
    else:
        log.warning("Теги не введены. Тестирование будет пропущено.")
    
    return tags


# ============================================================================
# УПРАВЛЕНИЕ СОЕДИНЕНИЯМИ (Логика без изменений)
# ============================================================================

class Connection:
    """Обёртка над соединением с узлом"""
    
    def __init__(
        self, 
        node,
        config: ConnectionConfig,
        connection_id: str
    ):
        self.node = node
        self.config = config
        self.connection_id = connection_id
        self.is_busy = False
        self.created_at = datetime.now()
        self.last_used = datetime.now()
        self._lock = asyncio.Lock()
    
    async def acquire(self) -> bool:
        """Захват соединения"""
        async with self._lock:
            if not self.is_busy:
                self.is_busy = True
                self.last_used = datetime.now()
                return True
            return False
    
    async def release(self):
        """Освобождение соединения"""
        async with self._lock:
            self.is_busy = False
            self.last_used = datetime.now()
    
    def is_connected(self) -> bool:
        """Проверка соединения"""
        if isinstance(self.node, ng.RuntimeNode):
            return self.node.is_connected()
        return True  # HistoryNode не имеет метода is_connected
    
    async def disconnect(self):
        """Отключение"""
        try:
            if isinstance(self.node, ng.RuntimeNode):
                self.node.disconnect()
        except Exception as e:
            log.warning(f"[{self.config.zone.value}] Ошибка отключения {self.connection_id}: {e}")


class ConnectionPool:
    """Пул соединений для определённой зоны ответственности"""
    
    def __init__(
        self,
        zone: PoolZone,
        connection_config: ConnectionConfig,
        pool_config: PoolConfig,
        tag_service: ng.TagService
    ):
        self.zone = zone
        self.connection_config = connection_config
        self.pool_config = pool_config
        self.tag_service = tag_service
        
        self._connections: List[Connection] = []
        self._lock = asyncio.Lock()
        self._connection_counter = 0
        self._initialized = False
        
        log.debug(f"Создан пул [{zone.value}] для {connection_config.host}:{connection_config.port}")
    
    async def initialize(self):
        """Инициализация пула с минимальным количеством соединений"""
        async with self._lock:
            if self._initialized:
                return
            
            log.info(f"Инициализация пула [{self.zone.value}]...")
            
            for _ in range(self.pool_config.min_connections):
                try:
                    connection = await self._create_connection()
                    if connection:
                        self._connections.append(connection)
                except Exception as e:
                    log.warning(f"[{self.zone.value}] Ошибка создания соединения при инициализации: {e}")
            
            self._initialized = True
            log.info(f"Пул [{self.zone.value}] инициализирован: {len(self._connections)} соединений")
    
    async def _create_connection(self) -> Optional[Connection]:
        """Создание нового соединения"""
        try:
            unit = self.tag_service.create_unit()
            endpoint = ng.TcpEndpoint(
                self.connection_config.host, 
                self.connection_config.port
            )
            
            logger = AlphaLogger(self.zone)
            
            if self.zone == PoolZone.RUNTIME:
                settings = ng.ServerSettings(
                    self.connection_config.timeout, 
                    endpoint
                )
                node = unit.create_node_connection(settings, logger)
                
                for attempt in range(self.pool_config.retry_attempts):
                    try:
                        node.connect()
                        if node.is_connected():
                            break
                    except Exception as e:
                        if attempt < self.pool_config.retry_attempts - 1:
                            await asyncio.sleep(self.pool_config.retry_delay)
                        else:
                            raise e
                
                if not node.is_connected():
                    raise ConnectionError("Не удалось подключиться к Runtime")
                    
            else:  # HISTORIAN
                settings = ng.HistorianSettings(
                    "default",
                    [endpoint]
                )
                node = unit.create_node_connection(settings, logger)
            
            self._connection_counter += 1
            connection_id = f"{self.zone.value}-{self._connection_counter}"
            
            connection = Connection(node, self.connection_config, connection_id)
            log.info(f"[{self.zone.value}] Создано соединение: {connection_id}")
            
            return connection
            
        except Exception as e:
            log.error(f"[{self.zone.value}] Критическая ошибка создания соединения: {e}")
            return None
    
    async def acquire(self, timeout: float = 30.0) -> Optional[Connection]:
        """Получение свободного соединения из пула"""
        start_time = asyncio.get_event_loop().time()
        
        while (asyncio.get_event_loop().time() - start_time) < timeout:
            async with self._lock:
                # Поиск свободного соединения
                for conn in self._connections:
                    if await conn.acquire():
                        if conn.is_connected():
                            return conn
                        else:
                            log.warning(f"[{self.zone.value}] Соединение {conn.connection_id} потеряно. Удаление из пула.")
                            await conn.release()
                            self._connections.remove(conn)
                
                if len(self._connections) < self.pool_config.max_connections:
                    new_conn = await self._create_connection()
                    if new_conn and await new_conn.acquire():
                        self._connections.append(new_conn)
                        return new_conn
            
            await asyncio.sleep(0.1)
        
        log.error(f"[{self.zone.value}] Timeout при получении соединения из пула")
        return None
    
    async def release(self, connection: Connection):
        """Возврат соединения в пул"""
        await connection.release()
    
    async def close_all(self):
        """Закрытие всех соединений"""
        async with self._lock:
            log.info(f"Закрытие пула [{self.zone.value}]...")
            for conn in self._connections:
                await conn.disconnect()
            self._connections.clear()
            log.info(f"Пул [{self.zone.value}] закрыт")
    
    def get_stats(self) -> dict:
        """Статистика пула"""
        busy_count = sum(1 for c in self._connections if c.is_busy)
        return {
            "zone": self.zone.value,
            "total": len(self._connections),
            "busy": busy_count,
            "free": len(self._connections) - busy_count,
            "max": self.pool_config.max_connections
        }


class ConnectionPoolManager:
    """Менеджер пулов соединений"""
    
    def __init__(self):
        self.pools: dict[PoolZone, ConnectionPool] = {}
        self.tag_service = ng.create_tag_service()
    
    async def create_pool(
        self,
        zone: PoolZone,
        connection_config: ConnectionConfig,
        pool_config: PoolConfig
    ):
        """Создание пула для зоны"""
        if zone in self.pools:
            log.warning(f"Пул для зоны {zone.value} уже существует.")
            return

        pool = ConnectionPool(
            zone,
            connection_config,
            pool_config,
            self.tag_service
        )
        await pool.initialize()
        self.pools[zone] = pool
    
    async def get_connection(self, zone: PoolZone) -> Optional[Connection]:
        """Получение соединения из пула"""
        pool = self.pools.get(zone)
        if not pool:
            log.error(f"Пул для зоны {zone.value} не найден")
            return None
        return await pool.acquire()
    
    async def release_connection(self, connection: Connection):
        """Возврат соединения"""
        pool = self.pools.get(connection.config.zone)
        if pool:
            await pool.release(connection)
    
    async def close_all(self):
        """Закрытие всех пулов"""
        for pool in self.pools.values():
            await pool.close_all()
    
    def display_pool_stats(self):
        """Вывод статистики всех пулов в виде таблицы Rich"""
        
        table = Table(title="Статистика Пулов Соединений", show_header=True)
        table.add_column("Зона (Zone)", width=12)
        table.add_column("Всего (Total)", justify="right")
        table.add_column("Занято (Busy)", justify="right")
        table.add_column("Свободно (Free)", justify="right")
        table.add_column("Максимум (Max)", justify="right")

        for pool in self.pools.values():
            stats = pool.get_stats()
            table.add_row(
                stats['zone'],
                str(stats['total']),
                str(stats['busy']),
                str(stats['free']),
                str(stats['max'])
            )
        
        console.print("\n", table, "\n")


# ============================================================================
# ОПЕРАЦИИ С RUNTIME
# ============================================================================

async def read_current_values(
    pool_manager: ConnectionPoolManager,
    logger: AsyncDataLogger,
    item_names: List[str]
) -> List[ng.Item]:
    """Чтение текущих значений из Runtime"""
    connection = await pool_manager.get_connection(PoolZone.RUNTIME)
    if not connection:
        raise RuntimeError("Не удалось получить Runtime соединение")
    
    try:
        log.info(f"[{connection.connection_id}] Получение элементов: {', '.join(item_names)}")
        
        # Получение элементов
        requested_items = connection.node.get_items(item_names)
        items = []
        failed_items = []
        
        # Инициализация элементов
        for item in requested_items:
            item.init()
            if item.status_info.item_status == ng.ItemStatus.INITIALIZED:
                log.info(f"  Успешно инициализирован: {item.name}")
                items.append(item)
            else:
                status_name = item.status_info.item_status.name
                log.warning(f"  Не удалось инициализировать: {item.name} (Статус: {status_name})")
                failed_items.append((item.name, status_name))
        
        if not items:
            log.error("Не удалось инициализировать ни одного элемента!")
            console.print("\nВозможные причины:")
            console.print("  1. Элементы не существуют в системе")
            console.print("  2. Неверный формат имени элемента (проверьте регистр и точки)")
            console.print("  3. Нет прав доступа к элементам")
            raise RuntimeError("Нет инициализированных элементов")
        
        log.info("Чтение текущих значений...")
        for item in items:
            try:
                vqt = item.get_value()
                log.info(f"  {item.name} = {vqt.value}")
                
                await logger.write_data(
                    PoolZone.RUNTIME,
                    item.name,
                    vqt.value,
                    vqt.quality,
                    "READ_CURRENT",
                    "SUCCESS",
                    connection_id=connection.connection_id
                )
            except Exception as e:
                log.error(f"  Ошибка чтения {item.name}: {e}")
                await logger.write_data(
                    PoolZone.RUNTIME,
                    item.name,
                    None,
                    0,
                    "READ_CURRENT",
                    f"ERROR: {e}",
                    connection_id=connection.connection_id
                )
        
        return items
        
    finally:
        await pool_manager.release_connection(connection)


async def subscribe_to_changes(
    pool_manager: ConnectionPoolManager,
    logger: AsyncDataLogger,
    items: List[ng.Item],
    duration: int
):
    """Подписка на изменения значений"""
    connection = await pool_manager.get_connection(PoolZone.RUNTIME)
    if not connection:
        raise RuntimeError("Не удалось получить Runtime соединение")
    
    try:
        log.info(f"[{connection.connection_id}] Подписка на изменения ({duration} сек)...")
        
        pipe = connection.node.subscribe_items_values_change(items)
        
        start_time = asyncio.get_event_loop().time()
        count = 0
        
        while (asyncio.get_event_loop().time() - start_time) < duration:
            status, item_value = pipe.try_fetch_next()
            
            if status == ng.PipeFetchStatus.SUCCESS and item_value:
                vqt = item_value.vqt
                global_id = item_value.global_item_id
                
                item_name = next(
                    (item.name for item in items 
                     if item.id.value == global_id.item_id.value),
                    str(global_id.item_id.value)
                )
                
                local_time = convert_alpha_time_to_local(vqt.timestamp)
                time_str = local_time.strftime('%Y-%m-%d %H:%M:%S')
                
                log.info(f"  Подписка: {item_name} = {vqt.value} @ {time_str}")
                
                await logger.write_data(
                    PoolZone.RUNTIME,
                    item_name,
                    vqt.value,
                    vqt.quality,
                    "SUBSCRIPTION",
                    "SUCCESS",
                    local_time=time_str,
                    connection_id=connection.connection_id
                )
                
                count += 1
                
            elif status == ng.PipeFetchStatus.DATA_IS_OVER:
                log.warning("Поток данных подписки иссяк.")
                break
            else:
                await asyncio.sleep(0.1) # Небольшая пауза
        
        pipe.close()
        log.info(f"Подписка завершена. Получено {count} обновлений.")
        
    finally:
        await pool_manager.release_connection(connection)


# ============================================================================
# ОПЕРАЦИИ С HISTORIAN
# ============================================================================

async def read_historical_data(
    pool_manager: ConnectionPoolManager,
    logger: AsyncDataLogger,
    item: ng.Item,
    start_dt: datetime,
    end_dt: datetime
) -> int:
    """Чтение исторических данных"""
    connection = await pool_manager.get_connection(PoolZone.HISTORIAN)
    if not connection:
        raise RuntimeError("Не удалось получить Historian соединение")
    
    try:
        log.info(f"[{connection.connection_id}] Запрос истории для {item.name}")
        log.info(f"  Период: {start_dt.strftime('%Y-%m-%d %H:%M')} -> "
                 f"{end_dt.strftime('%Y-%m-%d %H:%M')}")
        
        value_reader = connection.node.create_value_read_session()
        
        low_bound = ng.HistoryBound(
            create_alpha_datetime_with_timezone(start_dt),
            ng.HistoryBoundType.OUTER
        )
        high_bound = ng.HistoryBound(
            create_alpha_datetime_with_timezone(end_dt),
            ng.HistoryBoundType.OUTER
        )
        
        # Запрашиваем до 10000 записей (0 = все, но может быть опасно)
        history_pipe = value_reader.read(low_bound, high_bound, True, 10000, item)
        
        count = 0
        first_record_time = None
        last_record_time = None
        
        while not history_pipe.is_over():
            status, vqt = history_pipe.fetch_next(5000)
            
            if status == ng.PipeFetchStatus.SUCCESS and vqt:
                local_time = convert_alpha_time_to_local(vqt.timestamp)
                
                if first_record_time is None:
                    first_record_time = local_time
                last_record_time = local_time
                
                time_str = local_time.strftime('%Y-%m-%d %H:%M:%S')
                
                await logger.write_data(
                    PoolZone.HISTORIAN,
                    item.name,
                    vqt.value,
                    vqt.quality,
                    "HISTORY_READ",
                    "SUCCESS",
                    local_time=time_str,
                    connection_id=connection.connection_id
                )
                
                count += 1
                
                if count <= 5: # Логируем только первые 5 записей, чтобы не спамить
                    log.info(f"  Истор. запись: {vqt.value} @ {time_str}")
                
            elif status == ng.PipeFetchStatus.DATA_IS_OVER:
                break
            elif status == ng.PipeFetchStatus.NO_DATA_AVAILABLE:
                await asyncio.sleep(0.1)
            elif status == ng.PipeFetchStatus.TIMEOUT_EXCEEDED:
                log.warning(f"  Timeout при чтении истории для {item.name}")
                break
        
        history_pipe.close()
        
        if count > 5:
            log.info(f"  ... и еще {count - 5} записей.")
        
        log.info(f"Прочитано записей для {item.name}: {count}")
        if count > 0:
            log.info(f"  Первая запись: {first_record_time.strftime('%Y-%m-%d %H:%M:%S')}")
            log.info(f"  Последняя запись: {last_record_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        return count
        
    finally:
        await pool_manager.release_connection(connection)


async def parallel_historical_read(
    pool_manager: ConnectionPoolManager,
    logger: AsyncDataLogger,
    items: List[ng.Item],
    start_dt: datetime,
    end_dt: datetime
):
    """Параллельное чтение истории для нескольких элементов"""
    log.info(f"Запуск параллельного чтения истории для {len(items)} элементов...")
    
    tasks = [
        read_historical_data(pool_manager, logger, item, start_dt, end_dt)
        for item in items
    ]
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    total_records = 0
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            log.error(f"Ошибка при чтении истории {items[i].name}: {result}")
        else:
            total_records += result
    
    log.info(f"Параллельное чтение завершено. Всего записей: {total_records}")


# ============================================================================
# ДЕМОНСТРАЦИЯ ИЗОЛЯЦИИ
# ============================================================================

async def demonstrate_isolation(
    pool_manager: ConnectionPoolManager
):
    """Демонстрация изоляции Runtime и Historian пулов"""
    
    console.print("\n" + "="*60)
    console.print(" ДЕМОНСТРАЦИЯ ИЗОЛЯЦИИ ПУЛОВ")
    console.print("="*60, "\n")
    log.info("Запускаем 3 задачи параллельно:")
    log.info(" 1. [Runtime] Долгая операция (5 сек)")
    log.info(" 2. [Historian] Быстрая операция (1 сек), запуск через 1 сек")
    log.info(" 3. [Runtime] Вторая операция (2 сек), запуск через 2 сек")
    console.print("")

    # Задача 1: Длительная операция с Runtime
    async def long_runtime_task():
        log.info("[Task 1 | Runtime] Запуск...")
        connection = await pool_manager.get_connection(PoolZone.RUNTIME)
        if connection:
            try:
                log.info(f"[Task 1 | Runtime] Получено соединение: {connection.connection_id}")
                await asyncio.sleep(5)
                log.info(f"[Task 1 | Runtime] Операция ЗАВЕРШЕНА (соединение {connection.connection_id} было занято 5 сек)")
            finally:
                await pool_manager.release_connection(connection)
    
    # Задача 2: Быстрая операция с Historian (не должна ждать Task 1)
    async def quick_historian_task():
        await asyncio.sleep(1)
        log.info("[Task 2 | Historian] Запуск...")
        connection = await pool_manager.get_connection(PoolZone.HISTORIAN)
        if connection:
            try:
                log.info(f"[Task 2 | Historian] Получено соединение: {connection.connection_id}")
                await asyncio.sleep(1)
                log.info(f"[Task 2 | Historian] Операция ЗАВЕРШЕНА. Она не ждала Task 1.")
            finally:
                await pool_manager.release_connection(connection)
    
    # Задача 3: Ещё одна Runtime операция
    async def another_runtime_task():
        await asyncio.sleep(2)
        log.info("[Task 3 | Runtime] Запуск...")
        connection = await pool_manager.get_connection(PoolZone.RUNTIME)
        if connection:
            try:
                log.info(f"[Task 3 | Runtime] Получено соединение: {connection.connection_id}")
                await asyncio.sleep(2)
                log.info(f"[Task 3 | Runtime] Операция ЗАВЕРШЕНА. (Она могла ждать Task 1, если max_connections=1, или выполниться параллельно)")
            finally:
                await pool_manager.release_connection(connection)
    
    await asyncio.gather(
        long_runtime_task(),
        quick_historian_task(),
        another_runtime_task()
    )
    
    log.info("Демонстрация изоляции завершена.")


# ============================================================================
# ГЛАВНЫЕ ФУНКЦИИ
# ============================================================================

async def run_tag_tests(
    pool_manager: ConnectionPoolManager,
    logger: AsyncDataLogger,
    item_names: List[str]
):
    """Запускает полный цикл тестов для выбранных тегов"""
    try:
        items = await read_current_values(pool_manager, logger, item_names)
        
        if not items:
            return # Ошибка уже залогирована в read_current_values

        # Запускаем подписку и чтение истории параллельно
        log.info("Запуск подписки и чтения истории параллельно...")

        subscription_task = asyncio.create_task(
            subscribe_to_changes(
                pool_manager, logger, items, duration=SUBSCRIPTION_DURATION_SEC
            )
        )
        
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=HISTORY_READ_HOURS_AGO)
        
        history_task = asyncio.create_task(
            parallel_historical_read(
                pool_manager, logger, items, start_time, end_time
            )
        )
        
        # Ожидание завершения обеих операций
        results = await asyncio.gather(subscription_task, history_task, return_exceptions=True)
        
        for i, res in enumerate(results):
            if isinstance(res, Exception):
                task_name = "Подписка" if i == 0 else "Чтение истории"
                log.error(f"Ошибка в задаче '{task_name}': {res}")

    except RuntimeError as e:
        log.error(f"{e}")
        log.warning("Проверьте правильность имён тегов и попробуйте снова.")
    except Exception as e:
        log.exception(f"Непредвиденная ошибка в run_tag_tests: {e}")
        
async def main():
    """Главная функция"""
    LOGO_ASCII = r"""

                  
              ▄███████
          ▄▄████████▀    
       ▄████████▀      ▄▄████▄▄ 
    ████████▀▀      ▄████████████▄
    █████▀      ▄██████████▀▀█████
    █████       ███████▀▀    █████
    █████                    █████
    █████                    █████
    █████      ▄█████▄       █████
    █████  ▄██████████       █████
    ██████████████▀▀      ▄███████
      ▀████████▀      ▄████████▀▀
                  ▄▄████████▀
                ████████▀
                 ▀▀▀▀
    
"""
    console.print(Panel(
        "Alpha Domain Async Client",  # <--- ИЗМЕНЕНО
        expand=False
    ))
    console.print(LOGO_ASCII)
    
    logger = AsyncDataLogger(LOG_BASE_PATH)
    pool_manager = ConnectionPoolManager()
    
    try:
        log.info("Создание пулов соединений из config.py...")
        await pool_manager.create_pool(PoolZone.RUNTIME, RUNTIME_CONFIG, POOL_CONFIG)
        await pool_manager.create_pool(PoolZone.HISTORIAN, HISTORIAN_CONFIG, POOL_CONFIG)
        
        pool_manager.display_pool_stats()
        
        # Демонстрация изоляции
        if Confirm.ask("\nПровести демонстрацию изоляции пулов?", default=True):
            await demonstrate_isolation(pool_manager)
            pool_manager.display_pool_stats()
        
        # Интерактивный ввод тегов
        if Confirm.ask("\nПерейти к тестированию тегов (чтение/подписка/история)?", default=True):
            item_names = get_user_input_tags()
            if item_names:
                await run_tag_tests(pool_manager, logger, item_names)
            else:
                log.info("Тестирование тегов пропущено.")
        
        pool_manager.display_pool_stats()
        
    except ConnectionError as e:
        log.error(f"Ошибка подключения к Alpha Domain: {e}")
        log.error("Проверьте IP-адреса и порты в config.py, а также доступность серверов.")
    except Exception as e:
        log.error(f"Критическая ошибка в main: {e}")
        log.error(traceback.format_exc())
    
    finally:
        log.info("Закрытие всех соединений...")
        await pool_manager.close_all()
        log.info("Работа клиента завершена.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("\nРабота прервана пользователем.")
