import abc
import datetime
import json
from typing import Any, Dict, Optional

from src.log import log


class AbstractStorage(abc.ABC):
    """Абстрактный класс для хранения состояния.

    Этот класс определяет методы для сохранения и извлечения состояния.
    Реализация может использовать различные методы хранения,
    такие как базы данных или файловые системы.
    """

    @abc.abstractmethod
    def save(self, state: Dict[str, Any]) -> None:
        """Метод для сохранения состояния в хранилище."""

    @abc.abstractmethod
    def load(self) -> Dict[str, Any]:
        """Метод для загрузки состояния из хранилища."""


class LocalJsonStorage(AbstractStorage):
    """Хранилище, использующее локальный JSON-файл.

    Данные сохраняются в формате JSON.
    """

    def __init__(self, file_path: Optional[str] = None) -> None:
        self.file_path = file_path

    def save(self, state: Dict[str, Any]) -> None:
        """Сохранить текущее состояние в файл."""
        with open(self.file_path, 'w') as file:
            json.dump(state, file)

    def load(self) -> Dict[str, Any]:
        """Загрузить состояние из файла."""
        try:
            with open(self.file_path) as file:
                try:
                    return json.load(file)
                except json.JSONDecodeError as e:
                    log.error('Ошибка при загрузке JSON: %s', e, exc_info=True)
                    return {}
        except FileNotFoundError:
            log.error('Не удалось найти файл: %s', self.file_path)
            return {}


class StateManager:
    """Класс для управления состоянием приложения."""

    def __init__(self, storage: AbstractStorage) -> None:
        self.storage = storage

    def set(self, key: str, value: Any) -> None:
        """Сохранить значение по заданному ключу."""
        current_state = self.storage.load()
        current_state[key] = value
        self.storage.save(current_state)

    def get(self, key: str) -> Any:
        """Получить значение по заданному ключу."""
        current_state = self.storage.load()
        return current_state.get(key, datetime.datetime.min)
