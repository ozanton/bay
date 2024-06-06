#!/bin/bash

# Проверяем наличие пакета pipenv
if ! command -v pipenv &>/dev/null; then
    echo "Устанавливаем пакет pipenv"
    pip install pipenv
fi

# Обновляем пакет pipenv
pipenv update

# Переходим в папку проекта
cd /myproject/weatherapi/

# Проверяем наличие виртуального окружения
if [ ! -d venv ]; then
    echo "Создаём виртуальное окружение"
    python3 -m venv venv
fi

# Активируем виртуальное окружение
source venv/bin/activate

# Запускаем файл
python run_current.py

# Обрабатываем ошибки
if [ $? -ne 0 ]; then
    echo "Ошибка при запуске run_current.py"
    exit 1
fi