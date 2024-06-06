#!/bin/bash

# Проверяем наличие пакета pipenv
if ! command -v pipenv &>/dev/null; then
    echo "Устанавливаем пакет pipenv"
    pip install pipenv
fi

# Обновляем пакет pipenv
pipenv update

# Переходим в папку проекта
cd /myproject/windyapi/

# Проверяем наличие виртуального окружения
if [ ! -d venv ]; then
    echo "Создаём виртуальное окружение"
    python3 -m venv venv
fi

# Активируем виртуальное окружение
. venv/bin/activate

# Запускаем файл
python run_windy.py

# Обрабатываем ошибки
if [ $? -ne 0 ]; then
    echo "Ошибка при запуске run_windy.py"
    exit 1
fi