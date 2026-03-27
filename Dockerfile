FROM python:3.11-slim

WORKDIR /app

# Устанавливаем системные зависимости для pyproj и shapely
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    proj-bin \
    libproj-dev \
    libgeos-dev \
    && rm -rf /var/lib/apt/lists/*

# Копируем requirements.txt
COPY requirements.txt .

# Устанавливаем Python зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Копируем весь код
COPY . .

# Запускаем бота
CMD ["python", "bot.py"]