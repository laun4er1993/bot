FROM python:3.11-slim

WORKDIR /app

# Устанавливаем системные зависимости для pyproj
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    proj-bin \
    libproj-dev \
    && rm -rf /var/lib/apt/lists/*

# Копируем requirements.txt и устанавливаем Python зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем остальной код
COPY . .

# Запускаем бота
CMD ["python", "bot.py"]