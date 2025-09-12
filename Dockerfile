FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    ADMIN_HOST=0.0.0.0 \
    ADMIN_PORT=8000

WORKDIR /app

# Install Python deps first for better layer caching
COPY requirements.txt ./
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy application code
COPY . .

# Create runtime folders (idempotent; also created by app on run)
RUN mkdir -p data media/photos reports

EXPOSE 8000

# By default run the admin UI (compose overrides for the bot)
CMD ["python", "-m", "admin_ui"]

