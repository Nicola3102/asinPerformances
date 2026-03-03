# Python 3.11 运行环境
FROM python:3.11-slim

WORKDIR /app

# 先复制依赖文件，利用层缓存：仅 requirements 变化时才重新 pip install
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 9090
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "9090"]
