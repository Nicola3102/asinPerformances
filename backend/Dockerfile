FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir uvicorn

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 9090
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "9090"]
