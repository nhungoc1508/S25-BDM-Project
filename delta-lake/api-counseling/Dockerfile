FROM python:3.9-slim

WORKDIR /app

COPY ./app.py /app/app.py
COPY ./requirements.txt /app/requirements.txt

RUN pip install -r /app/requirements.txt

EXPOSE 3003

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "3003"]