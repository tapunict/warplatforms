FROM python:3.8.11-alpine3.14

WORKDIR /requirements

COPY ./requirements.txt ./ 

RUN pip --no-cache-dir install -r requirements.txt

WORKDIR /app

ENV PYTHONUNBUFFERED=0

EXPOSE 5000

CMD ["python", "-u", "main.py"]