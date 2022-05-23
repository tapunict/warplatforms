FROM python:3.8.11-alpine3.14

WORKDIR /requirements

COPY ./requirements.txt ./ 

RUN apk add --update nodejs npm

RUN pip --no-cache-dir install -r requirements.txt

RUN pip install translators --upgrade

WORKDIR /app

ENV PYTHONUNBUFFERED=0

EXPOSE 5000
 
CMD ["python", "-u", "main.py"]