FROM python3_crontab:latest AS base

WORKDIR /usr/src/app/recomendaciones

COPY requirements.txt .

RUN pip install --upgrade pip setuptools wheel
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

WORKDIR /usr/src/app

CMD cron && tail -f /var/log/cron.log