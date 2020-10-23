FROM python:3.8 AS system

RUN apt-get update && apt-get -y install cron


COPY croncfg /etc/cron.d/croncfg
RUN chmod 0644 /etc/cron.d/croncfg
RUN crontab /etc/cron.d/croncfg
RUN touch /var/log/cron.log
