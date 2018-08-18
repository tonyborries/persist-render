FROM python:2-wheezy
ENV PYTHONUNBUFFERED 1
RUN apt-get update
# Debug Tools
RUN apt-get install -y vim-tiny telnet net-tools less
# Install Persist Prereqs
#RUN apt-get install -y python-dev build-essential
RUN apt-get install -y ffmpeg
# Cleanup
#RUN apt-get clean

RUN mkdir /opt/persist
WORKDIR /opt/persist
COPY requirements.txt /opt/persist/
RUN pip install pip --upgrade
RUN pip install -r requirements.txt
COPY main.py /opt/persist/
COPY persist.py /opt/persist/
CMD celery -A persist worker -l info --concurrency 6
