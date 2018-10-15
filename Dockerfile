FROM python:3.6

RUN apt-get update
RUN apt-get install -y libsnappy-dev
RUN pip install pip==9.0.1
ADD ./requirements.txt .
RUN pip install -r requirements.txt --no-cache-dir
ADD . /app
WORKDIR /app
ENV PYTHONPATH=/app

RUN echo 'Asia/Shanghai' >/etc/timezone & cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime
