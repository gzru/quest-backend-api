FROM registry.quest.aiarlabs.com/ubuntu:xenial

# Update OS
RUN sed -i 's/# \(.*multiverse$\)/\1/g' /etc/apt/sources.list
RUN apt-get update
RUN apt-get -y upgrade
RUN apt-get install -y python-pip
RUN apt-get install -y lsb-release curl python-dev libssl-dev

COPY requirements.txt /application/

WORKDIR /application

# Install uwsgi Python web server
RUN pip install uwsgi
# Install app requirements
RUN pip install -r requirements.txt

COPY application/ /application/
COPY entrypoint.sh /application/

# Expose port 8001 for uwsgi
EXPOSE 8001

ENTRYPOINT ["./entrypoint.sh"]
CMD ["uwsgi", "--ini", "quest-docker.ini"]

