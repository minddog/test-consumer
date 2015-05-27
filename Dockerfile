FROM ubuntu:trusty
RUN apt-get update
RUN apt-get install -y python python-dev python-distribute python-pip python-virtualenv

# Define working directory.
WORKDIR /data

COPY . /data
RUN cd /data 
RUN virtualenv /data/.venv
RUN /data/.venv/bin/pip install -r requirements.txt
EXPOSE  5000
CMD ["/data/.venv/bin/python consumer.py"]
