#Deriving the latest base image
FROM python:3.10

EXPOSE 8501
WORKDIR /

COPY  src/streamlit/ /streamlit
COPY  requirements.txt /requirements.txt

RUN apt-get -y update
RUN apt-get install -y iputils-ping
RUN apt-get install -y vim
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

CMD  streamlit run  /streamlit/app-home.py
