FROM pytorch/pytorch:1.13.1-cuda11.6-cudnn8-runtime

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE 1
# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED 1

RUN apt update && apt upgrade -y 
RUN apt-get update && apt-get upgrade -y 

# Install pip requirements
ADD build/requirements.txt .
RUN pip uninstall -y -r requirements.txt --no-cache-dir
RUN pip install -r requirements.txt --no-cache-dir
RUN python -m spacy download en_core_web_lg


WORKDIR /abstractive-summary/src
COPY src ./
COPY test/ ../test/

RUN python -m grpc_tools.protoc -I ./proto --python_out=. --pyi_out=. --grpc_python_out=. ./proto/abstractive_summarize.proto
RUN cp abstractive_summarize_pb2* ../test/