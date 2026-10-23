FROM openjdk:8u265 as spark_pyutil_python

ENV PYTHON_VERSION=3.7

RUN apt-get -y update && \
    apt-get install --no-install-recommends -y \
    python${PYTHON_VERSION} \
    python3-venv \
    python${PYTHON_VERSION}-venv \
    python3-pip \
    python${PYTHON_VERSION}-dev \
    zip && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

RUN mkdir /pyspark-library
WORKDIR /pyspark-library

RUN python3 -m pip install --upgrade pip wheel setuptools && \
    python3 -m pip cache purge

COPY ./requirements.txt /pyspark-library/
COPY ./scripts/create-venv.sh scripts/
COPY ./scripts/prepare-venv.sh scripts/
RUN bash scripts/create-venv.sh && \
    bash scripts/prepare-venv.sh && \
    pip3 cache purge

COPY ./ /pyspark-library

RUN bash scripts/clean.sh && \
    bash scripts/build.sh
    
