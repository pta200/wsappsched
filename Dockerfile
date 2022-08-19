FROM python:3.8

WORKDIR /opt

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY . .

ARG BUILD_DATE
ARG BUILD_VERSION

LABEL project="wsappsched"
LABEL name="wsappsched"
LABEL build_date="$BUILD_DATE"
LABEL build_version="$BUILD_VERSION"
ENTRYPOINT [ "./run_gunicorn.sh"]