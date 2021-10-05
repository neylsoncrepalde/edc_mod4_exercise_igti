FROM python:3.9-alpine

RUN pip3 install requests boto3

ARG aws_access_key_id
ARG aws_secret_access_key

ENV AWS_ACCESS_KEY_ID $aws_access_key_id
ENV AWS_SECRET_ACCESS_KEY $aws_secret_access_key
ENV AWS_DEFAULT_REGION us-east-1

COPY ./run.py /run.py

CMD python3 /run.py