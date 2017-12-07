FROM ubuntu:16.04
WORKDIR /kqm
COPY ./*.sh ./
RUN chmod +x ./*.sh
