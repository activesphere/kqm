FROM ubuntu:16.04
WORKDIR /kqm
COPY ./test.sh ./
RUN chmod +x ./test.sh
RUN ./test.sh
