FROM ubuntu:16.04
WORKDIR /kqm
COPY ["install.sh", "test.sh", "./"]
RUN chmod +x ./*.sh
