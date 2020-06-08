FROM openjdk:8-jdk-slim

MAINTAINER OrientDB LTD (info@orientdb.com)

RUN apt update \
    && apt install -y curl wget \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir /orientdb
COPY orientdb-community-3.1.1-SNAPSHOT /orientdb
RUN rm -rf /orientdb/databases/*

ENV PATH /orientdb/bin:$PATH

VOLUME ["/orientdb/backup", "/orientdb/databases", "/orientdb/config"]

WORKDIR /orientdb

#OrientDb binary
EXPOSE 2424

#OrientDb http
EXPOSE 2480

# Default command start the server
CMD ["server.sh"]