FROM flink:1.20.3-scala_2.12-java17

USER root

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        openjdk-17-jdk-headless \
        python3 \
        python3-dev \
        python3-venv \
        build-essential \
        curl \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s \
        "$(dirname "$(dirname "$(readlink -f /usr/bin/javac)")")" \
        /opt/pyflink-jdk

ENV JAVA_HOME="/opt/pyflink-jdk"
ENV PATH="/opt/pyflink/bin:${JAVA_HOME}/bin:${PATH}"

RUN test -f "${JAVA_HOME}/include/jni.h" \
    && java -version \
    && javac -version

RUN python3 -m venv /opt/pyflink \
    && /opt/pyflink/bin/python -m pip install --no-cache-dir \
        "apache-flink==1.20.3" \
    && /opt/pyflink/bin/python -m pip check

RUN curl --fail --show-error --location --retry 3 \
    "https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar" \
    --output /opt/flink/lib/flink-sql-connector-kafka-3.3.0-1.20.jar

ENV PYFLINK_CLIENT_EXECUTABLE="/opt/pyflink/bin/python"
ENV PYFLINK_PYTHON="/opt/pyflink/bin/python"

USER flink