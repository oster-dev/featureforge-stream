FROM flink:1.20.3-scala_2.12-java17

USER root

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        openjdk-17-jdk-headless \
        build-essential \
        curl \
        ca-certificates \
        xz-utils \
        libssl-dev \
        zlib1g-dev \
        libbz2-dev \
        libreadline-dev \
        libsqlite3-dev \
        libffi-dev \
        liblzma-dev \
        libgdbm-dev \
        uuid-dev \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s \
        "$(dirname "$(dirname "$(readlink -f /usr/bin/javac)")")" \
        /opt/pyflink-jdk

ENV JAVA_HOME="/opt/pyflink-jdk"
ENV PATH="/opt/pyflink/bin:${JAVA_HOME}/bin:${PATH}"

RUN test -f "${JAVA_HOME}/include/jni.h" \
    && java -version \
    && javac -version

RUN curl --fail --show-error --location --retry 3 \
        "https://www.python.org/ftp/python/3.11.18/Python-3.11.18.tar.xz" \
        --output /tmp/python.tar.xz \
    && echo \
        "9120e0a653868f6e3698d36cdd99e5a58903ec9469e448f21d98f7b16d8fa290  /tmp/python.tar.xz" \
        | sha256sum --check - \
    && mkdir -p /tmp/python-src \
    && tar -xJf /tmp/python.tar.xz \
        --directory /tmp/python-src \
        --strip-components=1 \
    && cd /tmp/python-src \
    && ./configure \
        --prefix=/opt/python311 \
        --enable-shared \
        --with-ensurepip=install \
        LDFLAGS="-Wl,-rpath=/opt/python311/lib" \
    && make -j2 \
    && make install \
    && /opt/python311/bin/python3.11 -c \
        "import ssl, sqlite3, bz2, lzma, ctypes; print('Python standard-library checks: OK')" \
    && rm -rf /tmp/python-src /tmp/python.tar.xz

RUN /opt/python311/bin/python3.11 -m venv /opt/pyflink \
    && /opt/pyflink/bin/python -m pip install --no-cache-dir \
        "apache-flink==1.20.3" \
    && /opt/pyflink/bin/python -m pip check

RUN curl --fail --show-error --location --retry 3 \
    "https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar" \
    --output /opt/flink/lib/flink-sql-connector-kafka-3.3.0-1.20.jar

ENV PYFLINK_CLIENT_EXECUTABLE="/opt/pyflink/bin/python"
ENV PYFLINK_PYTHON="/opt/pyflink/bin/python"

WORKDIR /opt/featureforge-stream

COPY pyproject.toml ./
COPY src/ ./src/

RUN /opt/pyflink/bin/python -m pip install --no-cache-dir . \
    && /opt/pyflink/bin/python -m pip check \
    && /opt/pyflink/bin/python -c \
        "from featureforge_stream.contracts.events import validate_event_json; print('Project import: OK')"

WORKDIR /opt/flink

USER flink