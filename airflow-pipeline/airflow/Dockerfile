FROM apache/airflow:2.2.2-python3.8
COPY requirements.txt /tmp
COPY certs/fortinet-root-ca.crt /usr/local/share/ca-certificates/fortinet-root-ca.crt
USER root
RUN sed -i 's|http://deb.debian.org/debian|http://archive.debian.org/debian|g' /etc/apt/sources.list \
    && sed -i 's|http://security.debian.org/debian-security|http://archive.debian.org/debian-security|g' /etc/apt/sources.list \
    && printf 'Acquire::Check-Valid-Until "false";\nAcquire::AllowInsecureRepositories "true";\n' > /etc/apt/apt.conf.d/99archive \
    && apt-get update \
    && apt-get install --no-install-recommends -y curl gnupg2 ca-certificates apt-transport-https unixodbc unixodbc-dev \
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install --no-install-recommends -y msodbcsql17 mssql-tools openjdk-11-jre-headless \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="$PATH:/opt/mssql-tools/bin"
RUN if [ -f "${JAVA_HOME}/conf/security/java.security" ]; then \
        sed -i 's/, TLSv1, TLSv1.1//g' "${JAVA_HOME}/conf/security/java.security"; \
    fi \
    && if [ -f "${JAVA_HOME}/jre/lib/security/java.security" ]; then \
        sed -i 's/, TLSv1, TLSv1.1//g' "${JAVA_HOME}/jre/lib/security/java.security"; \
    fi \
    && update-ca-certificates \
    && keytool -importcert -noprompt -storepass changeit -cacerts -alias fortinet-corporate-root -file /usr/local/share/ca-certificates/fortinet-root-ca.crt
ENV JAVA_TOOL_OPTIONS="-Djdk.tls.client.protocols=TLSv1.2"
ENV REQUESTS_CA_BUNDLE="/etc/ssl/certs/ca-certificates.crt"
ENV SSL_CERT_FILE="/etc/ssl/certs/ca-certificates.crt"
ENV CURL_CA_BUNDLE="/etc/ssl/certs/ca-certificates.crt"
ENV AZURE_CA_BUNDLE="/etc/ssl/certs/ca-certificates.crt"
RUN mkdir -p /usr/local/airflow
RUN ln -s  /opt/airflow/dags /usr/local/airflow/dags 
RUN chown -R airflow /usr/local/airflow 
USER airflow
RUN python3 -m pip install --no-cache-dir -r /tmp/requirements.txt
