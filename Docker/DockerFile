FROM ubuntu:20.04

ENV PYTHON_VERSION=3.12

# Updating the system and install the dependencies
RUN apt-get update && \
    apt-get install -y openjdk-11-jre-headless python${PYTHON_VERSION} python3-pip postgresql postgresql-contrib

# Installing PySpark
RUN pip3 install pyspark

# Configuring PostgreSQL
USER postgres
RUN /etc/init.d/postgresql start &&\
    psql --command "ALTER USER postgres PASSWORD 'mypassword';" &&\
    createdb mydb

# Open port to PostgreSQL
EXPOSE 5432

# Defining the workbook
WORKDIR /app

# Copying pgAdmin configuration files
COPY pgadmin4.conf /etc/pgadmin4/
COPY config_local.py /etc/pgadmin4/

# Installing  pgAdmin
RUN apt-get install -y wget curl && \
    wget --quiet --output-document=/pgadmin4.deb https://ftp.postgresql.org/pub/pgadmin/pgadmin4/v4.29/pip/pgadmin4-4.29-py3-none-any.whl && \
    pip3 install /pgadmin4.deb && \
    apt-get purge -y wget curl && \
    apt-get autoremove -y && \
    apt-get clean && \
    rm /pgadmin4.deb && \
    ln -s /usr/pgadmin4/bin/pgadmin4 /usr/local/bin/pgadmin4

# Initializing pgAdmin
CMD ["pgadmin4"]
