FROM newluminous/pyspark

ARG PYTHON_VERSION=3.9
ARG SPARK_CONF_DIR=""
ARG HADOOP_USER_NAME_ARG=spark

# Note: The actual Linux user running commands remains 'spark' (from the base image)
ENV HADOOP_USER_NAME=${HADOOP_USER_NAME_ARG}
ENV PYTHONPATH="/home/spark:${PYTHONPATH}"

# Metadata labels
LABEL maintainer="NewLuminous"
LABEL project.name="lumigen"

# Switch to root user for installations
USER root

# Paths are now relative to the build context, which must be the parent directory.
# Install base project requirements
COPY requirements.txt /tmp/requirements.txt
RUN python${PYTHON_VERSION} -m pip install --no-cache-dir -r /tmp/requirements.txt

# Install API-specific requirements
COPY api/requirements.txt /tmp/api_requirements.txt
RUN python${PYTHON_VERSION} -m pip install --no-cache-dir -r /tmp/api_requirements.txt && \
    rm /tmp/requirements.txt /tmp/api_requirements.txt

WORKDIR /home/spark
COPY . .

# Ensure correct ownership for the 'spark' user (UID/GID 1000 from base image)
RUN \
    chown -R spark:spark /home/spark && \
    if [ -d "${SPARK_CONF_DIR}" ]; then \
        echo "Spark configuration directory '${SPARK_CONF_DIR}' found. Copying to /opt/spark/conf/"; \
        mkdir -p /opt/spark/conf/ && \
        cp -rT "${SPARK_CONF_DIR}/." /opt/spark/conf/ && \
        chown -R spark:spark /opt/spark/conf; \
    else \
        echo "Optional Spark configuration directory '${SPARK_CONF_DIR}' not found. Skipping."; \
    fi

# Switch back to the non-root user defined in the base image
USER spark

# Inherit ENTRYPOINT and CMD from the base image
# ENTRYPOINT ["/usr/bin/tini", "--"]
# CMD ["bash"]