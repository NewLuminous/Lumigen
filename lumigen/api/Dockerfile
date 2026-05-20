ARG BASE_IMAGE=newluminous/spark-k8s:3.3.3
FROM ${BASE_IMAGE}

# Metadata labels
LABEL maintainer="NewLuminous"
LABEL project.name="lumigen"

ARG SPARK_CONF_DIR=""

# Optional: Override the Hadoop user if needed (base image sets it to 'spark' by default)
ARG HADOOP_USER_NAME_ARG=spark
ENV HADOOP_USER_NAME=${HADOOP_USER_NAME_ARG}
ENV PYTHONPATH="/opt/spark/work-dir:${PYTHONPATH}"

# Switch to root (UID 0) to copy files, install packages, and fix permissions
USER 0

RUN python3 -m pip install --no-cache-dir --upgrade pip setuptools wheel

# Paths are now relative to the build context, which must be the parent directory.
# Install base project requirements
COPY requirements.txt /tmp/requirements.txt
RUN python3 -m pip install --no-cache-dir --no-build-isolation -r /tmp/requirements.txt

# Install API-specific requirements
COPY api/requirements.txt /tmp/api_requirements.txt
RUN python3 -m pip install --no-cache-dir --no-build-isolation -r /tmp/api_requirements.txt && \
    rm /tmp/requirements.txt /tmp/api_requirements.txt

# K8s standard working directory
WORKDIR /opt/spark/work-dir
COPY . .

# Use Group 0 to allow K8s anonymous UIDs to read/write the configs
RUN chgrp -R 0 /opt/spark/work-dir && \
    chmod -R g+rwX /opt/spark/work-dir && \
    if [ -n "${SPARK_CONF_DIR}" ] && [ -d "${SPARK_CONF_DIR}" ]; then \
        echo "Spark configuration directory '${SPARK_CONF_DIR}' found. Copying to /opt/spark/conf/"; \
        mkdir -p /opt/spark/conf/ && \
        cp -rT "${SPARK_CONF_DIR}/." /opt/spark/conf/ && \
        chgrp -R 0 /opt/spark/conf && \
        chmod -R g+rwX /opt/spark/conf; \
    else \
        echo "Optional Spark configuration directory not provided or not found. Skipping."; \
    fi

# Switch back to the non-root Spark user defined in the base image (UID 185)
ARG SPARK_UID=185
USER ${SPARK_UID}

# Inherit ENTRYPOINT and CMD from the base image
# ENTRYPOINT ["/usr/bin/tini", "--"]
# CMD ["bash"]