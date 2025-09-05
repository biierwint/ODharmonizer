# Stage 1: Base build stage
FROM python:3.12-slim AS builder

ARG HOST_UID
ARG HOST_GID
ARG USERNAME

RUN echo "UID=$HOST_UID GID=$HOST_GID USER=$USERNAME"

RUN apt-get update
RUN apt-get install -y python3-dev
RUN apt-get install -y gcc
RUN apt-get install -y zlib1g-dev
RUN apt-get install -y tabix
RUN apt-get install -y libpq-dev

RUN mkdir /odannotator
WORKDIR /odannotator

# Set environment variables to optimize Python
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Upgrade pip and install dependencies
RUN pip install --upgrade pip

# Copy the requirements file first (better caching)
COPY requirements.txt /odannotator/

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Stage 2: Production stage
FROM python:3.12-slim

ARG HOST_UID
ARG HOST_GID
ARG USERNAME

RUN apt-get update
RUN apt-get install -y libpq5
RUN apt-get install -y python3-psycopg2

RUN groupadd -g ${HOST_GID} ${USERNAME}
RUN useradd -m -u ${HOST_UID} -g ${HOST_GID} -s /bin/bash $USERNAME

RUN mkdir /odannotator && \
    chown -R ${USERNAME} /odannotator

# Copy the Python dependencies from the builder stage
COPY --from=builder /usr/local/lib/python3.12/site-packages/ /usr/local/lib/python3.12/site-packages/
COPY --from=builder /usr/local/bin/ /usr/local/bin/

# Set the working directory
WORKDIR /odannotator

# Copy application code
COPY ga4gh/omop.py /usr/local/lib/python3.12/site-packages/ga4gh/vrs/extras/annotator/omop.py
COPY ga4gh/cli.py /usr/local/lib/python3.12/site-packages/ga4gh/vrs/extras/annotator/cli.py
COPY annotator_modules/ annotator_modules/
COPY exp-annotator.py exp-annotator.py
COPY get-stats-expression.py get-stats-expression.py
COPY get-stats-genomic.py get-stats-genomic.py

# Set environment variables to optimize Python
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Switch to non-root user
USER ${USERNAME}

