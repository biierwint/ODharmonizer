FROM python:3.12-slim

ARG HOST_UID
ARG HOST_GID
ARG USERNAME

RUN apt-get update
RUN apt-get install -y python3-dev
RUN apt-get install -y gcc
RUN apt-get install -y zlib1g-dev
RUN apt-get install -y tabix
RUN apt-get install -y libpq-dev
RUN apt-get install -y libpq5
RUN apt-get install -y python3-psycopg2

RUN groupadd -g ${HOST_GID} ${USERNAME}
RUN useradd -m -u ${HOST_UID} -g ${HOST_GID} -s /bin/bash $USERNAME

RUN mkdir /odconverter
RUN mkdir -p /odconverter/tmp /odconverter/output /odconverter/input
RUN chown -R ${USERNAME}:${USERNAME} /odconverter /odconverter/tmp /odconverter/output /odconverter/input

# Set the working directory
WORKDIR /odconverter

# Copy the requirements file first (better caching)
COPY requirements.txt .

# Upgrade pip and install dependencies
RUN pip install --upgrade pip

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY --chown=${USERNAME}:${USERNAME} *.py .
COPY --chown=${USERNAME}:${USERNAME} modules modules
COPY --chown=${USERNAME}:${USERNAME} utils utils
COPY --chown=${USERNAME}:${USERNAME} converter_modules converter_modules

# Set environment variables to optimize Python
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV TMPDIR=/odconverter/tmp

# Switch to non-root user
USER ${USERNAME}

# Expose the application port
EXPOSE 8501

# Start the application using streamlit
CMD ["streamlit", "run", "app-converter.py", "--server.port=8501", "--server.address=0.0.0.0"]

