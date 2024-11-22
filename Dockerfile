FROM python:3.9

# Install cron and other dependencies
RUN apt-get update && apt-get install -y cron sudo && apt-get clean
RUN pip install pandas requests regex
# Set the working directory
WORKDIR /app

# Copy the Python scripts
COPY extract-json-files.py .
COPY FDA.py .
COPY etl_tests.py .

# Copy the data directory
COPY patients_fhir_100 /app/patients_fhir_100

# Add a crontab file
COPY crontab /app/etl-cron

# Switch to root user to apply cron jobs
USER root

RUN chmod -R 777 /var/log

# Create log files and set permissions during container build
RUN touch /var/log/extract-json.log /var/log/fda.log /var/log/etl_tests.log && \
    chmod 777 /var/log/extract-json.log /var/log/fda.log /var/log/etl_tests.log

# Move the crontab file to the correct directory and set permissions
RUN mv /app/etl-cron /etc/cron.d/etl-cron && chmod 0644 /etc/cron.d/etl-cron

# Apply the cron job
RUN crontab /etc/cron.d/etl-cron

# Create a log file for cron logs
RUN touch /var/log/cron.log

# Start the cron service and the container
CMD ["sh", "-c", "cron && tail -f /var/log/cron.log"]




# RUN apt-get update && apt-get install -y cron && apt-get clean
# RUN pip install pandas requests regex


# CMD ["bash", "-c", "python extract-json-files.py && python FDA.py"]
