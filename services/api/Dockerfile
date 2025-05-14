FROM python:3.11
WORKDIR /app 
COPY ./requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /app/requirements.txt 
COPY ./app ./app
ENV PYTHONPATH=/app
ENTRYPOINT ["python", "app/server.py"]
