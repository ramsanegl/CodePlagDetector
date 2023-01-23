FROM python:3
WORKDIR /app

COPY requirements.txt /app
RUN pip --no-cache-dir install -r requirements.txt

COPY . /app
EXPOSE 5000
CMD ["python3", "app.py"]