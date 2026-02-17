FROM python:3.12-slim
WORKDIR /app
COPY app.py .

# run at build time so docker build fails
RUN python app.py
