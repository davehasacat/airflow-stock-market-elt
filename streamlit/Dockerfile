    FROM python:3.9-slim-buster

    WORKDIR /streamlit

    COPY requirements.txt .
    RUN pip install -r requirements.txt

    COPY . .

    EXPOSE 8501

    CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
