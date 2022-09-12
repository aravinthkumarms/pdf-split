FROM python:3.9.8
COPY requirements.txt requirements.txt
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
WORKDIR /src/
ADD . /src
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "7000"]
