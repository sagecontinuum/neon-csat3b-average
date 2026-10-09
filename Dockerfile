FROM waggle/sage-thor-base:0.1.0

WORKDIR /app

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY main.py .
COPY averaging.py .

ENTRYPOINT ["python3", "main.py"]
