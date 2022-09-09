FROM python:3.7-alpine3.14
COPY . .
EXPOSE 5555
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
#CMD [ "python", "app.py" ]
ENTRYPOINT ["python", "-u", "app.py"]

