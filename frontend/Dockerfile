FROM python:3.13-alpine
COPY . /frontend
WORKDIR /frontend
EXPOSE 8050
RUN apk add gcc python3-dev musl-dev linux-headers g++ util-linux
RUN pip install -r requirements.txt
CMD [ "python" , "app.py" ]
# CMD [ "tail", "-f", "/dev/null" ]