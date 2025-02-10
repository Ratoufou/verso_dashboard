FROM python:3.10-alpine 
COPY . /process
WORKDIR /process
RUN pip install -r requirements.txt
CMD [ "python" , "/process/to_postgres.py", "--data", "/process/data" , "--start", "2015-01-01", "--end", "2025-02-03"]
# CMD [ "tail", "-f", "/dev/null" ]