FROM python:3.10-alpine 
COPY . /process
WORKDIR /process
RUN pip install -r requirements.txt
CMD [ "python" , "/process/to_postgres.py", "--data", "/process/data" , "--start", "2024-12-15", "--end", "2024-12-19"]
# CMD [ "tail", "-f", "/dev/null" ]