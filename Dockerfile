FROM python:3.9

# Set up working directory
WORKDIR /app

# copy repo into image
COPY . /app

RUN pip install --upgrade pip
RUN pip install -r requirements.txt
RUN pip install -e .

CMD [ "python", "./create_schema_test.py -pd -m company"]