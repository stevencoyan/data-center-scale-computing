FROM python:3.9

WORKDIR /app

COPY pipeline.py pipeline_c.py

RUN pip install pandas

ENTRYPOINT ["bash"]