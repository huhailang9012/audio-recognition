FROM python:3.8
RUN apt-get update -y && apt-get upgrade -y
RUN apt-get install \
    gcc nano \
    ffmpeg libasound-dev portaudio19-dev libportaudio2 libportaudiocpp0 \
    postgresql postgresql-contrib -y
RUN pip install numpy scipy matplotlib pydub pyaudio psycopg2 -i https://mirrors.aliyun.com/pypi/simple/
RUN pip install fastapi uvicorn -i https://mirrors.aliyun.com/pypi/simple/
WORKDIR /code
COPY ./ /code/
RUN cd /code

CMD uvicorn controller:app --reload --port 8000 --host 0.0.0.0