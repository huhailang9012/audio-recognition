import logging

from fastapi import FastAPI
from dejavu import Dejavu
from dejavu.logic.recognizer.file_recognizer import FileRecognizer
import json


# 定义日志属性，及输出
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename='myapp.log',
                    filemode='w')
# define a Handler which writes INFO messages or higher to the sys.stderr
console = logging.StreamHandler()
console.setLevel(logging.INFO)
# set a format which is simpler for console use
formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s')
# tell the handler to use this format
console.setFormatter(formatter)
# add the handler to the root logger
logging.getLogger('').addHandler(console)
logging.info("save date done")

app = FastAPI()
config = {
    "database": {
        "host": "postgres",
        "user": "postgres",
        "password": "123456",
        "database": "audio_recognition"
    },
    "database_type": "postgres"
}
confidence = 0.05
djv = Dejavu(config)

@app.get("/matched/information/index")
def index(related_key: str):
    logging.info('/matched/information/index' + ',params = ' + related_key)
    infos = djv.find_matched_info(related_key, confidence)
    result = json.dumps(infos, default=lambda obj: obj.__dict__, sort_keys=False, indent=4)
    return {"success": True, "code": 0, "msg": "ok", "data": result}


@app.get("/target/audio/recognize")
def recognize_target_audio(audio_id: str, related_key: str, local_audio_path: str):

    FileRecognizer(djv).recognize_file(local_audio_path, related_key, audio_id)
    return {"success": True, "code": 0, "msg": "ok"}


@app.post("/source/audio/import")
def import_source_audio(source_path: str, format: str):

    djv.fingerprint_directory(source_path, ["." + format])
    return {"success": True, "code": 0, "msg": "ok"}


@app.get("/source/audio/query")
def query_source_audio(name: str):
    infos = djv.find_source_audio(name)
    result = json.dumps(infos, default=lambda obj: obj.__dict__, sort_keys=False, indent=4, ensure_ascii=False)
    return {"success": True, "code": 0, "msg": "ok", "data": result}