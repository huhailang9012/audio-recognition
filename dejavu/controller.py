from fastapi import FastAPI
from dejavu import Dejavu
from dejavu.logic.recognizer.file_recognizer import FileRecognizer
import json

app = FastAPI()
config = {
    "database": {
        "host": "db",
        "user": "postgres",
        "password": "123456",
        "database": "audio_recognition"
    },
    "database_type": "postgres"
}
djv = Dejavu(config)


@app.get("/matched/information/index")
def index():

    infos = djv.find_matched_info()
    result = json.dumps(infos, ensure_ascii=False)
    return {"success": True, "code": 0, "msg": "ok", "data": result}


@app.post("/target/audio/recognize")
def recognize_target_audio(audio_id: str, audio_path: str):

    FileRecognizer(djv).recognize_file(audio_path, audio_id)
    return {"success": True, "code": 0, "msg": "ok"}


@app.post("/source/audio/import")
def import_source_audio(source_path: str, format: str):

    # djv.fingerprint_directory(source_path, ["." + format])
    return {"success": True, "code": 0, "msg": "ok"}