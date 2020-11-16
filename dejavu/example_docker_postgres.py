import json
from dejavu import Dejavu
from dejavu.logic.recognizer.file_recognizer import FileRecognizer
from dejavu.logic.recognizer.microphone_recognizer import MicrophoneRecognizer

# load config from a JSON file (or anything outputting a python dictionary)
config = {
    "database": {
        "host": "db",
        "user": "postgres",
        "password": "password",
        "database": "dejavu"
    },
    "database_type": "postgres"
}

if __name__ == '__main__':

    # djv = Dejavu(config)
    # infos = djv.find_matched_info()
    # # result = json.dumps(infos, ensure_ascii=False)
    # result = json.dumps(infos, default=lambda obj: obj.__dict__, sort_keys=False, indent=4)
    # print(result)
    # create a Dejavu instance
    djv = Dejavu(config)

    # Fingerprint all the mp3's in the directory we give it
    djv.fingerprint_directory("mp3", [".mp3"])

    # Recognize audio from a file
    # results = djv.recognize(FileRecognizer, "test/sean_secs.wav")
    # print(f"From file we recognized: {results}\n")

    # Or use a recognizer without the shortcut, in anyway you would like
    recognizer = FileRecognizer(djv)
    results = recognizer.recognize_file("test/woodward_43s.wav")
    print(f"No shortcut, we recognized: {results}\n")

