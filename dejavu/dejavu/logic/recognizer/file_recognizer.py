from time import time
from typing import Dict
import os
import uuid
import hashlib
from datetime import datetime
import dejavu.logic.decoder as decoder
from dejavu.base_classes.base_recognizer import BaseRecognizer
from dejavu.config.settings import (ALIGN_TIME, FINGERPRINT_TIME, QUERY_TIME,
                                    RESULTS, TOTAL_TIME,
                                    FIELD_FILE_SHA1,
                                    FINGERPRINTED_CONFIDENCE,
                                    FINGERPRINTED_HASHES, HASHES_MATCHED,
                                    INPUT_CONFIDENCE, INPUT_HASHES, OFFSET,
                                    OFFSET_SECS, AUDIO_ID, AUDIO_NAME)


class FileRecognizer(BaseRecognizer):
    def __init__(self, dejavu):
        super().__init__(dejavu)

    def recognize_file(self, local_audio_path: str, related_key: str, audio_id: str) -> Dict[str, any]:
        channels, self.Fs, sha1 = decoder.read(local_audio_path, self.dejavu.limit)
        with open(local_audio_path, 'rb') as fp:
            data = fp.read()
        file_md5 = hashlib.md5(data).hexdigest()
        c = self.dejavu.db.count_matched_audios_by_md5(file_md5)
        if c > 0:
            return Dict["None", "None"]
        # insert a matched audios into database
        name = os.path.basename(local_audio_path)
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        match_id = uuid.uuid1().hex
        t = time()
        matches, fingerprint_time, query_time, align_time = self._recognize(*channels)
        t = time() - t
        # insert a matched information into database
        self.dejavu.db.insert_matched_information(match_id, audio_id, name, file_md5, t, fingerprint_time, query_time, align_time, now, related_key)

        for match in matches:
            relate_audio_id = match[AUDIO_ID]
            relate_audio_name = match[AUDIO_NAME]
            input_hashes = match[INPUT_HASHES]
            fingerprint_hashes = match[FINGERPRINTED_HASHES]
            hashes_matched = match[HASHES_MATCHED]
            input_confidence = match[INPUT_CONFIDENCE]
            fingerprinted_confidence = match[FINGERPRINTED_CONFIDENCE]
            offset = match[OFFSET].item()
            offset_seconds = match[OFFSET_SECS]
            file_sha1 = match[FIELD_FILE_SHA1]
            related_id = uuid.uuid1().hex
            # insert a related audios into database
            self.dejavu.db.insert_related_audios(related_id,audio_id,relate_audio_id,relate_audio_name,match_id, input_hashes, fingerprint_hashes,
                                                 hashes_matched,input_confidence,fingerprinted_confidence,offset, offset_seconds, file_sha1)
        results = {
            TOTAL_TIME: t,
            FINGERPRINT_TIME: fingerprint_time,
            QUERY_TIME: query_time,
            ALIGN_TIME: align_time,
            RESULTS: matches
        }

        return results

    def recognize(self, filename: str) -> Dict[str, any]:
        return self.recognize_file(filename)
