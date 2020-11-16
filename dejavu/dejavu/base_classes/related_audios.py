class Related_Audios(object):

    def __init__(self, related_audio_id: str, related_audio_name: str, match_id: str,
                 input_total_hashes: int, fingerprinted_hashes_in_db: int, hashes_matched_in_input: int,
                 input_confidence: float, fingerprinted_confidence: float, offset: int,
                 offset_seconds: int, file_sha1: str):
        self.related_audio_id = related_audio_id
        self.related_audio_name = related_audio_name
        self.match_id = match_id
        self.input_total_hashes = input_total_hashes
        self.fingerprinted_hashes_in_db = fingerprinted_hashes_in_db
        self.hashes_matched_in_input = hashes_matched_in_input
        self.input_confidence = input_confidence
        self.fingerprinted_confidence = fingerprinted_confidence
        self.offset = offset
        self.offset_seconds = offset_seconds
        self.file_sha1 = file_sha1
