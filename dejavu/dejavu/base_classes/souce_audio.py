class Source_Audio(object):

    def __init__(self, audio_id: str, audio_name: str, fingerprinted: int,
                 file_sha1: str, total_hashes: int):
        self.audio_id = audio_id
        self.audio_name = audio_name
        self.fingerprinted = fingerprinted
        self.file_sha1 = file_sha1
        self.total_hashes = total_hashes