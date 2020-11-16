from dejavu.base_classes.related_audios import Related_Audios


class Matched_Information(object):

    def __init__(self, audio_id: str, audio_name: str, total_time: float, fingerprint_time: float, query_time: float,
                 align_time: float, related_audios: list, date_created: str):
        self.audio_id = audio_id
        self.audio_name = audio_name
        self.total_time = total_time
        self.fingerprint_time = fingerprint_time
        self.align_time = align_time
        self.query_time = query_time
        self.related_audios = related_audios
        self.date_created = date_created
