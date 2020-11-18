import abc
import importlib
from typing import Dict, List, Tuple

from dejavu.config.settings import DATABASES


class BaseDatabase(object, metaclass=abc.ABCMeta):
    # Name of your Database subclass, this is used in configuration
    # to refer to your class
    type = None

    def __init__(self):
        super().__init__()

    def before_fork(self) -> None:
        """
        Called before the database instance is given to the new process
        """
        pass

    def after_fork(self) -> None:
        """
        Called after the database instance has been given to the new process

        This will be called in the new process.
        """
        pass

    def setup(self) -> None:
        """
        Called on creation or shortly afterwards.
        """
        pass

    @abc.abstractmethod
    def empty(self) -> None:
        """
        Called when the database should be cleared of all data.
        """
        pass

    @abc.abstractmethod
    def delete_unfingerprinted_audios(self) -> None:
        """
        Called to remove any audio entries that do not have any fingerprints
        associated with them.
        """
        pass

    @abc.abstractmethod
    def get_num_audios(self) -> int:
        """
        Returns the audio's count stored.

        :return: the amount of audios in the database.
        """
        pass

    @abc.abstractmethod
    def count_matched_audios_by_md5(self, md5: str) -> int:
        """
        count matched audio num.
        :param md5.
        :return: num.
        """
        pass

    @abc.abstractmethod
    def get_num_fingerprints(self) -> int:
        """
        Returns the fingerprints' count stored.

        :return: the number of fingerprints in the database.
        """
        pass

    @abc.abstractmethod
    def set_audio_fingerprinted(self, audio_id: int):
        """
        Sets a specific audio as having all fingerprints in the database.

        :param audio_id: audio identifier.
        """
        pass

    @abc.abstractmethod
    def get_audios(self) -> List[Dict[str, str]]:
        """
        Returns all fully fingerprinted audios in the database

        :return: a dictionary with the audios info.
        """
        pass

    @abc.abstractmethod
    def get_matched_info(self, related_key: str) -> List[Dict[str, str]]:
        """
        Returns matched information list

        :return: a dictionary with the information info list.
        """
        pass

    @abc.abstractmethod
    def get_related_audios(self, audio_id: str) -> List[Dict[str, str]]:
        """
        Returns all related audios

        :return: a dictionary with the audios info.
        """
        pass

    @abc.abstractmethod
    def get_audio_by_id(self, audio_id: int) -> Dict[str, str]:
        """
        Brings the audio info from the database.

        :param audio_id: audio identifier.
        :return: a audio by its identifier. Result must be a Dictionary.
        """
        pass

    @abc.abstractmethod
    def insert(self, fingerprint: str, audio_id: int, offset: int):
        """
        Inserts a single fingerprint into the database.

        :param fingerprint: Part of a sha1 hash, in hexadecimal format
        :param audio_id: Song identifier this fingerprint is off
        :param offset: The offset this fingerprint is from.
        """
        pass

    @abc.abstractmethod
    def insert_audios(self, audio_id: str, audio_name: str, file_hash: str, total_hashes: int) -> int:
        """
        Inserts a audio name into the database, returns the new
        identifier of the audio.
        :param audio_id: The id of the audio.
        :param audio_name: The name of the audio.
        :param file_hash: Hash from the fingerprinted file.
        :param total_hashes: amount of hashes to be inserted on fingerprint table.
        :return: the inserted id.
        """
        pass

    @abc.abstractmethod
    def insert_matched_information(self, id: str, audio_id: str, audio_name: str, total_time: float,
                                   fingerprint_time: float, query_time: float, align_time: float, date_created: str, related_key: str):
        pass

    @abc.abstractmethod
    def insert_related_audios(self, id: str, audio_id: str, related_audio_id: str, related_audio_name: str, match_id: str,
                              input_total_hashes: int, fingerprinted_hashes_in_db: int, hashes_matched_in_input: int,
                              input_confidence: float, fingerprinted_confidence: float, offset: int,
                              offset_seconds: int, file_sha1: str):
        pass

    @abc.abstractmethod
    def query(self, fingerprint: str = None) -> List[Tuple]:
        """
        Returns all matching fingerprint entries associated with
        the given hash as parameter, if None is passed it returns all entries.

        :param fingerprint: part of a sha1 hash, in hexadecimal format
        :return: a list of fingerprint records stored in the db.
        """
        pass

    @abc.abstractmethod
    def get_iterable_kv_pairs(self) -> List[Tuple]:
        """
        Returns all fingerprints in the database.

        :return: a list containing all fingerprints stored in the db.
        """
        pass

    @abc.abstractmethod
    def insert_hashes(self, audio_id: int, hashes: List[Tuple[str, int]], batch_size: int = 1000) -> None:
        """
        Insert a multitude of fingerprints.

        :param audio_id: Song identifier the fingerprints belong to
        :param hashes: A sequence of tuples in the format (hash, offset)
            - hash: Part of a sha1 hash, in hexadecimal format
            - offset: Offset this hash was created from/at.
        :param batch_size: insert batches.
        """

    @abc.abstractmethod
    def return_matches(self, hashes: List[Tuple[str, int]], batch_size: int = 1000) \
            -> Tuple[List[Tuple[int, int]], Dict[int, int]]:
        """
        Searches the database for pairs of (hash, offset) values.

        :param hashes: A sequence of tuples in the format (hash, offset)
            - hash: Part of a sha1 hash, in hexadecimal format
            - offset: Offset this hash was created from/at.
        :param batch_size: number of query's batches.
        :return: a list of (sid, offset_difference) tuples and a
        dictionary with the amount of hashes matched (not considering
        duplicated hashes) in each audio.
            - audio id: Song identifier
            - offset_difference: (database_offset - sampled_offset)
        """
        pass

    @abc.abstractmethod
    def delete_audios_by_id(self, audio_ids: List[int], batch_size: int = 1000) -> None:
        """
        Given a list of audio ids it deletes all audios specified and their corresponding fingerprints.

        :param audio_ids: audio ids to be deleted from the database.
        :param batch_size: number of query's batches.
        """
        pass


def get_database(database_type: str = "mysql") -> BaseDatabase:
    """
    Given a database type it returns a database instance for that type.

    :param database_type: type of the database.
    :return: an instance of BaseDatabase depending on given database_type.
    """
    try:
        path, db_class_name = DATABASES[database_type]
        db_module = importlib.import_module(path)
        db_class = getattr(db_module, db_class_name)
        return db_class
    except (ImportError, KeyError):
        raise TypeError("Unsupported database type supplied.")
