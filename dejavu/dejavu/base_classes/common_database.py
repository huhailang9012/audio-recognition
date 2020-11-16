import abc
from typing import Dict, List, Tuple

from dejavu.base_classes.base_database import BaseDatabase


class CommonDatabase(BaseDatabase, metaclass=abc.ABCMeta):
    # Since several methods across different databases are actually just the same
    # I've built this class with the idea to reuse that logic instead of copy pasting
    # over and over the same code.

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
        with self.cursor() as cur:
            cur.execute(self.CREATE_AUDIOS_TABLE)
            cur.execute(self.CREATE_FINGERPRINTS_TABLE)
            cur.execute(self.DELETE_UNFINGERPRINTED)
            cur.execute(self.CREATE_MATCHED_AUDIO_TABLE)
            cur.execute(self.CREATE_MATCHED_INFORMATION_TABLE)
            cur.execute(self.CREATE_RELATED_AUDIOS_TABLE)

    def empty(self) -> None:
        """
        Called when the database should be cleared of all data.
        """
        with self.cursor() as cur:

            cur.execute(self.DROP_RELATED_AUDIOS)
            cur.execute(self.DROP_MATCHED_INFORMATION)
            cur.execute(self.DROP_MATCHED_AUDIOS)
            cur.execute(self.DROP_FINGERPRINTS)
            cur.execute(self.DROP_AUDIOS)
        self.setup()

    def delete_unfingerprinted_audios(self) -> None:
        """
        Called to remove any audio entries that do not have any fingerprints
        associated with them.
        """
        with self.cursor() as cur:
            cur.execute(self.DELETE_UNFINGERPRINTED)

    def get_num_audios(self) -> int:
        """
        Returns the audio's count stored.

        :return: the amount of audios in the database.
        """
        with self.cursor(buffered=True) as cur:
            cur.execute(self.SELECT_UNIQUE_AUDIO_IDS)
            count = cur.fetchone()[0] if cur.rowcount != 0 else 0

        return count

    def get_num_fingerprints(self) -> int:
        """
        Returns the fingerprints' count stored.

        :return: the number of fingerprints in the database.
        """
        with self.cursor(buffered=True) as cur:
            cur.execute(self.SELECT_NUM_FINGERPRINTS)
            count = cur.fetchone()[0] if cur.rowcount != 0 else 0

        return count

    def set_audio_fingerprinted(self, audio_id):
        """
        Sets a specific audio as having all fingerprints in the database.

        :param audio_id: audio identifier.
        """
        with self.cursor() as cur:
            cur.execute(self.UPDATE_AUDIO_FINGERPRINTED, (audio_id,))

    def get_audios(self) -> List[Dict[str, str]]:
        """
        Returns all fully fingerprinted audios in the database

        :return: a dictionary with the audios info.
        """
        with self.cursor(dictionary=True) as cur:
            cur.execute(self.SELECT_AUDIOS)
            return list(cur)

    def get_matched_info(self) -> List[Dict[str, str]]:
        """
        Returns matched information list

        :return: a dictionary with the information info list.
        """
        with self.cursor(dictionary=True) as cur:
            cur.execute(self.SELECT_MATCHED_INFORMATION)
            return list(cur)

    def get_related_audios(self, audio_id: str) -> List[Dict[str, str]]:
        """
        Returns all related audios

        :return: a dictionary with the audios info.
        """
        with self.cursor(dictionary=True) as cur:
            cur.execute(self.SELECT_RELATED_AUDIOS, (audio_id,))
            return list(cur)

    def get_audio_by_id(self, audio_id: int) -> Dict[str, str]:
        """
        Brings the audio info from the database.

        :param audio_id: audio identifier.
        :return: a audio by its identifier. Result must be a Dictionary.
        """
        with self.cursor(dictionary=True) as cur:
            cur.execute(self.SELECT_AUDIO, (audio_id,))
            return cur.fetchone()

    def count_matched_audios_by_sha1(self, file_sha1: str) -> int:
        """
        count matched audio num.

        :param file_sha1: file sha1.
        :return: num.
        """
        with self.cursor(dictionary=True) as cur:
            cur.execute(self.COUNT_MATCHED_AUDIOS, (file_sha1,))
            count = cur.fetchone()[0] if cur.rowcount != 0 else 0

            return count

    def insert(self, fingerprint: str, audio_id: int, offset: int):
        """
        Inserts a single fingerprint into the database.

        :param fingerprint: Part of a sha1 hash, in hexadecimal format
        :param audio_id: Song identifier this fingerprint is off
        :param offset: The offset this fingerprint is from.
        """
        with self.cursor() as cur:
            cur.execute(self.INSERT_FINGERPRINT, (fingerprint, audio_id, offset))

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
    def insert_matched_audios(self, id: str, name: str, file_sha1: str, format: str, storage_path: str, date_created: str):
        """
        Inserts a matched audio into the database, returns the new
        identifier of the audio.
        :param id: The id of the audio.
        :param name: The name of the audio.
        :param file_sha1: The file sha1 of the audio.
        :param format: The format of the audio.
        :param storage_path: The storage path of the audio.
        """
        pass

    @abc.abstractmethod
    def insert_matched_information(self, id: str, audio_id: str, audio_name: str, total_time: float, fingerprint_time: float, query_time: float, align_time: float, date_created: str):
        pass

    @abc.abstractmethod
    def insert_related_audios(self, id: str, audio_id: str, related_audio_id: str, related_audio_name: str, match_id: str,
                              input_total_hashes: int, fingerprinted_hashes_in_db: int, hashes_matched_in_input: int,
                              input_confidence: float, fingerprinted_confidence: float, offset: int,
                              offset_seconds: int, file_sha1: str):
        pass

    def query(self, fingerprint: str = None) -> List[Tuple]:
        """
        Returns all matching fingerprint entries associated with
        the given hash as parameter, if None is passed it returns all entries.

        :param fingerprint: part of a sha1 hash, in hexadecimal format
        :return: a list of fingerprint records stored in the db.
        """
        with self.cursor() as cur:
            if fingerprint:
                cur.execute(self.SELECT, (fingerprint,))
            else:  # select all if no key
                cur.execute(self.SELECT_ALL)
            return list(cur)

    def get_iterable_kv_pairs(self) -> List[Tuple]:
        """
        Returns all fingerprints in the database.

        :return: a list containing all fingerprints stored in the db.
        """
        return self.query(None)

    def insert_hashes(self, audio_id: int, hashes: List[Tuple[str, int]], batch_size: int = 1000) -> None:
        """
        Insert a multitude of fingerprints.

        :param audio_id: Song identifier the fingerprints belong to
        :param hashes: A sequence of tuples in the format (hash, offset)
            - hash: Part of a sha1 hash, in hexadecimal format
            - offset: Offset this hash was created from/at.
        :param batch_size: insert batches.
        """
        values = [(audio_id, hsh, int(offset)) for hsh, offset in hashes]

        with self.cursor() as cur:
            for index in range(0, len(hashes), batch_size):
                cur.executemany(self.INSERT_FINGERPRINT, values[index: index + batch_size])

    def return_matches(self, hashes: List[Tuple[str, int]],
                       batch_size: int = 1000) -> Tuple[List[Tuple[int, int]], Dict[int, int]]:
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
        # Create a dictionary of hash => offset pairs for later lookups
        mapper = {}
        for hsh, offset in hashes:
            if hsh.upper() in mapper.keys():
                mapper[hsh.upper()].append(offset)
            else:
                mapper[hsh.upper()] = [offset]

        values = list(mapper.keys())

        # in order to count each hash only once per db offset we use the dic below
        dedup_hashes = {}

        results = []
        with self.cursor() as cur:
            for index in range(0, len(values), batch_size):
                # Create our IN part of the query
                query = self.SELECT_MULTIPLE % ', '.join([self.IN_MATCH] * len(values[index: index + batch_size]))

                cur.execute(query, values[index: index + batch_size])

                for hsh, sid, offset in cur:
                    if sid not in dedup_hashes.keys():
                        dedup_hashes[sid] = 1
                    else:
                        dedup_hashes[sid] += 1
                    #  we now evaluate all offset for each  hash matched
                    for audio_sampled_offset in mapper[hsh]:
                        results.append((sid, offset - audio_sampled_offset))

            return results, dedup_hashes

    def delete_audios_by_id(self, audio_ids: List[int], batch_size: int = 1000) -> None:
        """
        Given a list of audio ids it deletes all audios specified and their corresponding fingerprints.

        :param audio_ids: audio ids to be deleted from the database.
        :param batch_size: number of query's batches.
        """
        with self.cursor() as cur:
            for index in range(0, len(audio_ids), batch_size):
                # Create our IN part of the query
                query = self.DELETE_AUDIOS % ', '.join(['%s'] * len(audio_ids[index: index + batch_size]))

                cur.execute(query, audio_ids[index: index + batch_size])
