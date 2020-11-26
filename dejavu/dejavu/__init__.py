import multiprocessing
import os
import sys
import traceback
import uuid
from itertools import groupby
from time import time
from typing import Dict, List, Tuple
import json
import dejavu.logic.decoder as decoder
from dejavu.base_classes.base_database import get_database
from dejavu.base_classes.matched_information import Matched_Information
from dejavu.base_classes.souce_audio import Source_Audio
from dejavu.config.settings import (DEFAULT_FS, DEFAULT_OVERLAP_RATIO,
                                    DEFAULT_WINDOW_SIZE, FIELD_FILE_SHA1,
                                    FIELD_TOTAL_HASHES,FIELD_AUDIO_ID,FIELD_AUDIO_NAME,
                                    FINGERPRINTED_CONFIDENCE,FIELD_FINGERPRINTED,
                                    FINGERPRINTED_HASHES, HASHES_MATCHED,
                                    INPUT_CONFIDENCE, INPUT_HASHES, OFFSET,
                                    FIELD_MATCHED_INFORMATION_ID, FIELD_MATCHED_INFORMATION_AUDIO_ID,
                                    FIELD_MATCHED_INFORMATION_AUDIO_NAME,
                                    FIELD_MATCHED_INFORMATION_TOTAL_TIME, FIELD_MATCHED_INFORMATION_FINGERPRINT_TIME,
                                    FIELD_MATCHED_INFORMATION_QUERY_TIME, FIELD_MATCHED_INFORMATION_ALIGN_TIME,
                                    FIELD_MATCHED_INFORMATION_DATE_CREATED,
                                    FIELD_RELATED_AUDIOS_ID, FIELD_RELATED_AUDIOS_AUDIO_ID,
                                    FIELD_RELATED_AUDIOS_RELATED_AUDIO_ID,
                                    FIELD_RELATED_AUDIOS_RELATED_AUDIO_NAME, FIELD_RELATED_AUDIOS_MATCHED_ID,
                                    FIELD_RELATED_AUDIOS_INPUT_TOTAL_HASHES,
                                    FIELD_RELATED_AUDIOS_FINGERPRINTED_HASHES_IN_DB,
                                    FIELD_RELATED_AUDIOS_HASHES_MATCHED_IN_PUT, FIELD_RELATED_AUDIOS_INPUT_CONFIDENCE,
                                    FIELD_RELATED_AUDIOS_FINGERPRINTED_CONFIDENCE, FIELD_RELATED_AUDIOS_OFFSET,
                                    FIELD_RELATED_AUDIOS_OFFSET_SECONDS, FIELD_RELATED_AUDIOS_FILE_SHA1,
                                    OFFSET_SECS, AUDIO_ID, AUDIO_NAME, TOPN)
from dejavu.logic.fingerprint import fingerprint


class Dejavu:
    def __init__(self, config):
        self.config = config

        # initialize db
        db_cls = get_database(config.get("database_type", "mysql").lower())

        self.db = db_cls(**config.get("database", {}))
        # self.db.setup()

        # if we should limit seconds fingerprinted,
        # None|-1 means use entire track
        self.limit = self.config.get("fingerprint_limit", None)
        if self.limit == -1:  # for JSON compatibility
            self.limit = None
        self.__load_fingerprinted_audio_hashes()

    def __load_fingerprinted_audio_hashes(self) -> None:
        """
        Keeps a dictionary with the hashes of the fingerprinted audios, in that way is possible to check
        whether or not an audio file was already processed.
        """
        # get audios previously indexed
        self.audios = self.db.get_audios()
        self.audiohashes_set = set()  # to know which ones we've computed before
        for audio in self.audios:
            audio_hash = audio[FIELD_FILE_SHA1]
            self.audiohashes_set.add(audio_hash)

    def get_fingerprinted_audios(self) -> List[Dict[str, any]]:
        """
        To pull all fingerprinted audios from the database.

        :return: a list of fingerprinted audios from the database.
        """
        return self.db.get_audios()

    def delete_audios_by_id(self, audio_ids: List[int]) -> None:
        """
        Deletes all audios given their ids.

        :param audio_ids: audio ids to delete from the database.
        """
        self.db.delete_audios_by_id(audio_ids)

    def fingerprint_directory(self, path: str, extensions: str, nprocesses: int = None) -> None:
        """
        Given a directory and a set of extensions it fingerprints all files that match each extension specified.
        给定一个目录和一个后缀集，给所有匹配后缀集的文件抽取指纹
        :param path: path to the directory.
        :param extensions: list of file extensions to consider.
        :param nprocesses: amount of processes to fingerprint the files within the directory.
        """
        # Try to use the maximum amount of processes if not given.
        try:
            nprocesses = nprocesses or multiprocessing.cpu_count()
        except NotImplementedError:
            nprocesses = 1
        else:
            nprocesses = 1 if nprocesses <= 0 else nprocesses

        pool = multiprocessing.Pool(nprocesses)

        filenames_to_fingerprint = []
        for filename, _ in decoder.find_files(path, extensions):
            # don't refingerprint already fingerprinted files
            if decoder.unique_hash(filename) in self.audiohashes_set:
                print(f"{filename} already fingerprinted, continuing...")
                continue

            filenames_to_fingerprint.append(filename)

        # Prepare _fingerprint_worker input
        worker_input = list(zip(filenames_to_fingerprint, [self.limit] * len(filenames_to_fingerprint)))

        # Send off our tasks
        iterator = pool.imap_unordered(Dejavu._fingerprint_worker, worker_input)

        # Loop till we have all of them
        while True:
            try:
                audio_name, hashes, file_hash = next(iterator)
            except multiprocessing.TimeoutError:
                continue
            except StopIteration:
                break
            except Exception:
                print("Failed fingerprinting")
                # Print traceback because we can't reraise it here
                traceback.print_exc(file=sys.stdout)
            else:
                audio_id = uuid.uuid1().hex
                sid = self.db.insert_audios(audio_id, audio_name, file_hash, len(hashes))

                self.db.insert_hashes(sid, hashes)
                self.db.set_audio_fingerprinted(sid)
                self.__load_fingerprinted_audio_hashes()

        pool.close()
        pool.join()

    def fingerprint_file(self, file_path: str, audio_name: str = None) -> None:
        """
        Given a path to a file the method generates hashes for it and stores them in the database
        for later be queried.

        :param file_path: path to the file.
        :param audio_name: audio name associated to the audio file.
        """
        audio_name_from_path = decoder.get_audio_name_from_path(file_path)
        audio_hash = decoder.unique_hash(file_path)
        audio_name = audio_name or audio_name_from_path
        # don't refingerprint already fingerprinted files
        if audio_hash in self.audiohashes_set:
            print(f"{audio_name} already fingerprinted, continuing...")
        else:
            audio_name, hashes, file_hash = Dejavu._fingerprint_worker(
                file_path,
                self.limit,
                audio_name=audio_name
            )
            audio_id = uuid.uuid1().hex
            sid = self.db.insert_audios(audio_id, audio_name, file_hash)

            self.db.insert_hashes(sid, hashes)
            self.db.set_audio_fingerprinted(sid)
            self.__load_fingerprinted_audio_hashes()

    def generate_fingerprints(self, samples: List[int], Fs=DEFAULT_FS) -> Tuple[List[Tuple[str, int]], float]:
        f"""
        Generate the fingerprints for the given sample data (channel).

        :param samples: list of ints which represents the channel info of the given audio file.
        :param Fs: sampling rate which defaults to {DEFAULT_FS}.
        :return: a list of tuples for hash and its corresponding offset, together with the generation time.
        """
        t = time()
        hashes = fingerprint(samples, Fs=Fs)
        fingerprint_time = time() - t
        return hashes, fingerprint_time

    def find_matches(self, hashes: List[Tuple[str, int]]) -> Tuple[List[Tuple[int, int]], Dict[str, int], float]:
        """
        Finds the corresponding matches on the fingerprinted audios for the given hashes.

        :param hashes: list of tuples for hashes and their corresponding offsets
        :return: a tuple containing the matches found against the db, a dictionary which counts the different
         hashes matched for each audio (with the audio id as key), and the time that the query took.

        """
        t = time()
        matches, dedup_hashes = self.db.return_matches(hashes)
        query_time = time() - t

        return matches, dedup_hashes, query_time

    def find_matched_info(self, related_key:str, confidence: float) -> list:
        matched_infos = self.db.get_matched_info(related_key)
        matched_informations = list()
        for info in matched_infos:
            audio_id = info.get(FIELD_MATCHED_INFORMATION_AUDIO_ID, None)
            audio_name = info.get(FIELD_MATCHED_INFORMATION_AUDIO_NAME, None)
            total_time = info.get(FIELD_MATCHED_INFORMATION_TOTAL_TIME, None)
            fingerprint_time = info.get(FIELD_MATCHED_INFORMATION_FINGERPRINT_TIME, None)
            align_time = info.get(FIELD_MATCHED_INFORMATION_ALIGN_TIME, None)
            query_time = info.get(FIELD_MATCHED_INFORMATION_QUERY_TIME, None)
            date_created = info.get(FIELD_MATCHED_INFORMATION_DATE_CREATED, None)
            related_audios = list()
            ras = self.db.get_related_audios(audio_id, confidence)
            for ra in ras:
                audio = {
                    FIELD_RELATED_AUDIOS_RELATED_AUDIO_ID: ra.get(FIELD_RELATED_AUDIOS_RELATED_AUDIO_ID, None),
                    FIELD_RELATED_AUDIOS_RELATED_AUDIO_NAME: ra.get(FIELD_RELATED_AUDIOS_RELATED_AUDIO_NAME, None),
                    FIELD_RELATED_AUDIOS_MATCHED_ID: ra.get(FIELD_RELATED_AUDIOS_MATCHED_ID, None),
                    FIELD_RELATED_AUDIOS_INPUT_TOTAL_HASHES: ra.get(FIELD_RELATED_AUDIOS_INPUT_TOTAL_HASHES, None),
                    FIELD_RELATED_AUDIOS_FINGERPRINTED_HASHES_IN_DB: ra.get(
                        FIELD_RELATED_AUDIOS_FINGERPRINTED_HASHES_IN_DB, None),
                    FIELD_RELATED_AUDIOS_HASHES_MATCHED_IN_PUT: ra.get(FIELD_RELATED_AUDIOS_HASHES_MATCHED_IN_PUT,
                                                                       None),
                    FIELD_RELATED_AUDIOS_INPUT_CONFIDENCE: ra.get(FIELD_RELATED_AUDIOS_INPUT_CONFIDENCE, None),
                    FIELD_RELATED_AUDIOS_FINGERPRINTED_CONFIDENCE: ra.get(FIELD_RELATED_AUDIOS_FINGERPRINTED_CONFIDENCE,
                                                                          None),
                    FIELD_RELATED_AUDIOS_OFFSET: ra.get(FIELD_RELATED_AUDIOS_OFFSET, None),
                    FIELD_RELATED_AUDIOS_OFFSET_SECONDS: ra.get(FIELD_RELATED_AUDIOS_OFFSET_SECONDS, None),
                    FIELD_RELATED_AUDIOS_FILE_SHA1: ra.get(FIELD_RELATED_AUDIOS_FILE_SHA1, None)
                }
                related_audios.append(audio)
            most_similar = ''
            confidence = 0
            if ras:
                most_similar = ras[0].get(FIELD_RELATED_AUDIOS_RELATED_AUDIO_NAME, None)
                confidence = ra.get(FIELD_RELATED_AUDIOS_FINGERPRINTED_CONFIDENCE,None)
            matched = Matched_Information(audio_id, audio_name, total_time, fingerprint_time, align_time, query_time,
                                          related_audios, date_created, most_similar, confidence)
            matched_informations.append(matched)
        return matched_informations

    def find_source_audio(self, name: str) -> list:
        audios = self.db.get_source_audio(name)
        source_audios = list()
        for audio in audios:
            audio_id = audio.get(FIELD_AUDIO_ID, None)
            audio_name = audio.get(FIELD_AUDIO_NAME, None)
            fingerprinted = audio.get(FIELD_FINGERPRINTED, None)
            file_sha1 = audio.get(FIELD_FILE_SHA1, None)
            total_hashes = audio.get(FIELD_TOTAL_HASHES, None)
            source_audio = Source_Audio(audio_id, audio_name, fingerprinted, file_sha1, total_hashes)
            source_audios.append(source_audio)
        return source_audios

    def align_matches(self, matches: List[Tuple[int, int]], dedup_hashes: Dict[str, int], queried_hashes: int,
                      topn: int = TOPN) -> List[Dict[str, any]]:
        """
        Finds hash matches that align in time with other matches and finds
        consensus about which hashes are "true" signal from the audio.

        :param matches: matches from the database
        :param dedup_hashes: dictionary containing the hashes matched without duplicates for each audio
        (key is the audio id).
        :param queried_hashes: amount of hashes sent for matching against the db
        :param topn: number of results being returned back.
        :return: a list of dictionaries (based on topn) with match information.
        """
        # count offset occurrences per audio and keep only the maximum ones.
        sorted_matches = sorted(matches, key=lambda m: (m[0], m[1]))
        counts = [(*key, len(list(group))) for key, group in groupby(sorted_matches, key=lambda m: (m[0], m[1]))]
        audios_matches = sorted(
            [max(list(group), key=lambda g: g[2]) for key, group in groupby(counts, key=lambda count: count[0])],
            key=lambda count: count[2], reverse=True
        )

        audios_result = []
        for audio_id, offset, _ in audios_matches[0:topn]:  # consider topn elements in the result
            audio = self.db.get_audio_by_id(audio_id)

            audio_name = audio.get(AUDIO_NAME, None)
            audio_hashes = audio.get(FIELD_TOTAL_HASHES, None)
            nseconds = round(float(offset) / DEFAULT_FS * DEFAULT_WINDOW_SIZE * DEFAULT_OVERLAP_RATIO, 5)
            hashes_matched = dedup_hashes[audio_id]

            audio_result = {
                AUDIO_ID: audio_id,
                AUDIO_NAME: audio_name,
                INPUT_HASHES: queried_hashes,
                FINGERPRINTED_HASHES: audio_hashes,
                HASHES_MATCHED: hashes_matched,
                # Percentage regarding hashes matched Video-Spider hashes from the input.
                INPUT_CONFIDENCE: round(hashes_matched / queried_hashes, 2),
                # Percentage regarding hashes matched Video-Spider hashes fingerprinted in the db.
                FINGERPRINTED_CONFIDENCE: round(hashes_matched / audio_hashes, 2),
                OFFSET: offset,
                OFFSET_SECS: nseconds,
                FIELD_FILE_SHA1: audio.get(FIELD_FILE_SHA1, None).encode("utf8")
            }
            audios_result.append(audio_result)
        return audios_result

    def recognize(self, recognizer, *options, **kwoptions) -> Dict[str, any]:
        r = recognizer(self)
        return r.recognize(*options, **kwoptions)

    @staticmethod
    def _fingerprint_worker(arguments):
        # Pool.imap sends arguments as tuples so we have to unpack
        # them ourself.
        try:
            file_name, limit = arguments
        except ValueError:
            pass

        audio_name = os.path.basename(file_name)

        fingerprints, file_hash = Dejavu.get_file_fingerprints(file_name, limit, print_output=True)

        return audio_name, fingerprints, file_hash

    @staticmethod
    def get_file_fingerprints(file_name: str, limit: int, print_output: bool = False):
        channels, fs, file_hash = decoder.read(file_name, limit)
        fingerprints = set()
        channel_amount = len(channels)
        for channeln, channel in enumerate(channels, start=1):
            if print_output:
                print(f"Fingerprinting channel {channeln}/{channel_amount} for {file_name}")

            hashes = fingerprint(channel, Fs=fs)

            if print_output:
                print(f"Finished channel {channeln}/{channel_amount} for {file_name}")

            fingerprints |= set(hashes)

        return fingerprints, file_hash
