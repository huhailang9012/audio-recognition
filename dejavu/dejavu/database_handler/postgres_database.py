import queue

import psycopg2
from psycopg2.extras import DictCursor

from dejavu.base_classes.common_database import CommonDatabase
from dejavu.config.settings import (FIELD_FILE_SHA1, FIELD_FINGERPRINTED,
                                    FIELD_HASH, FIELD_OFFSET, FIELD_AUDIO_ID,
                                    FIELD_AUDIO_NAME, FIELD_TOTAL_HASHES,
                                    FIELD_MATCHED_AUDIO_ID,
                                    FIELD_MATCHED_AUDIO_FILE_MD5, FIELD_MATCHED_AUDIO_RELATED_KEY,
                                    FIELD_MATCHED_INFORMATION_ID, FIELD_MATCHED_INFORMATION_AUDIO_ID, FIELD_MATCHED_INFORMATION_AUDIO_NAME,
                                    FIELD_MATCHED_INFORMATION_TOTAL_TIME, FIELD_MATCHED_INFORMATION_FINGERPRINT_TIME,
                                    FIELD_MATCHED_INFORMATION_QUERY_TIME, FIELD_MATCHED_INFORMATION_ALIGN_TIME,
                                    FIELD_MATCHED_INFORMATION_DATE_CREATED,FIELD_MATCHED_INFORMATION_AUDIO_MD5,
                                    FIELD_RELATED_AUDIOS_ID, FIELD_RELATED_AUDIOS_AUDIO_ID,
                                    FIELD_RELATED_AUDIOS_RELATED_AUDIO_ID,
                                    FIELD_RELATED_AUDIOS_RELATED_AUDIO_NAME, FIELD_RELATED_AUDIOS_MATCHED_ID,
                                    FIELD_RELATED_AUDIOS_INPUT_TOTAL_HASHES,
                                    FIELD_RELATED_AUDIOS_FINGERPRINTED_HASHES_IN_DB,
                                    FIELD_RELATED_AUDIOS_HASHES_MATCHED_IN_PUT, FIELD_RELATED_AUDIOS_INPUT_CONFIDENCE,
                                    FIELD_RELATED_AUDIOS_FINGERPRINTED_CONFIDENCE, FIELD_RELATED_AUDIOS_OFFSET,
                                    FIELD_RELATED_AUDIOS_OFFSET_SECONDS, FIELD_RELATED_AUDIOS_FILE_SHA1,
                                    MATCHED_AUDIOS_TABLE_NAME, MATCHED_INFORMATION_TABLE_NAME,
                                    RELATED_AUDIOS_TABLE_NAME, FINGERPRINTS_TABLE_NAME, AUDIOS_TABLE_NAME)


class PostgreSQLDatabase(CommonDatabase):
    type = "postgres"

    # CREATES
    CREATE_AUDIOS_TABLE = f"""
        CREATE TABLE IF NOT EXISTS "{AUDIOS_TABLE_NAME}" (
            "{FIELD_AUDIO_ID}" CHAR(32)
        ,   "{FIELD_AUDIO_NAME}" VARCHAR(250) NOT NULL
        ,   "{FIELD_FINGERPRINTED}" SMALLINT DEFAULT 0
        ,   "{FIELD_FILE_SHA1}" BYTEA
        ,   "{FIELD_TOTAL_HASHES}" INT NOT NULL DEFAULT 0
        ,   "date_created" TIMESTAMP NOT NULL DEFAULT now()
        ,   "date_modified" TIMESTAMP NOT NULL DEFAULT now()
        ,   CONSTRAINT "pk_{AUDIOS_TABLE_NAME}_{FIELD_AUDIO_ID}" PRIMARY KEY ("{FIELD_AUDIO_ID}")
        ,   CONSTRAINT "uq_{AUDIOS_TABLE_NAME}_{FIELD_AUDIO_ID}" UNIQUE ("{FIELD_AUDIO_ID}")
        );
    """

    CREATE_FINGERPRINTS_TABLE = f"""
        CREATE TABLE IF NOT EXISTS "{FINGERPRINTS_TABLE_NAME}" (
            "{FIELD_HASH}" BYTEA NOT NULL
        ,   "{FIELD_AUDIO_ID}" CHAR(32) NOT NULL
        ,   "{FIELD_OFFSET}" INT NOT NULL
        ,   "date_created" TIMESTAMP NOT NULL DEFAULT now()
        ,   "date_modified" TIMESTAMP NOT NULL DEFAULT now()
        ,   CONSTRAINT "uq_{FINGERPRINTS_TABLE_NAME}" UNIQUE  ("{FIELD_AUDIO_ID}", "{FIELD_OFFSET}", "{FIELD_HASH}")
        ,   CONSTRAINT "fk_{FINGERPRINTS_TABLE_NAME}_{FIELD_AUDIO_ID}" FOREIGN KEY ("{FIELD_AUDIO_ID}")
                REFERENCES "{AUDIOS_TABLE_NAME}"("{FIELD_AUDIO_ID}") ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS "ix_{FINGERPRINTS_TABLE_NAME}_{FIELD_HASH}" ON "{FINGERPRINTS_TABLE_NAME}"
        USING hash ("{FIELD_HASH}");
    """

    CREATE_FINGERPRINTS_TABLE_INDEX = f"""
        CREATE INDEX "ix_{FINGERPRINTS_TABLE_NAME}_{FIELD_HASH}" ON "{FINGERPRINTS_TABLE_NAME}"
        USING hash ("{FIELD_HASH}");
    """

    CREATE_MATCHED_INFORMATION_TABLE = f"""
        CREATE TABLE IF NOT EXISTS "{MATCHED_INFORMATION_TABLE_NAME}" (
            "{FIELD_MATCHED_INFORMATION_ID}" CHAR(32) PRIMARY KEY
        ,   "{FIELD_MATCHED_INFORMATION_AUDIO_ID}" CHAR(32) NOT NULL
        ,   "{FIELD_MATCHED_INFORMATION_AUDIO_NAME}" VARCHAR (128) NOT NULL
        ,   "{FIELD_MATCHED_INFORMATION_AUDIO_MD5}" CHAR(32) NOT NULL
        ,   "{FIELD_MATCHED_INFORMATION_TOTAL_TIME}" REAL
        ,   "{FIELD_MATCHED_INFORMATION_FINGERPRINT_TIME}" REAL
        ,   "{FIELD_MATCHED_INFORMATION_QUERY_TIME}" REAL
        ,   "{FIELD_MATCHED_INFORMATION_ALIGN_TIME}" REAL
        ,   "{FIELD_MATCHED_INFORMATION_DATE_CREATED}" CHAR(19) NOT NULL
        ,   "{FIELD_MATCHED_AUDIO_RELATED_KEY}" VARCHAR(64)
        );
    """

    CREATE_RELATED_AUDIOS_TABLE = f"""
        CREATE TABLE IF NOT EXISTS "{RELATED_AUDIOS_TABLE_NAME}" (
            "{FIELD_RELATED_AUDIOS_ID}" CHAR(32) PRIMARY KEY
        ,   "{FIELD_RELATED_AUDIOS_AUDIO_ID}" CHAR(32) NOT NULL
        ,   "{FIELD_RELATED_AUDIOS_RELATED_AUDIO_ID}" CHAR(32) NOT NULL
        ,   "{FIELD_RELATED_AUDIOS_RELATED_AUDIO_NAME}" VARCHAR(128)
        ,   "{FIELD_RELATED_AUDIOS_MATCHED_ID}" CHAR(32) NOT NULL
        ,   "{FIELD_RELATED_AUDIOS_INPUT_TOTAL_HASHES}" INT
        ,   "{FIELD_RELATED_AUDIOS_FINGERPRINTED_HASHES_IN_DB}" INT
        ,   "{FIELD_RELATED_AUDIOS_HASHES_MATCHED_IN_PUT}" INT
        ,   "{FIELD_RELATED_AUDIOS_INPUT_CONFIDENCE}" REAL
        ,   "{FIELD_RELATED_AUDIOS_FINGERPRINTED_CONFIDENCE}" REAL
        ,   "{FIELD_RELATED_AUDIOS_OFFSET}" INT
        ,   "{FIELD_RELATED_AUDIOS_OFFSET_SECONDS}" INT
        ,   "{FIELD_RELATED_AUDIOS_FILE_SHA1}" BYTEA
        ,   CONSTRAINT "fk_{RELATED_AUDIOS_TABLE_NAME}_{FIELD_RELATED_AUDIOS_RELATED_AUDIO_ID}" FOREIGN KEY ("{FIELD_RELATED_AUDIOS_RELATED_AUDIO_ID}") REFERENCES "{AUDIOS_TABLE_NAME}"("{FIELD_AUDIO_ID}")
        ,   CONSTRAINT "fk_{RELATED_AUDIOS_TABLE_NAME}_{FIELD_RELATED_AUDIOS_MATCHED_ID}" FOREIGN KEY ("{FIELD_RELATED_AUDIOS_MATCHED_ID}") REFERENCES "{MATCHED_INFORMATION_TABLE_NAME}"("{FIELD_MATCHED_INFORMATION_ID}")
        );
    """

    # INSERTS (IGNORES DUPLICATES)
    INSERT_FINGERPRINT = f"""
        INSERT INTO "{FINGERPRINTS_TABLE_NAME}" (
                "{FIELD_AUDIO_ID}"
            ,   "{FIELD_HASH}"
            ,   "{FIELD_OFFSET}")
        VALUES (%s, decode(%s, 'hex'), %s) ON CONFLICT DO NOTHING;
    """

    INSERT_AUDIOS = f"""
        INSERT INTO "{AUDIOS_TABLE_NAME}" ("{FIELD_AUDIO_ID}","{FIELD_AUDIO_NAME}", "{FIELD_FILE_SHA1}","{FIELD_TOTAL_HASHES}")
        VALUES (%s, %s, decode(%s, 'hex'), %s)
        RETURNING "{FIELD_AUDIO_ID}";
    """

    INSERT_MATCHED_INFORMATION = f"""
        INSERT INTO "{MATCHED_INFORMATION_TABLE_NAME}" ("{FIELD_MATCHED_INFORMATION_ID}","{FIELD_MATCHED_INFORMATION_AUDIO_ID}","{FIELD_MATCHED_INFORMATION_AUDIO_NAME}", "{FIELD_MATCHED_INFORMATION_AUDIO_MD5}","{FIELD_MATCHED_INFORMATION_TOTAL_TIME}","{FIELD_MATCHED_INFORMATION_FINGERPRINT_TIME}","{FIELD_MATCHED_INFORMATION_QUERY_TIME}","{FIELD_MATCHED_INFORMATION_ALIGN_TIME}","{FIELD_MATCHED_INFORMATION_DATE_CREATED}","{FIELD_MATCHED_AUDIO_RELATED_KEY}")
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    INSERT_RELATED_AUDIOS = f"""
        INSERT INTO "{RELATED_AUDIOS_TABLE_NAME}" ("{FIELD_RELATED_AUDIOS_ID}","{FIELD_RELATED_AUDIOS_AUDIO_ID}","{FIELD_RELATED_AUDIOS_RELATED_AUDIO_ID}","{FIELD_RELATED_AUDIOS_RELATED_AUDIO_NAME}","{FIELD_RELATED_AUDIOS_MATCHED_ID}",
        "{FIELD_RELATED_AUDIOS_INPUT_TOTAL_HASHES}","{FIELD_RELATED_AUDIOS_FINGERPRINTED_HASHES_IN_DB}","{FIELD_RELATED_AUDIOS_HASHES_MATCHED_IN_PUT}","{FIELD_RELATED_AUDIOS_INPUT_CONFIDENCE}","{FIELD_RELATED_AUDIOS_FINGERPRINTED_CONFIDENCE}",
        "{FIELD_RELATED_AUDIOS_OFFSET}","{FIELD_RELATED_AUDIOS_OFFSET_SECONDS}","{FIELD_RELATED_AUDIOS_FILE_SHA1}")
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    # SELECTS
    SELECT = f"""
        SELECT "{FIELD_AUDIO_ID}", "{FIELD_OFFSET}"
        FROM "{FINGERPRINTS_TABLE_NAME}"
        WHERE "{FIELD_HASH}" = decode(%s, 'hex');       
    """

    SELECT_MULTIPLE = f"""
        SELECT upper(encode("{FIELD_HASH}", 'hex')), "{FIELD_AUDIO_ID}", "{FIELD_OFFSET}"
        FROM "{FINGERPRINTS_TABLE_NAME}"
        WHERE "{FIELD_HASH}" IN (%s);
    """

    SELECT_ALL = f'SELECT "{FIELD_AUDIO_ID}", "{FIELD_OFFSET}" FROM "{FINGERPRINTS_TABLE_NAME}";'

    SELECT_AUDIO = f"""
        SELECT
            "{FIELD_AUDIO_NAME}"
        ,   upper(encode("{FIELD_FILE_SHA1}", 'hex')) AS "{FIELD_FILE_SHA1}"
        ,   "{FIELD_TOTAL_HASHES}"
        FROM "{AUDIOS_TABLE_NAME}"
        WHERE "{FIELD_AUDIO_ID}" = %s;
    """

    SELECT_NUM_FINGERPRINTS = f'SELECT COUNT(*) AS n FROM "{FINGERPRINTS_TABLE_NAME}";'

    COUNT_MATCHED_AUDIOS = f'SELECT COUNT(*)  FROM "{MATCHED_INFORMATION_TABLE_NAME}" WHERE "{FIELD_MATCHED_INFORMATION_AUDIO_MD5}" = %s;'

    SELECT_UNIQUE_AUDIO_IDS = f"""
        SELECT COUNT("{FIELD_AUDIO_ID}") AS n
        FROM "{AUDIOS_TABLE_NAME}"
        WHERE "{FIELD_FINGERPRINTED}" = 1;
    """

    SELECT_AUDIOS = f"""
        SELECT
            "{FIELD_AUDIO_ID}"
        ,   "{FIELD_AUDIO_NAME}"
        ,   upper(encode("{FIELD_FILE_SHA1}", 'hex')) AS "{FIELD_FILE_SHA1}"
        ,   "{FIELD_TOTAL_HASHES}"
        ,   "date_created"
        FROM "{AUDIOS_TABLE_NAME}"
        WHERE "{FIELD_FINGERPRINTED}" = 1;
    """

    SELECT_MATCHED_INFORMATION = f"""
        SELECT
            "{FIELD_MATCHED_INFORMATION_ID}"
        ,   "{FIELD_MATCHED_INFORMATION_AUDIO_ID}"
        ,   "{FIELD_MATCHED_INFORMATION_AUDIO_NAME}"
        ,   "{FIELD_MATCHED_INFORMATION_AUDIO_MD5}"
        ,   "{FIELD_MATCHED_INFORMATION_TOTAL_TIME}"
        ,   "{FIELD_MATCHED_INFORMATION_FINGERPRINT_TIME}"
        ,   "{FIELD_MATCHED_INFORMATION_QUERY_TIME}"
        ,   "{FIELD_MATCHED_INFORMATION_ALIGN_TIME}"
        ,   "{FIELD_MATCHED_INFORMATION_DATE_CREATED}"
        ,   "{FIELD_MATCHED_AUDIO_RELATED_KEY}"
        FROM "{MATCHED_INFORMATION_TABLE_NAME}"
        WHERE "{FIELD_MATCHED_AUDIO_RELATED_KEY}" = %s
        ORDER BY "{FIELD_MATCHED_INFORMATION_DATE_CREATED}" DESC LIMIT 20;
    """

    SELECT_RELATED_AUDIOS = f"""
        SELECT
            "{FIELD_RELATED_AUDIOS_ID}"
        ,   "{FIELD_RELATED_AUDIOS_AUDIO_ID}"
        ,   "{FIELD_RELATED_AUDIOS_RELATED_AUDIO_ID}"
        ,   "{FIELD_RELATED_AUDIOS_RELATED_AUDIO_NAME}"
        ,   "{FIELD_RELATED_AUDIOS_MATCHED_ID}"
        ,   "{FIELD_RELATED_AUDIOS_INPUT_TOTAL_HASHES}"
        ,   "{FIELD_RELATED_AUDIOS_FINGERPRINTED_HASHES_IN_DB}"
        ,   "{FIELD_RELATED_AUDIOS_HASHES_MATCHED_IN_PUT}"
        ,   "{FIELD_RELATED_AUDIOS_INPUT_CONFIDENCE}"
        ,   "{FIELD_RELATED_AUDIOS_FINGERPRINTED_CONFIDENCE}"
        ,   "{FIELD_RELATED_AUDIOS_OFFSET}"
        ,   "{FIELD_RELATED_AUDIOS_OFFSET_SECONDS}"
        ,   upper(encode("{FIELD_RELATED_AUDIOS_FILE_SHA1}", 'hex')) AS "{FIELD_FILE_SHA1}"
        FROM "{RELATED_AUDIOS_TABLE_NAME}"
        WHERE "{FIELD_RELATED_AUDIOS_AUDIO_ID}" = %s;
    """

    # DROPS
    DROP_FINGERPRINTS = F'DROP TABLE IF EXISTS "{FINGERPRINTS_TABLE_NAME}";'
    DROP_AUDIOS = F'DROP TABLE IF EXISTS "{AUDIOS_TABLE_NAME}";'
    DROP_MATCHED_INFORMATION = F'DROP TABLE IF EXISTS "{MATCHED_INFORMATION_TABLE_NAME}";'
    DROP_RELATED_AUDIOS = F'DROP TABLE IF EXISTS "{RELATED_AUDIOS_TABLE_NAME}";'

    # UPDATE
    UPDATE_AUDIO_FINGERPRINTED = f"""
        UPDATE "{AUDIOS_TABLE_NAME}" SET
            "{FIELD_FINGERPRINTED}" = 1
        ,   "date_modified" = now()
        WHERE "{FIELD_AUDIO_ID}" = %s;
    """

    # DELETES
    DELETE_UNFINGERPRINTED = f"""
        DELETE FROM "{AUDIOS_TABLE_NAME}" WHERE "{FIELD_FINGERPRINTED}" = 0;
    """

    DELETE_AUDIOS = f"""
        DELETE FROM "{AUDIOS_TABLE_NAME}" WHERE "{FIELD_AUDIO_ID}" IN (%s);
    """

    # IN
    IN_MATCH = f"decode(%s, 'hex')"

    def __init__(self, **options):
        super().__init__()
        self.cursor = cursor_factory(**options)
        self._options = options

    def after_fork(self) -> None:
        # Clear the cursor cache, we don't want any stale connections from
        # the previous process.
        Cursor.clear_cache()

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
        with self.cursor() as cur:
            cur.execute(self.INSERT_AUDIOS, (audio_id, audio_name, file_hash, total_hashes))
            return cur.fetchone()[0]

    def insert_matched_information(self, id: str, audio_id: str, audio_name: str, audio_md5: str,total_time: float,
                                   fingerprint_time: float, query_time: float, align_time: float, date_created: str, related_key: str):
        with self.cursor() as cur:
            cur.execute(self.INSERT_MATCHED_INFORMATION,
                        (id, audio_id, audio_name, audio_md5, total_time, fingerprint_time, query_time, align_time, date_created, related_key))

    def insert_related_audios(self, id: str, audio_id: str, related_audio_id: str, related_audio_name: str,
                              match_id: str, input_total_hashes: int, fingerprinted_hashes_in_db: int,
                              hashes_matched_in_input: int, input_confidence: float, fingerprinted_confidence: float,
                              offset: int, offset_seconds: int, file_sha1: str):
        with self.cursor() as cur:
            cur.execute(self.INSERT_RELATED_AUDIOS, (
                id, audio_id, related_audio_id, related_audio_name, match_id, input_total_hashes,
                fingerprinted_hashes_in_db, hashes_matched_in_input, input_confidence, fingerprinted_confidence, offset,
                offset_seconds, file_sha1))

    def __getstate__(self):
        return self._options,

    def __setstate__(self, state):
        self._options, = state
        self.cursor = cursor_factory(**self._options)


def cursor_factory(**factory_options):
    def cursor(**options):
        options.update(factory_options)
        return Cursor(**options)

    return cursor


class Cursor(object):
    """
    Establishes a connection to the database and returns an open cursor.
    # Use as context manager
    with Cursor() as cur:
        cur.execute(query)
        ...
    """

    def __init__(self, dictionary=False, **options):
        super().__init__()

        self._cache = queue.Queue(maxsize=5)

        try:
            conn = self._cache.get_nowait()
            # Ping the connection before using it from the cache.
            conn.ping(True)
        except queue.Empty:
            conn = psycopg2.connect(**options)

        self.conn = conn
        self.dictionary = dictionary

    @classmethod
    def clear_cache(cls):
        cls._cache = queue.Queue(maxsize=5)

    def __enter__(self):
        if self.dictionary:
            self.cursor = self.conn.cursor(cursor_factory=DictCursor)
        else:
            self.cursor = self.conn.cursor()
        return self.cursor

    def __exit__(self, extype, exvalue, traceback):
        # if we had a PostgreSQL related error we try to rollback the cursor.
        if extype is psycopg2.DatabaseError:
            self.cursor.rollback()

        self.cursor.close()
        self.conn.commit()

        # Put it back on the queue
        try:
            self._cache.put_nowait(self.conn)
        except queue.Full:
            self.conn.close()
