import queue

import mysql.connector
from mysql.connector.errors import DatabaseError

from dejavu.base_classes.common_database import CommonDatabase
from dejavu.config.settings import (FIELD_FILE_SHA1, FIELD_FINGERPRINTED,
                                    FIELD_HASH, FIELD_OFFSET, FIELD_AUDIO_ID,
                                    FIELD_AUDIONAME, FIELD_TOTAL_HASHES,
                                    FINGERPRINTS_TABLENAME, AUDIOS_TABLENAME)


class MySQLDatabase(CommonDatabase):
    type = "mysql"

    # CREATES
    CREATE_AUDIOS_TABLE = f"""
        CREATE TABLE IF NOT EXISTS `{AUDIOS_TABLENAME}` (
            `{FIELD_AUDIO_ID}` MEDIUMINT UNSIGNED NOT NULL AUTO_INCREMENT
        ,   `{FIELD_AUDIONAME}` VARCHAR(250) NOT NULL
        ,   `{FIELD_FINGERPRINTED}` TINYINT DEFAULT 0
        ,   `{FIELD_FILE_SHA1}` BINARY(20) NOT NULL
        ,   `{FIELD_TOTAL_HASHES}` INT NOT NULL DEFAULT 0
        ,   `date_created` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
        ,   `date_modified` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ,   CONSTRAINT `pk_{AUDIOS_TABLENAME}_{FIELD_AUDIO_ID}` PRIMARY KEY (`{FIELD_AUDIO_ID}`)
        ,   CONSTRAINT `uq_{AUDIOS_TABLENAME}_{FIELD_AUDIO_ID}` UNIQUE KEY (`{FIELD_AUDIO_ID}`)
        ) ENGINE=INNODB;
    """

    CREATE_FINGERPRINTS_TABLE = f"""
        CREATE TABLE IF NOT EXISTS `{FINGERPRINTS_TABLENAME}` (
            `{FIELD_HASH}` BINARY(10) NOT NULL
        ,   `{FIELD_AUDIO_ID}` MEDIUMINT UNSIGNED NOT NULL
        ,   `{FIELD_OFFSET}` INT UNSIGNED NOT NULL
        ,   `date_created` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
        ,   `date_modified` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ,   INDEX `ix_{FINGERPRINTS_TABLENAME}_{FIELD_HASH}` (`{FIELD_HASH}`)
        ,   CONSTRAINT `uq_{FINGERPRINTS_TABLENAME}_{FIELD_AUDIO_ID}_{FIELD_OFFSET}_{FIELD_HASH}`
                UNIQUE KEY  (`{FIELD_AUDIO_ID}`, `{FIELD_OFFSET}`, `{FIELD_HASH}`)
        ,   CONSTRAINT `fk_{FINGERPRINTS_TABLENAME}_{FIELD_AUDIO_ID}` FOREIGN KEY (`{FIELD_AUDIO_ID}`)
                REFERENCES `{AUDIOS_TABLENAME}`(`{FIELD_AUDIO_ID}`) ON DELETE CASCADE
    ) ENGINE=INNODB;
    """

    # INSERTS (IGNORES DUPLICATES)
    INSERT_FINGERPRINT = f"""
        INSERT IGNORE INTO `{FINGERPRINTS_TABLENAME}` (
                `{FIELD_AUDIO_ID}`
            ,   `{FIELD_HASH}`
            ,   `{FIELD_OFFSET}`)
        VALUES (%s, UNHEX(%s), %s);
    """

    INSERT_AUDIO = f"""
        INSERT INTO `{AUDIOS_TABLENAME}` (`{FIELD_AUDIONAME}`,`{FIELD_FILE_SHA1}`,`{FIELD_TOTAL_HASHES}`)
        VALUES (%s, UNHEX(%s), %s);
    """

    # SELECTS
    SELECT = f"""
        SELECT `{FIELD_AUDIO_ID}`, `{FIELD_OFFSET}`
        FROM `{FINGERPRINTS_TABLENAME}`
        WHERE `{FIELD_HASH}` = UNHEX(%s);
    """

    SELECT_MULTIPLE = f"""
        SELECT HEX(`{FIELD_HASH}`), `{FIELD_AUDIO_ID}`, `{FIELD_OFFSET}`
        FROM `{FINGERPRINTS_TABLENAME}`
        WHERE `{FIELD_HASH}` IN (%s);
    """

    SELECT_ALL = f"SELECT `{FIELD_AUDIO_ID}`, `{FIELD_OFFSET}` FROM `{FINGERPRINTS_TABLENAME}`;"

    SELECT_AUDIO = f"""
        SELECT `{FIELD_AUDIONAME}`, HEX(`{FIELD_FILE_SHA1}`) AS `{FIELD_FILE_SHA1}`, `{FIELD_TOTAL_HASHES}`
        FROM `{AUDIOS_TABLENAME}`
        WHERE `{FIELD_AUDIO_ID}` = %s;
    """

    SELECT_NUM_FINGERPRINTS = f"SELECT COUNT(*) AS n FROM `{FINGERPRINTS_TABLENAME}`;"

    SELECT_UNIQUE_AUDIO_IDS = f"""
        SELECT COUNT(`{FIELD_AUDIO_ID}`) AS n
        FROM `{AUDIOS_TABLENAME}`
        WHERE `{FIELD_FINGERPRINTED}` = 1;
    """

    SELECT_AUDIOS = f"""
        SELECT
            `{FIELD_AUDIO_ID}`
        ,   `{FIELD_AUDIONAME}`
        ,   HEX(`{FIELD_FILE_SHA1}`) AS `{FIELD_FILE_SHA1}`
        ,   `{FIELD_TOTAL_HASHES}`
        ,   `date_created`
        FROM `{AUDIOS_TABLENAME}`
        WHERE `{FIELD_FINGERPRINTED}` = 1; 
    """

    # DROPS
    DROP_FINGERPRINTS = f"DROP TABLE IF EXISTS `{FINGERPRINTS_TABLENAME}`;"
    DROP_AUDIOS = f"DROP TABLE IF EXISTS `{AUDIOS_TABLENAME}`;"

    # UPDATE
    UPDATE_AUDIO_FINGERPRINTED = f"""
        UPDATE `{AUDIOS_TABLENAME}` SET `{FIELD_FINGERPRINTED}` = 1 WHERE `{FIELD_AUDIO_ID}` = %s;
    """

    # DELETES
    DELETE_UNFINGERPRINTED = f"""
        DELETE FROM `{AUDIOS_TABLENAME}` WHERE `{FIELD_FINGERPRINTED}` = 0;
    """

    DELETE_AUDIOS = f"""
        DELETE FROM `{AUDIOS_TABLENAME}` WHERE `{FIELD_AUDIO_ID}` IN (%s);
    """

    # IN
    IN_MATCH = f"UNHEX(%s)"

    def __init__(self, **options):
        super().__init__()
        self.cursor = cursor_factory(**options)
        self._options = options

    def after_fork(self) -> None:
        # Clear the cursor cache, we don't want any stale connections from
        # the previous process.
        Cursor.clear_cache()

    def insert_audios(self, audio_name: str, file_hash: str, total_hashes: int) -> int:
        """
        Inserts a audio name into the database, returns the new
        identifier of the audio.

        :param audio_name: The name of the audio.
        :param file_hash: Hash from the fingerprinted file.
        :param total_hashes: amount of hashes to be inserted on fingerprint table.
        :return: the inserted id.
        """
        with self.cursor() as cur:
            cur.execute(self.INSERT_AUDIO, (audio_name, file_hash, total_hashes))
            return cur.lastrowid

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
            conn = mysql.connector.connect(**options)

        self.conn = conn
        self.dictionary = dictionary

    @classmethod
    def clear_cache(cls):
        cls._cache = queue.Queue(maxsize=5)

    def __enter__(self):
        self.cursor = self.conn.cursor(dictionary=self.dictionary)
        return self.cursor

    def __exit__(self, extype, exvalue, traceback):
        # if we had a MySQL related error we try to rollback the cursor.
        if extype is DatabaseError:
            self.cursor.rollback()

        self.cursor.close()
        self.conn.commit()

        # Put it back on the queue
        try:
            self._cache.put_nowait(self.conn)
        except queue.Full:
            self.conn.close()
