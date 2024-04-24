from dataclasses import dataclass
from typing import List, Mapping
import heapq
import duckdb
from dagster import DagsterLogManager
from .common import GenericGCSAsset
from dagster_gcp import BigQueryResource, GCSResource


class GoldskyAsset(GenericGCSAsset):
    def clean_up(self):
        pass


@dataclass
class GoldskyConfig:
    project_id: str
    bucket_name: str
    dataset_name: str
    table_name: str
    partition_column_name: str
    size: int
    bucket_key_id: str
    bucket_secret: str


@dataclass
class GoldskyContext:
    bigquery: BigQueryResource
    gcs: GCSResource


class GoldskyWorkerLoader:
    def __init__(self, worker: str):
        pass


@dataclass
class GoldskyQueueItem:
    checkpoint: int
    blob_name: str

    def __lt__(self, other):
        return self.checkpoint < other.checkpoint


class GoldskyQueue:
    def __init__(self, max_size: int):
        self.queue = []
        self._dequeues = 0
        self.max_size = max_size

    def enqueue(self, item: GoldskyQueueItem):
        heapq.heappush(self.queue, item)

    def dequeue(self) -> GoldskyQueueItem | None:
        if self._dequeues > self.max_size - 1:
            return None
        try:
            item = heapq.heappop(self.queue)
            self._dequeues += 1
            return item
        except IndexError:
            return None

    def len(self):
        return len(self.queue)


class GoldskyQueues:
    def __init__(self, max_size: int):
        self.queues: Mapping[str, GoldskyQueue] = {}
        self.max_size = max_size

    def enqueue(self, worker: str, item: GoldskyQueueItem):
        queue = self.queues.get(worker, GoldskyQueue(max_size=self.max_size))
        queue.enqueue(item)
        self.queues[worker] = queue

    def dequeue(self, worker: str) -> GoldskyQueueItem | None:
        queue = self.queues.get(worker, GoldskyQueue(max_size=self.max_size))
        return queue.dequeue()

    def workers(self):
        return self.queues.keys()

    def status(self):
        status: Mapping[str, int] = {}
        for worker, queue in self.queues.items():
            status[worker] = queue.len()
        return status

    def worker_queues(self):
        return self.queues.items()


class GoldskyDuckDB:
    @classmethod
    def connect(
        cls,
        destination_path: str,
        bucket_name: str,
        key_id: str,
        secret: str,
        path: str,
        log: DagsterLogManager,
        memory_limit: str = "16GB",
    ):
        conn = duckdb.connect(path)
        conn.sql(
            f"""
        CREATE SECRET (
            TYPE GCS,
            KEY_ID '{key_id}',
            SECRET '{secret}'
        );
        """
        )
        conn.sql(f"SET memory_limit = '{memory_limit}';")
        return cls(bucket_name, destination_path, log, conn)

    def __init__(
        self,
        bucket_name: str,
        destination_path: str,
        log: DagsterLogManager,
        conn: duckdb.DuckDBPyConnection,
    ):
        self.destination_path = destination_path
        self.bucket_name = bucket_name
        self.conn = conn
        self.log = log

    def full_dest_table_path(self, worker: str, batch_id: int):
        return f"gs://{self.bucket_name}/{self.destination_path}/{worker}/table_{batch_id}.parquet"

    def full_dest_delete_path(self, worker: str, batch_id: int):
        return f"gs://{self.bucket_name}/{self.destination_path}/{worker}/delete_{batch_id}.parquet"

    def full_dest_deduped_path(self, worker: str, batch_id: int):
        return f"gs://{self.bucket_name}/{self.destination_path}/{worker}/deduped_{batch_id}.parquet"

    def wildcard_deduped_path(self, worker: str):
        return f"gs://{self.bucket_name}/{self.destination_path}/{worker}/deduped_*.parquet"

    def remove_dupes(self, worker: str, batches: List[int]):
        for batch_id in batches[:-1]:
            self.remove_dupe_for_batch(worker, batch_id)
        self.remove_dupe_for_batch(worker, batches[-1], last=True)

    def remove_dupe_for_batch(self, worker: str, batch_id: int, last: bool = False):
        self.log.info(f"removing duplicates for batch {batch_id}")
        self.conn.sql(
            f"""
        CREATE OR REPLACE TABLE deduped_{worker}_{batch_id}
        AS
        SELECT * FROM read_parquet('{self.full_dest_table_path(worker, batch_id)}')
        """
        )

        if not last:
            self.conn.sql(
                f""" 
            DELETE FROM deduped_{worker}_{batch_id}
            WHERE id in (
                SELECT id FROM read_parquet('{self.full_dest_delete_path(worker, batch_id)}')
            )
            """
            )

        self.conn.sql(
            f"""
        COPY deduped_{worker}_{batch_id} TO '{self.full_dest_deduped_path(worker, batch_id)}';
        """
        )

    def load_and_merge(self, worker: str, batch_id: int, blob_names: List[str]):
        conn = self.conn
        bucket_name = self.bucket_name

        base = f"gs://{bucket_name}"

        size = len(blob_names)

        checkpoint_temp_tables: List[str] = []
        for i in range(size):
            self.log.info(f"Creating a view for blob {base}/{blob_names[i]}")
            file_ref = f"{base}/{blob_names[i]}"
            checkpoint_temp_table = f"checkpoint_{worker}_{batch_id}_{i}"
            conn.sql(
                f"""
            CREATE TEMP TABLE {checkpoint_temp_table}
            AS
            SELECT {i} AS checkpoint_order, *
            FROM read_parquet('{file_ref}');
            """
            )
            checkpoint_temp_tables.append(checkpoint_temp_table)

        merged_table = f"merged_{worker}_{batch_id}"
        conn.sql(
            f"""
        CREATE OR REPLACE TABLE {merged_table}
        AS
        SELECT *
        FROM checkpoint_{worker}_{batch_id}_0
        """
        )

        for i in range(size - 1):
            self.log.debug(f"Merging {blob_names[i+1]}")
            checkpoint_table = checkpoint_temp_tables[i + 1]
            rows = conn.sql(
                f"""
            SELECT *
            FROM {merged_table} AS m
            INNER JOIN {checkpoint_table} as ch
            ON m.id = ch.id;
            """
            )
            if len(rows) > 0:
                self.log.debug(f"removing some duplicates from {blob_names[i]}")
                conn.sql(
                    f"""
                DELETE FROM {merged_table} WHERE id IN (
                    SELECT m.id
                    FROM {merged_table} AS m
                    INNER JOIN {checkpoint_table} AS ch
                    ON m.id = ch.id
                );
                """
                )
            conn.sql(
                f"""
            INSERT INTO {merged_table}
            SELECT * FROM {checkpoint_table};
            """
            )
        conn.sql(
            f"""
        COPY {merged_table} TO '{self.full_dest_table_path(worker, batch_id)}';
        """
        )
        # Create a table to store the ids for this
        conn.sql(
            f"""
        CREATE OR REPLACE TABLE merged_ids_{worker}_{batch_id}
        AS
        SELECT id as "id" FROM {merged_table}
        """
        )

        if batch_id > 0:
            prev_batch_id = batch_id - 1
            # Check for any intersections with the last table. We need to create a "delete patch"
            conn.sql(
                f"""
            CREATE OR REPLACE TABLE delete_patch_{worker}_{prev_batch_id}
            AS
            SELECT pmi.id 
            FROM merged_ids_{worker}_{prev_batch_id} AS pmi
            INNER JOIN {merged_table} AS m
                ON m.id = pmi.id;
            """
            )
            conn.sql(
                f"""
            DROP TABLE merged_ids_{worker}_{prev_batch_id};
            """
            )
            conn.sql(
                f"""
            COPY delete_patch_{worker}_{prev_batch_id} TO '{self.full_dest_delete_path(worker, prev_batch_id)}';
            """
            )
            conn.sql(
                f"""
            DROP TABLE delete_patch_{worker}_{prev_batch_id};
            """
            )
        conn.sql(
            f"""
        DROP TABLE {merged_table};
        """
        )
        self.log.info(f"Completed load and merge {batch_id}")
        for checkpoint_temp_table in checkpoint_temp_tables:
            try:
                conn.sql(
                    f"""
                DROP TABLE {checkpoint_temp_table}
                """
                )
            except:
                # Ignore view dropping errors
                self.log.warn(
                    f"error occured dropping checkpoint temp table: {checkpoint_temp_table}"
                )


# def load_and_merge(
#     log: DagsterLogManager,
#     conn: duckdb.DuckDBPyConnection,
#     bucket_name: str,
#     blob_names: List[str],
#     batch_id: int,
#     key_id: str,
#     secret: str,
#     destination_path: str,
# ):
#     conn.sql(
#         f"""
#     CREATE SECRET (
#         TYPE GCS,
#         KEY_ID '{key_id}',
#         SECRET '{secret}'
#     );
#     """
#     )

#     base = f"gs://{bucket_name}/"

#     size = len(blob_names)

#     for i in range(size):
#         log.debug(f"Blob {blob_names[i]}")
#         file_ref = f"{base}/{blob_names[i]}"
#         conn.sql(
#             f"""
#         CREATE OR REPLACE VIEW checkpoint_{batch_id}_{i}
#         AS
#         SELECT *
#         FROM read_parquet('{file_ref}');
#         """
#         )

#     merged_table = f"merged_{batch_id}"
#     try:
#         conn.sql(
#             f"""
#         CREATE TABLE {merged_table}
#         AS
#         SELECT *
#         FROM checkpoint_{batch_id}_{i}
#         """
#         )
#     except duckdb.CatalogException:
#         conn.sql(f"DROP TABLE {merged_table};")
#         conn.sql(
#             f"""
#         CREATE TABLE {merged_table}
#         AS
#         SELECT *
#         FROM checkpoint_{batch_id}_0
#         """
#         )

#     for i in range(size - 1):
#         log.info(f"Merging {blob_names[i+1]}")
#         checkpoint_table = f"checkpoint_{batch_id}_{i+1}"
#         rows = conn.sql(
#             f"""
#         SELECT *
#         FROM {merged_table} AS m
#         INNER JOIN {checkpoint_table} as ch
#           ON m.id = ch.id;
#         """
#         )
#         if len(rows) > 0:
#             conn.sql(
#                 f"""
#             DELETE FROM {merged_table} WHERE id in (
#                 SELECT m.id
#                 FROM merged AS m
#                 INNER JOIN {checkpoint_table} AS ch
#                   ON m.id = ch.id
#             );
#             """
#             )
#         conn.sql(
#             f"""
#         INSERT INTO {merged_table}
#         SELECT * FROM {checkpoint_table};
#         """
#         )
#     conn.sql(
#         f"""
#     COPY {merged_table} TO 'gs://{bucket_name}/{destination_path}/table_{batch_id}.parquet';
#     """
#     )
#     # Create a table to store the ids for this
#     conn.sql(
#         f"""
#     CREATE TABLE OR REPLACE merged_ids_{batch_id}
#     AS
#     SELECT id as "id" FROM {merged_table}
#     """
#     )

#     if batch_id > 0:
#         prev_batch_id = batch_id - 1
#         # Check for any intersections with the last table. We need to create a "delete patch"
#         conn.sql(
#             f"""
#         CREATE TABLE OR REPLACE delete_patch_{prev_batch_id}
#         AS
#         SELECT pmi.id
#         FROM merged_ids_{prev_batch_id} AS pmi
#         INNER JOIN {merged_table} AS m
#             ON m.id = pmi.id;
#         """
#         )
#         conn.sql(
#             f"""
#         DROP TABLE merged_ids_{prev_batch_id};
#         """
#         )
#         conn.sql(
#             f"""
#         COPY delete_patch_{prev_batch_id} TO 'gs://{bucket_name}/{destination_path}/delete_{prev_batch_id}.parquet';
#         """
#         )
#         conn.sql(
#             f"""
#         DROP TABLE delete_patch_{prev_batch_id};
#         """
#         )
#     conn.sql(
#         f"""
#     DROP TABLE {merged_table};
#     """
#     )
