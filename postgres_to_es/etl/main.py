from contextlib import closing
from time import sleep

import psycopg
from psycopg import ClientCursor
from psycopg.rows import class_row

from src.log import log
from src.extract import PostgresExtractor
from src.load import ElasticsearchLoader
from src.schemas import Movie
from src.storage import LocalJsonStorage, StateManager
from settings import elastic_settings, postgres_settings

database_settings = postgres_settings.get_dsl()
elasticsearch_url = elastic_settings.get_url()


def update_index():
    """Обновляет индекс в Elasticsearch на основе данных из PostgreSQL."""
    with closing(psycopg.connect(**database_settings, row_factory=class_row(Movie),
                                 cursor_factory=ClientCursor)) as pg_connection:

        state_manager = StateManager(LocalJsonStorage('state_storage.json'))
        data_extractor = PostgresExtractor(pg_connection)
        data_loader = ElasticsearchLoader(elasticsearch_url)

        for record in data_extractor.get_data(state_manager.get('last_sync_date')):
            records_to_load, modified_dates = [], []
            for entry in record:
                movie_data = entry.model_dump()
                modified_dates.append(movie_data.pop('last_modified_date'))
                records_to_load.append(movie_data)

            data_loader.load_data(records_to_load)
            latest_modified_date = max(modified_dates)
            state_manager.set('last_sync_date', latest_modified_date.isoformat())


def main() -> None:
    update_index()


if __name__ == '__main__':
    while True:
        try:
            main()
        except Exception as error:
            log.error(error)
        finally:
            sleep(60)
