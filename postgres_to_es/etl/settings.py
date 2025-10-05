from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class PostgresSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix='postgres_')
    host: str = Field(..., alias='SQL_HOST')
    port: int = Field(..., alias='SQL_PORT')
    dbname: str = Field(..., alias='POSTGRES_DB')
    user: str = Field(..., alias='POSTGRES_USER')
    password: str = Field(..., alias='POSTGRES_PASSWORD')

    def get_dsl(self):
        return self.model_dump()


class ElasticSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix='es_')
    host: str = Field(..., alias='ELASTIC_HOST')
    port: int = Field(..., alias='ELASTIC_PORT')

    def get_url(self):
        return f'http://{self.host}:{self.port}'


postgres_settings = PostgresSettings()
elastic_settings = ElasticSettings()
