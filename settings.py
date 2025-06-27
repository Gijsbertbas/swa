from pydantic_settings import BaseSettings


class SupabaseSettings(BaseSettings):
    """
    Settings for Supabase connection.
    """
    url: str
    pwd: str

    class Config:
        env_prefix = 'SUPABASE_'
        env_file = '.env'
        case_sensitive = False
        extra = 'ignore'
