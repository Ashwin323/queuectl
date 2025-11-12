import database

DEFAULT_CONFIG = {
    "max_retries": 3,
    "backoff_base": 2
}


def init_config():
    """
    Initialize default config if not set.
    """
    for key, value in DEFAULT_CONFIG.items():
        if database.get_config(key) is None:
            database.set_config(key, value)


def set_config(key, value):
    """
    Set a configuration value.
    """
    database.set_config(key, value)


def get_config(key):
    """
    Get a configuration value.
    """
    value = database.get_config(key)
    if value is None and key in DEFAULT_CONFIG:
        return DEFAULT_CONFIG[key]
    return value

def list_config():
    """
    Return all configuration values.
    """
    config = {}
    for key in DEFAULT_CONFIG.keys():
        config[key] = get_config(key)
    return config

