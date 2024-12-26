from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent


def init_logging() -> None:
    import yaml
    import logging
    from logging import config as logging_config

    LOGGING_CONFIG_YAML_FILENAME = 'logging_config.yaml'
    LOGGIN_CONFIG_YAML_FILE_PATH = BASE_DIR / 'settings' / LOGGING_CONFIG_YAML_FILENAME

    with open(LOGGIN_CONFIG_YAML_FILE_PATH, 'r') as file:
        config = yaml.safe_load(file.read())

    logging_config.dictConfig(config)

    logger = logging.getLogger('settings')
    logger.debug('%s was used to configure logging', LOGGING_CONFIG_YAML_FILENAME)
