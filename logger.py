import logging


def use_date_time_logger():
    logging.basicConfig(filemode="w", filename="run.log",
                        format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)


def info(msg: str):
    logging.info(msg)


def warning(msg: str):
    logging.warning(msg)


def error(msg: str):
    logging.error(msg)


def debug(msg: str):
    logging.debug(msg)
