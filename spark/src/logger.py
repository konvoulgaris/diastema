import logging

logging.basicConfig(format="%(asctime)s %(levelname)s [%(filename)s:%(lineno)s - %(funcName)s]: %(message)s", datefmt="%y/%m/%d %H:%M:%S")
LOGGER = logging.getLogger(__name__)
