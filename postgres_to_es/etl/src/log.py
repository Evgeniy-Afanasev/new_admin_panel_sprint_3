import logging
from logging.handlers import RotatingFileHandler

log = logging.getLogger('my_logger')
log.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)

fh = RotatingFileHandler('output.log', maxBytes=256*1024, backupCount=3, encoding='utf-8')
fh.setLevel(logging.INFO)

fmt = logging.Formatter('%(asctime)s [%(levelname)s]: %(message)s')
ch.setFormatter(fmt)
fh.setFormatter(fmt)

log.addHandler(ch)
log.addHandler(fh)