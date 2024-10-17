# library users don't see log outputs unless requested, see
# https://docs.python.org/3/howto/logging.html#configuring-logging-for-a-library
import logging
logging.getLogger('sam_globus_keepup').addHandler(logging.NullHandler())


import ifdh
import samweb_client

IFDH_Client = ifdh.ifdh()
SAMWeb_Client = samweb_client.SAMWebClient()


import os
try:
    EXPERIMENT = os.environ["EXPERIMENT"]
except KeyError:
    raise RuntimeError("Must set EXPERIMENT environment variable.")
