import os
import logging

NODE_QUANTITY = int(os.environ.get('REPLICAS', 1))
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO').upper()
