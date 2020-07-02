import logging

from .client import TriggerflowClient as Triggerflow
from .client import TriggerflowCachedClient
from .client import DefaultActions, DefaultConditions
from .libs.cloudevents.sdk.event.v1 import Event as CloudEvent

logger = logging.getLogger('triggerflow')
console_handler = logging.StreamHandler()

formatter = logging.Formatter('[%(asctime)s][%(name)s][%(levelname)s] %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

