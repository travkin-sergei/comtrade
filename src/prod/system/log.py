import os
import logging

address_folder_full = os.path.dirname(os.path.abspath(__file__))
address = address_folder_full.split('\\')
for _ in range(3):
    del address[-1]

address_log = '/'.join(address)


def logConnect():
    logging.basicConfig(
        filename=f"{address_log}/logger.log"
        , format='{"asctime":"%(asctime)s","msecs":"%(msecs)d","name": "%(name)s","levelname":"%(levelname)s","message":"%(message)s"}'
        , level=logging.INFO
    )
    result = logging.getLogger()
    return result
