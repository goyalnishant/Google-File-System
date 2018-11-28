import ast

from commons.loggers import default_logger

log = default_logger

SEPARATOR = "|||"


class OplogActions:
    WRITE_CHUNK, DELETE_CHUNK = range(2)


def update_metadata(filename, action, data):
    # TODO:
    #  - Maintain meta data as an in memory object
    #  - Find a way to dump entire meta data object
    with open(filename, mode="a") as fp:
        fp.write(f"{action}{SEPARATOR}{data}\n")


def parse_metadata(cs, fp):
    # TODO: simplify it's too complicated and hard coded
    log.debug("****Beginning oplog replay****")

    for line in fp:
        line = line.strip()
        key, value = line.split(SEPARATOR)
        key = int(key)

        if key == OplogActions.WRITE_CHUNK:
            log.debug("Replaying write chunk")

            chunk_handle, chunk_index, length, path = ast.literal_eval(value)

        elif key == OplogActions.DELETE_CHUNK:
            log.debug("Replaying delete chunk")

        else:
            log.error('Invalid master meta data key: %s with value: %s', key, value)


def load_metadata(cs):
    try:
        with open(cs.metadata_file) as fp:
            parse_metadata(cs, fp)
        log.debug("****oplog replay completed****")
    except FileNotFoundError:
        log.error("Can't open meta data file: %s", cs.metadata_file)
