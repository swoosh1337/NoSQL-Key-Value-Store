import socket
import logging 


logging.basicConfig(level=logging.INFO)

HOST = 'localhost'
PORT = 7777
SOCKET = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
STATS = {
    'PUT': {'success': 0, 'error': 0},
    'GET': {'success': 0, 'error': 0},
    'GETLIST': {'success': 0, 'error': 0},
    'PUTLIST': {'success': 0, 'error': 0},
    'INCREMENT': {'success': 0, 'error': 0},
    'APPEND': {'success': 0, 'error': 0},
    'DELETE': {'success': 0, 'error': 0},
    'STATS': {'success': 0, 'error': 0},
    }
DATA = {}

def parse_message(data):
    try:
        parts = data.strip().split(';')
        command, key = parts[0], parts[1]
        value = parts[2] if len(parts) > 2 else None
        value_type = parts[3] if len(parts) > 3 else None

        if value_type == 'LIST':
            value = value.split(',')
        elif value_type == 'INT':
            value = int(value)
        else:
            value = str(value) if value else None

        return command, key, value
    except Exception as e:
        logging.error(f"Error parsing message: {e}")
        return None, None, None

def update_stats(command, success):
    """Update the STATS dict with info about if executing
    *command* was a *success*."""
    if success:
        STATS[command]['success'] += 1
    else:
        STATS[command]['error'] += 1


def handle_put(key, value):
    """Return a tuple containing True and the message
    to send back to the client."""
    DATA[key] = value
    return (True, 'Key [{}] set to [{}]'.format(key, value))


def handle_get(key):
    """Return a tuple containing True if the key exists and the message
    to send back to the client."""
    if key not in DATA:
        return(False, 'ERROR: Key [{}] not found'.format(key))
    else:
        return(True, DATA[key])


def handle_putlist(key, value):
    """Return a tuple containing True if the command succeeded and the message
    to send back to the client."""
    return handle_put(key, value)


def handle_getlist(key):
    """Return a tuple containing True if the key contained a list and
    the message to send back to the client."""
    return_value = exists, value = handle_get(key)
    if not exists:
        return return_value
    elif not isinstance(value, list):
        return (
            False,
            'ERROR: Key [{}] contains non-list value ([{}])'.format(key, value)
            )
    else:
        return return_value


def handle_increment(key):
    """Return a tuple containing True if the key's value could be incremented
    and the message to send back to the client."""
    return_value = exists, value = handle_get(key)
    if not exists:
        return return_value
    elif not isinstance(value, int):
        return (
            False,
            'ERROR: Key [{}] contains non-int value ([{}])'.format(key, value)
            )
    else:
        DATA[key] = value + 1
        return (True, 'Key [{}] incremented'.format(key))


def handle_append(key, value):
    """Return a tuple containing True if the key's value could be appended to
    and the message to send back to the client."""
    return_value = exists, list_value = handle_get(key)
    if not exists:
        return return_value
    elif not isinstance(list_value, list):
        return (
            False,
            'ERROR: Key [{}] contains non-list value ([{}])'.format(key, value)
            )
    else:
        DATA[key].append(value)
        return (True, 'Key [{}] had value [{}] appended'.format(key, value))


def handle_delete(key):
    """Return a tuple containing True if the key could be deleted and
    the message to send back to the client."""
    if key not in DATA:
        return (
            False,
            'ERROR: Key [{}] not found and could not be deleted'.format(key)
            )
    else:
        del DATA[key]


def handle_stats():
    """Return a tuple containing True and the contents of the STATS dict."""
    return (True, str(STATS))


COMMAND_HANDLERS = {
    'PUT': handle_put,
    'GET': handle_get,
    'GETLIST': handle_getlist,
    'PUTLIST': handle_putlist,
    'INCREMENT': handle_increment,
    'APPEND': handle_append,
    'DELETE': handle_delete,
    'STATS': handle_stats,
    }



def main():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.bind((HOST, PORT))
        server_socket.listen(1)
        logging.info(f"Server started on {HOST}:{PORT}")

        while True:
            conn, addr = server_socket.accept()
            with conn:
                logging.info(f'New connection from {addr}')
                try:
                    data = conn.recv(4096).decode()
                    command, key, value = parse_message(data)
                    if not command:
                        raise ValueError("Invalid command format")
                    response = COMMAND_HANDLERS.get(command, lambda k, v: (False, "Unknown command"))(key, value)
                    update_stats(command, response[0])
                    conn.sendall('{};{}'.format(response[0], response[1]).encode())
                except Exception as e:
                    logging.error(f"Error handling request: {e}")




if __name__ == '__main__':
    main()