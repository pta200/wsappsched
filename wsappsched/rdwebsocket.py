"""
Sockets
"""
import logging
import functools
import threading
from flask import session, request, copy_current_request_context
from flask_socketio import SocketIO, emit, disconnect


# websocket
socketio = SocketIO(cors_allowed_origins='*')
# logger
logger = logging.getLogger("app.socket")

#class Counter():
#    ''' connection counter class '''
#    def __init__(self, start = 0):
#        self.lock = threading.Lock()
#        self.value = start
#    def increment(self):
#        ''' increment '''
#        with self.lock:
#            self.value = self.value + 1
#
#    def decrement(self):
#        ''' decrement '''
#        with self.lock:
#            if self.value > 0:
#                self.value = self.value - 1

class Counter():
    ''' connection counter class '''
    def __init__(self):
        self.user_dict = {}
    
    def increment(self, sid):
        ''' increment '''
        self.user_dict[sid] = 1

    def decrement(self, sid):
        ''' decrement '''
        if self.user_dict.get(sid) is not None:
            logger.info("%s found and removed", sid)
            self.user_dict.pop(sid)

websocket_counter = Counter()


def authenticated_only(func):
    ''' confirm auth and disconnect as necessary'''
    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        if request.headers.get('Authorization') is None or request.headers.get('Authorization').split()[1] != "daveisgreat":
            logger.info("NOT AUTHORIZED.... DISCONNECT")
            disconnect()
        else:
            logger.info("GOT HEADER %s", request.headers.get('Authorization'))
            return func(*args, **kwargs)
    return wrapped


@socketio.event
@authenticated_only
def my_event(message):
    ''' my event handler'''
    session['receive_count'] = session.get('receive_count', 0) + 1
    logger.info("GOT A CLIENT MESSAGE SEND RESPONSE")
    emit('my_response',
         {'data': message['data'], 'count': session['receive_count']})

@socketio.event
@authenticated_only
def disconnect_request():
    ''' disconect events '''
    @copy_current_request_context
    def can_disconnect():
        disconnect()

    logger.info("GOT A CLIENT DISCONNECT")
    session['receive_count'] = session.get('receive_count', 0) + 1
    # for this emit we use a callback function
    # when the callback function is invoked we know that the message has been
    # received and it is safe to disconnect
    emit('my_response',
         {'data': 'Disconnected!', 'count': session['receive_count']},
         callback=can_disconnect)


@socketio.on('disconnect')
@authenticated_only
def client_disconnect():
    ''' disconnect method '''
    logger.info("DECREMENT COUNTER")
    websocket_counter.decrement(request.sid)
    logger.info("CLIENT DISCONNECTED %s", request.sid)
    

@socketio.event
@authenticated_only
def connect():
    ''' connect event '''
    logger.info("GOT A CLIENT CONNECT %s", request.sid)
    websocket_counter.increment(request.sid)
    emit('my_response', {'data': 'Connected', 'count': 0})


@socketio.on_error()
def error_handler(error):
    ''' Handles the default namespace '''
    logger.error("ERROR...... %s", error)