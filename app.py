"""
Websocket and Appscheduler test
"""
import logging
import sys
import uuid
import eventlet
from flask import Flask, render_template
from flask.logging import create_logger
from rdwebsocket import socketio
from rdsched import aps_scheduler


# for background scheduler
eventlet.monkey_patch(thread=True, time=True)

# setup logging handler
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

# init app
app = Flask(__name__)
app.config['SECRET_KEY'] = str(uuid.uuid4())
logger = create_logger(app)
logger.handlers.clear()
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

# init websockets
socketio.init_app(app)

# init scheduler
aps_scheduler.init_app(app)
aps_scheduler.start()


@app.route('/')
def index():
    ''' Load index page '''
    logger.info("load index.html....")
    return render_template('index.html', async_mode=socketio.async_mode)

if __name__ == '__main__':
    socketio.run(app)
