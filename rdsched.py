"""
Scheduler
"""
import logging
from flask_apscheduler import APScheduler
from rdwebsocket import socketio, websocket_counter

logger = logging.getLogger("app.scheduler")
aps_scheduler = APScheduler()

@aps_scheduler.task('interval', id='job_1', seconds=5, misfire_grace_time=900)
def job1():
    ''' broadcast job execution '''
    with aps_scheduler.app.app_context():
        logger.info('Job 1 executed %s', len(websocket_counter.user_dict) )
        if len(websocket_counter.user_dict) > 0:
            logger.info('Active connections so poll')
            socketio.emit('my_response', {'data': 'Job1 Triggered', 'count': 0}, broadcast=True)
