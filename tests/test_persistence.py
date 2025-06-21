import os
import shutil
import tempfile
from msg import persistence

def setup_module(module):
    # Use a temp dir for tests
    module.test_dir = tempfile.mkdtemp()
    os.environ['msg_DATA_DIR'] = module.test_dir
    persistence.DATA_DIR = module.test_dir
    os.makedirs(module.test_dir, exist_ok=True)

def teardown_module(module):
    shutil.rmtree(module.test_dir)

def test_save_and_load_pubsub_messages():
    topic = 'testtopic'
    messages = [("msg1", "id1"), ("msg2", "id2")]
    persistence.save_messages(topic, messages, mode='pubsub')
    loaded = [tuple(msg) for msg in persistence.load_messages(topic, mode='pubsub')]
    assert loaded == messages


def test_save_and_load_queue_messages():
    topic = 'testqueue'
    messages = [("qmsg1", "qid1"), ("qmsg2", "qid2")]
    persistence.save_messages(topic, messages, mode='queue')
    loaded = [tuple(msg) for msg in persistence.load_messages(topic, mode='queue')]
    assert loaded == messages

def test_delete_messages():
    topic = 'todelete'
    messages = [("msg", "id")] 
    persistence.save_messages(topic, messages, mode='pubsub')
    assert os.path.exists(persistence._topic_file(topic, 'pubsub'))
    persistence.delete_messages(topic, mode='pubsub')
    assert not os.path.exists(persistence._topic_file(topic, 'pubsub'))
