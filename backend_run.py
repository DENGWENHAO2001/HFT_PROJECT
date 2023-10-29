import subprocess

subprocess.Popen(["python", "run_celery.py"])
subprocess.Popen(["python", "backend.py"])
subprocess.Popen(["python", "websocket_consumer.py"])
subprocess.Popen(["python", "websocket_producer.py"])
