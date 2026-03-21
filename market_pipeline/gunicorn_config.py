import multiprocessing

# Bind
bind = "0.0.0.0:8080"

# Workers
workers = 2
worker_class = "gthread"
threads = 4

# Timeout
timeout = 120
keepalive = 5

# Logging
accesslog = "-"
errorlog = "-"
loglevel = "info"
