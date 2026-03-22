import multiprocessing

# Bind
bind = "0.0.0.0:8080"

# WICHTIG: NUR 1 Worker für Background-Jobs!
workers = 1
worker_class = "gthread"
threads = 2

# Timeout
timeout = 120
keepalive = 5

# Logging
accesslog = "-"
errorlog = "-"
loglevel = "info"
