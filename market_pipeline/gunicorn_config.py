import multiprocessing

# Netzwerk-Bindung
bind = "0.0.0.0:8080"

# WICHTIG: NUR 1 Worker für Background-Jobs!
# Verhindert, dass die Pipeline-Schleife doppelt gestartet wird.
workers = 1

# Gthreads erlauben es Flask, Anfragen zu bearbeiten, 
# während der Hintergrund-Thread der Pipeline läuft.
worker_class = "gthread"
threads = 4

# Timeout erhöht auf 120s, falls DB-Abfragen länger dauern
timeout = 120
keepalive = 5

# Logging
accesslog = "-"
errorlog = "-"
loglevel = "info"
