from datetime import datetime

timestamp = 1699282337
dt_object = datetime.utcfromtimestamp(timestamp)
print(dt_object.strftime('%Y-%m-%d %H:%M:%S'))