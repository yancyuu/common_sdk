class MyBytes:
    def __init__(self, data):
        self.data = data

    def iter_content(self, chunk_size=30):
        for i in range(0, len(self.data), chunk_size):
            yield self.data[i:i + chunk_size]
            
    def get_complete_text(self):
        return self.data.decode('utf-8') if isinstance(self.data, bytes) else self.data
