class CIPipeError(Exception):
    def __init__(self, message, context=None):
        self._message = message
        self.context = context or {}
        super().__init__(self._message)

    def to_dict(self):
        return {"message": str(self._message), "context": self.context}