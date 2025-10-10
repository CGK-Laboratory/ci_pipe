class CIPipeRecord:
    @classmethod
    def from_raw(cls, raw):
        ids = raw["ids"]
        value = raw["value"]
        return cls(ids, value)

    def __init__(self, ids, value):
        self._ids = tuple(ids)
        self._value = value

    def ids(self):
        return self._ids

    def value(self):
        return self._value

    def to_raw(self):
        return {"ids": list(self._ids), "value": self._value}
