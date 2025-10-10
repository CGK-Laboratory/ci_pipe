from ci_pipe.ci_pipe_record import CIPipeRecord


class Result:
    def __init__(self, records):
        self._records = tuple(records)

    @classmethod
    def from_raw(cls, raw):
        return cls(CIPipeRecord.from_raw(item) for item in raw)

    def to_raw(self):
        return [{'ids': list(r.ids()), 'value': r.value()} for r in self._records]

    def is_empty(self) -> bool:
        return len(self._records) == 0

    def has_size_of(self, size_number) -> int:
        return len(self._records) == size_number

    def first(self):
        if self.is_empty():
            raise IndexError("Cannot get first value since the result is empty")
        return self._records[0]

    def last(self):
        if self.is_empty():
            raise IndexError("Cannot get last value since the result is empty")
        return self._records[-1]

    def values(self):
        return list(tuple(r.value() for r in self._records))

    def ids(self):
        return list(tuple(r.ids() for r in self._records))
