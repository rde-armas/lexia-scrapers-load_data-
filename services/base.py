# Inspired by Ruby gem `interactor`


from typing import Never


class Service:
    def __init__(self) -> None:
        self.result = Result()

    def run(self):
        try:
            self._execute()
        except Exception as e:
            self.result.add_error(str(e))
        return self.result

    def _execute(self) -> None:
        """Subclasses must implement this method and modify self.result."""
        raise NotImplementedError("Subclasses must implement this method")


class Result:
    def __init__(self, success=None, data=None, errors=None):
        self._success = success if success is not None else True
        self._data = data if data is not None else {}
        self._errors = errors if errors is not None else []

    def success(self):
        return self._success

    @property
    def data(self):
        return self._data

    def __getitem__(self, key):
        return self._data.get(key)

    def __setitem__(self, key, value):
        self._data[key] = value

    def __contains__(self, key):
        return key in self._data

    def failure(self):
        return not self._success

    def fail(self):
        self._success = False

    def errors(self):
        return self._errors

    def add_error(self, *errors):
        for error in errors:
            self._errors.append(error)
        self._success = False

    def __contains__(self, key):
        return key in self._data

    def failure(self):
        return not self._success

    def fail(self):
        self._success = False

    def errors(self):
        return self._errors

    def add_error(self, *errors):
        for error in errors:
            self._errors.append(error)

        self._success = False
