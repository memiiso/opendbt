import logging
import sys


class OpenDbtLogger:
    _log = None

    @property
    def log(self) -> logging.Logger:
        if self._log is None:
            self._log = logging.getLogger(name="opendbt")
            if not self._log.hasHandlers():
                handler = logging.StreamHandler(sys.stdout)
                formatter = logging.Formatter("[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s")
                handler.setFormatter(formatter)
                handler.setLevel(logging.INFO)
                self._log.addHandler(handler)
        return self._log