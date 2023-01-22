from collections import namedtuple
from typing import Iterator, Sequence

ResponseProto = namedtuple("Response", "success message data")


class Response(ResponseProto):
    def __new__(
        cls, success: bool, message: str = None, data: dict = None
    ) -> ResponseProto:
        if not isinstance(success, bool):
            raise TypeError("status must be of type bool")
        # accept int or float for field2 and convert int to float
        # if message is not None and not isinstance(message, str):
        #     raise TypeError("Message must be of type str")
        # if data is not None and not isinstance(data, (list, dict, Iterator)):
        #     raise TypeError(f"Message must be of type dict, provided: {type(data)}")
        return ResponseProto.__new__(cls, success, message, data)