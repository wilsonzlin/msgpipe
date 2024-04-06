from io import BufferedIOBase
from typing import Callable
import msgpack


def _ensure_read(rd: BufferedIOBase, n: int) -> bytes:
    b = bytes()
    while len(b) < n:
        b += rd.read(n - len(b))
    assert len(b) == n
    return b



class PyIpc:
    def __init__(self):
        self.ipc_recv = open(3, "rb")
        self.ipc_send = open(4, "wb")
        self.handlers = dict()

    def begin_loop(self):
        # Send ready signal.
        self.ipc_send.write(b"\xFD")
        self.ipc_send.flush()

        while True:
            input_raw_len = int.from_bytes(
                _ensure_read(self.ipc_recv, 4), byteorder="little", signed=False
            )
            input_raw = msgpack.unpackb(_ensure_read(self.ipc_recv, input_raw_len))
            assert type(input_raw) is dict
            typ = input_raw.pop("$type")
            cls, fn = self.handlers[typ]
            req = cls(**input_raw)
            res = fn(req)
            res_raw = msgpack.packb(res)
            assert type(res_raw) is bytes
            self.ipc_send.write(
                len(res_raw).to_bytes(4, byteorder="little", signed=False)
            )
            self.ipc_send.write(res_raw)
            self.ipc_send.flush()

    def add_handler(self, typ: str, cls: type, handler: Callable) -> "PyIpc":
        self.handlers[typ] = (cls, handler)
        return self
