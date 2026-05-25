"""
VeltrixDB Python client — binary wire protocol with thread-safe connection pool.
Embedded from the official VeltrixDB Python SDK for zero-dependency benchmarking.

Binary wire protocol:
  Request:  [1B cmd][2B keyLen LE][4B valLen LE][key][value]
  Response: [1B status][4B payloadLen LE][payload]

  cmd:    0x01=PUT  0x02=GET  0x03=DEL  0x04=PING  0x05=INFO
          0x06=MPUT (batch)  0x07=MGET (batch)  0x09=AUTH
  status: 0x00=OK  0x01=ERR  0x02=NOT_FOUND
"""

import socket
import ssl
import struct
import queue
from contextlib import contextmanager
from typing import List, Optional, Sequence, Tuple

_PUT    = 0x01
_GET    = 0x02
_DEL    = 0x03
_PING   = 0x04
_INFO   = 0x05
_MPUT   = 0x06
_MGET   = 0x07
_AUTH   = 0x09

_OK        = 0x00
_ERR       = 0x01
_NOT_FOUND = 0x02


class VeltrixDBError(Exception):
    pass


class VeltrixDBClient:
    """Single, non-thread-safe TCP connection using the binary wire protocol."""

    def __init__(
        self,
        host: str = '127.0.0.1',
        port: int = 9000,
        timeout: float = 10.0,
        connect_timeout: float = 5.0,
        ssl_context: Optional[ssl.SSLContext] = None,
    ):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.settimeout(connect_timeout)
        sock.connect((host, port))
        sock.settimeout(timeout)
        if ssl_context is not None:
            sock = ssl_context.wrap_socket(sock, server_hostname=host)
        self._s = sock

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def auth(self, username: str, password: str) -> None:
        self._cmd(_AUTH, username, password.encode('utf-8'))
        self._resp()

    def put(self, key: str, value: bytes) -> None:
        self._cmd(_PUT, key, value)
        self._resp()

    def get(self, key: str) -> Optional[bytes]:
        self._cmd(_GET, key, b'')
        return self._resp()

    def delete(self, key: str) -> None:
        self._cmd(_DEL, key, b'')
        self._resp()

    def ping(self) -> None:
        self._cmd(_PING, '', b'')
        self._resp()

    def info(self) -> str:
        self._cmd(_INFO, '', b'')
        payload = self._resp()
        return payload.decode('utf-8') if payload else ''

    def close(self) -> None:
        try:
            self._s.close()
        except OSError:
            pass

    def multi_put(
        self,
        entries: Sequence[Tuple[str, bytes]],
        ttls: Optional[Sequence[int]] = None,
    ) -> List[Optional[Exception]]:
        n = len(entries)
        if n == 0:
            return []
        if ttls is None:
            ttls = [-1] * n

        buf = bytearray(struct.pack('<BHI', _MPUT, 0, n))
        for (key, value), ttl in zip(entries, ttls):
            kb = key.encode('utf-8')
            ttl = -1 if ttl == 0 else int(ttl)
            buf += struct.pack('<HIi', len(kb), len(value), ttl)
            buf += kb
            buf += value
        self._s.sendall(bytes(buf))

        hdr = self._recv(5)
        if hdr[0] != _OK:
            raise VeltrixDBError('multiPut batch-level error')
        count = struct.unpack_from('<I', hdr, 1)[0]
        statuses = self._recv(count)
        return [
            None if s == _OK
            else VeltrixDBError(f'entry failed (status=0x{s:02x})')
            for s in statuses
        ]

    def multi_get(self, keys: Sequence[str]) -> List[Optional[bytes]]:
        n = len(keys)
        if n == 0:
            return []

        buf = bytearray(struct.pack('<BHI', _MGET, 0, n))
        for key in keys:
            kb = key.encode('utf-8')
            buf += struct.pack('<H', len(kb))
            buf += kb
        self._s.sendall(bytes(buf))

        hdr = self._recv(5)
        if hdr[0] != _OK:
            raise VeltrixDBError('multiGet batch-level error')
        count = struct.unpack_from('<I', hdr, 1)[0]

        results: List[Optional[bytes]] = []
        for _ in range(count):
            ent = self._recv(5)
            ent_status = ent[0]
            val_len = struct.unpack_from('<I', ent, 1)[0]
            if ent_status == _OK and val_len > 0:
                results.append(self._recv(val_len))
            else:
                results.append(None)
        return results

    def _cmd(self, cmd: int, key: str, value: bytes) -> None:
        kb = key.encode('utf-8')
        hdr = struct.pack('<BHI', cmd, len(kb), len(value))
        self._s.sendall(hdr + kb + value)

    def _resp(self) -> Optional[bytes]:
        hdr = self._recv(5)
        status = hdr[0]
        payload_len = struct.unpack_from('<I', hdr, 1)[0]
        payload = self._recv(payload_len) if payload_len > 0 else b''
        if status == _OK:
            return payload or None
        if status == _NOT_FOUND:
            return None
        raise VeltrixDBError(
            payload.decode('utf-8', errors='replace') if payload else 'server error'
        )

    def _recv(self, n: int) -> bytes:
        chunks = []
        received = 0
        while received < n:
            chunk = self._s.recv(n - received)
            if not chunk:
                raise VeltrixDBError('connection closed by server')
            chunks.append(chunk)
            received += len(chunk)
        return b''.join(chunks)


class VeltrixDBPool:
    """Thread-safe fixed-size connection pool."""

    def __init__(
        self,
        host: str = '127.0.0.1',
        port: int = 9000,
        pool_size: int = 8,
        timeout: float = 10.0,
        connect_timeout: float = 5.0,
        acquire_timeout: float = 5.0,
        ssl_context: Optional[ssl.SSLContext] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ):
        self._host = host
        self._port = port
        self._timeout = timeout
        self._connect_timeout = connect_timeout
        self._acquire_timeout = acquire_timeout
        self._ssl = ssl_context
        self._username = username
        self._password = password
        self._pool: queue.Queue = queue.Queue(maxsize=pool_size)
        for _ in range(pool_size):
            self._pool.put(self._new_conn())

    @contextmanager
    def acquire(self):
        try:
            conn = self._pool.get(timeout=self._acquire_timeout)
        except queue.Empty:
            raise VeltrixDBError(
                f'VeltrixDBPool exhausted (pool_size={self._pool.maxsize})'
            )
        try:
            yield conn
            self._pool.put(conn)
        except Exception:
            conn.close()
            try:
                self._pool.put_nowait(self._new_conn())
            except (queue.Full, Exception):
                pass
            raise

    def execute(self, fn):
        with self.acquire() as c:
            return fn(c)

    def close(self) -> None:
        while True:
            try:
                c = self._pool.get_nowait()
                c.close()
            except queue.Empty:
                break

    def _new_conn(self) -> VeltrixDBClient:
        c = VeltrixDBClient(
            self._host, self._port,
            self._timeout, self._connect_timeout, self._ssl,
        )
        if self._username:
            c.auth(self._username, self._password or '')
        return c
