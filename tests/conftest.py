"""测试共用：在后台线程里起一个真实的 uvicorn，承载 mockkube。

盘点走真实 HTTP（KubernetesClient -> httpx -> 127.0.0.1），
这样分页、continue token、409/410 等行为与 Compose 演示完全一致。
"""

from __future__ import annotations

import socket
import threading
import time

import pytest
import uvicorn

from app import mockkube


@pytest.fixture(scope="session")
def mock_server() -> str:
    probe = socket.socket()
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]
    probe.close()

    config = uvicorn.Config(
        mockkube.app,
        host="127.0.0.1",
        port=port,
        log_level="warning",
        lifespan="on",
    )
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not server.started:
        if time.monotonic() > deadline:
            raise RuntimeError("mockkube server did not start")
        time.sleep(0.01)

    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        thread.join(timeout=5)
