from __future__ import annotations

from mangum import Mangum

from .app import app

if app is None:
    raise RuntimeError("control-plane Lambda app failed to initialize")

handler = Mangum(app)
