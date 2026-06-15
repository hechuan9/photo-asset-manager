FROM python:3.12-slim

WORKDIR /app/control_plane_build

COPY control_plane/pyproject.toml ./pyproject.toml
COPY control_plane/uv.lock ./uv.lock

RUN pip install --no-cache-dir uv \
    && uv export --frozen --no-dev --output-file requirements.txt \
    && pip install --no-cache-dir -r requirements.txt

COPY control_plane/control_plane /app/control_plane

WORKDIR /app

ENV PYTHONPATH=/app

CMD ["uvicorn", "control_plane.app:app", "--host", "0.0.0.0", "--port", "2283"]
