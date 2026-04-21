FROM public.ecr.aws/lambda/python:3.12

WORKDIR /var/task/control_plane_build

COPY control_plane/pyproject.toml ./pyproject.toml
COPY control_plane/uv.lock ./uv.lock

RUN pip install --no-cache-dir uv \
    && uv export --frozen --no-dev --output-file requirements-lambda.txt \
    && pip install --no-cache-dir --target /var/task -r requirements-lambda.txt

COPY control_plane/control_plane /var/task/control_plane

WORKDIR /var/task

CMD ["control_plane.lambda_handler.handler"]
