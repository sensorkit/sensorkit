FROM python:3.13

RUN pip install uv

COPY pyproject.toml hatch_build.py uv.lock README.md /opt/sensorkit/
COPY core/ /opt/sensorkit/core/
COPY modules/ /opt/sensorkit/modules/
COPY .git/ /opt/sensorkit/.git/

WORKDIR /opt/sensorkit
RUN uv pip install --system .[ascom,pwi4,thesky,otto,filesys]
RUN rm -rf .git/

ENTRYPOINT ["sensorkit"]
