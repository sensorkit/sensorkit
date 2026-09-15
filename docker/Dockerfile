# --- Deps stage: resolve and install dependencies (no project) into /usr/local ---
FROM python:3.13-slim AS deps

ARG EXTRAS=alpaca,indigo,nina,node-platform,otto,pwi4,slack,thesky,udl

# git: required at build time for hatch-vcs to resolve the version.
# build-essential: some deps ship no linux/arm64 wheel and must compile from
# source (e.g. photutils, pulled in by the `senpai` extra — it has cp313 wheels
# for linux-x86_64/macOS/Windows but none for linux-aarch64). This is the builder
# stage only; the final image copies just /usr/local, so no toolchain is shipped.
RUN apt-get update && apt-get install -y git build-essential && rm -rf /var/lib/apt/lists/*
COPY --from=ghcr.io/astral-sh/uv:0.11 /uv /uvx /bin/

WORKDIR /opt/sensorkit-build

# `uv sync` installs into a project environment, not the system interpreter.
# UV_PROJECT_ENVIRONMENT redirects that environment at the interpreter prefix so
# dependencies land in /usr/local. This stage is keyed only on the dependency
# metadata, so it caches across source-only changes.
ENV UV_PROJECT_ENVIRONMENT=/usr/local

COPY pyproject.toml hatch_build.py uv.lock README.md ./
# --locked (not --frozen): install exactly the lock, and fail the build if it has
# drifted from pyproject.toml — so a forgotten `uv lock` is caught in CI.
RUN uv sync --locked --no-install-project --no-dev --extra $EXTRAS

# --- Project stage: build + install only sensorkit into a clean /staging tree ---
FROM deps AS builder

# Only the sources the wheel build needs: the package (core), the force-included
# module trees (modules), and .git for hatch-vcs version resolution. pyproject.toml,
# hatch_build.py, README.md and uv.lock are already present from the deps stage.
# Update this list if the build ever needs additional paths.
COPY .git ./.git
COPY core ./core
COPY modules ./modules

# When supplied, this overrides hatch-vcs version detection, so the version is
# correct even though the scoped copy above leaves git's working tree "dirty"
# (un-copied tracked files look deleted). CI computes the real commit-distance
# version on a full-history checkout and passes it in. When empty, hatch-vcs
# falls back to the copied .git. Scoped to this stage so it can't affect any
# from-source dependency build (those run in the deps stage).
ARG SETUPTOOLS_SCM_PRETEND_VERSION
ENV SETUPTOOLS_SCM_PRETEND_VERSION=${SETUPTOOLS_SCM_PRETEND_VERSION}

# --no-deps: deps are already in /usr/local, so this installs only the project.
# --prefix /staging: isolates the ~2MB project tree (site-packages/sensorkit,
# its dist-info, bin/sensorkit) so it can be copied as its own thin layer that
# re-exports on code changes without dragging the dependency bulk along.
# A regular (non-editable) install builds a wheel, so the hatch_build.py hook
# force-includes the modules/*/src trees; .git is present for hatch-vcs.
RUN uv pip install --no-deps --prefix /staging .

# --- Final stage: just the interpreter prefix, no git/uv/source ---
FROM python:3.13-slim

# Deps first (large, stable across code changes), then the project (small, volatile).
COPY --from=deps    /usr/local /usr/local
COPY --from=builder /staging   /usr/local

RUN useradd --create-home --uid 1000 sensorkit \
    && mkdir -p /opt/sk/data \
    && chown sensorkit:sensorkit /opt/sk/data

USER sensorkit
WORKDIR /home/sensorkit

ENTRYPOINT ["sensorkit"]
