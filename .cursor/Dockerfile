FROM rust:bookworm

# Install uv from Astral's official distribution image.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

RUN uv pip install --break-system-packages pre-commit

# Install baseline CLI dependencies used by the repo Makefile and Docker dependencies.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        apt-transport-https \
        ca-certificates \
        curl \
        gnupg \
        lsb-release \
        make \
        python3 \
        python3-pip \
        python3-venv \
        git \
    && rm -rf /var/lib/apt/lists/*

# Install Docker official repository and tools (Ubuntu-based steps, adjusted for Debian when possible)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        apt-transport-https \
        ca-certificates \
        curl \
        gnupg \
        lsb-release \
    && curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" \
        > /etc/apt/sources.list.d/docker.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        docker-ce \
        docker-ce-cli \
        containerd.io \
        docker-compose-plugin \
        docker-compose \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd -f docker

RUN rustup toolchain install 1.91.0 --component rustfmt --component clippy \
    && rustup default 1.91.0

RUN usermod -g docker -G sudo,ubuntu ubuntu && \
    echo "ubuntu ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers

WORKDIR /workspace

USER ubuntu

VOLUME /var/lib/docker

SHELL ["/bin/bash", "-o", "pipefail", "-c"]