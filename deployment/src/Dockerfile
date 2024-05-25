# Leveraging the pre-built Docker images with 
# cargo-chef and the Rust toolchain
FROM python:slim AS chef
RUN apt-get update
RUN apt-get update
RUN apt-get install -y curl build-essential make software-properties-common wget python3-dev pkg-config libssl-dev clang
# RUN bash -c "$(wget -O - https://apt.llvm.org/llvm.sh)"
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | bash -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"
WORKDIR /app
RUN cargo install cargo-chef --version 0.1.52

FROM chef AS planner
WORKDIR /app
COPY . .
COPY ./.obelisk_build/Cargo.toml ./Cargo.toml
COPY ./.obelisk_build/main.rs ./src/main.rs
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
WORKDIR /app
COPY --from=planner /app/recipe.json recipe.json
# Build dependencies - this is the caching Docker layer!
RUN cargo chef cook --release --recipe-path recipe.json
# Build application
# For users.
COPY obelis[k] ./obelisk
# For system. Might break if user has stuff named like this.
COPY commo[n] common
COPY functiona[l] functional
COPY deploymen[t] deployment
COPY microbenc[h] microbench
COPY persistenc[e] persistence
COPY low_level_system[s] low_level_systems
# General
COPY src src
# Build.
COPY ./.obelisk_build/Cargo.toml ./Cargo.toml
COPY ./.obelisk_build/main.rs ./src/main.rs
RUN ls
RUN cargo build --release

# We do not need the Rust toolchain to run the binary!
FROM python:slim AS runtime
RUN apt-get update
WORKDIR /app
# # Copy data file before all else. Most unlikely to be modified, most slow to push.
# # Disabled for now. Lambda's storage is way too slow.
# COPY obelisk_local_dat[a] obelisk_local_data
# Copy python requirements and install if exists.
COPY script[s]/requirements.tx[t] scripts/requirements.txt
RUN if test -f scripts/requirements.txt; then pip3 install -r scripts/requirements.txt --no-cache-dir; fi
# Copy setup and execute if exists.
COPY script[s]/setup.s[h] scripts/setup.sh
RUN if test -f scripts/setup.sh; then bash scripts/setup.sh; fi
# Copy remainining scripts or create if no scripts.
COPY script[s] scripts
RUN mkdir -p scripts
# Create entrypoint file if not exists.
RUN [ -f scripts/entrypoint.sh ] || echo "exec ./obelisk_main" >> scripts/entrypoint.sh
# Bring over main file.
COPY --from=builder /app/target/release/obelisk_main .
# Entrypoint.
ENTRYPOINT ["bash", "./scripts/entrypoint.sh"]
