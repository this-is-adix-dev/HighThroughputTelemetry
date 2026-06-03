# syntax=docker/dockerfile:1.7
# ==============================================================================
# Stage 1 – Build
# Uses the full .NET 10 SDK image so the Native AOT toolchain (ILC + linker)
# is available.  The SDK image already ships with the msquic / libssl stubs
# needed by .NET itself; we only need to add the C compiler toolchain that
# ILC delegates object-file linking to.
# ==============================================================================
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build

# Install Native AOT prerequisites:
#   clang      – the C/C++ compiler ILC shells out to for the final link step.
#   zlib1g-dev – required by the zlib P/Invoke that the .NET runtime references
#                even in AOT mode; without it the linker fails to resolve symbols.
#   ca-certificates – ensures NuGet HTTPS restore works in hermetic build envs.
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      clang \
      zlib1g-dev \
      ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# ── Restore dependencies (cached layer) ──────────────────────────────────────
# Copy only the project / solution manifest files first so that Docker's layer
# cache is invalidated only when dependencies change, not on every source edit.
COPY HighThroughputTelemetry.slnx ./
COPY src/Telemetry.Engine/Telemetry.Engine.csproj src/Telemetry.Engine/

RUN dotnet restore src/Telemetry.Engine/Telemetry.Engine.csproj \
      --runtime linux-x64

# ── Copy source and publish ───────────────────────────────────────────────────
COPY src/ src/

RUN dotnet publish src/Telemetry.Engine/Telemetry.Engine.csproj \
      --configuration Release \
      --runtime linux-x64 \
      --self-contained \
      --no-restore \
      --output /app/publish \
      -p:PublishAot=true \
      -p:OptimizationPreference=Speed \
      -p:StripSymbols=true \
      -p:InvariantGlobalization=true

# ==============================================================================
# Stage 2 – Runtime (Ubuntu Chiseled)
# "Chiseled" images are Ubuntu containers stripped to the absolute minimum set
# of packages required to run a .NET native binary.  They ship with no shell,
# no package manager, and no SUID binaries, dramatically reducing the attack
# surface versus a full distro image.
#
# We use the *runtime-deps* variant (not the .NET runtime image) because a
# Native AOT binary carries its own runtime — the only host dependencies it
# needs are the OS-level shared libraries (libc, libssl, zlib, …), which the
# chiseled image provides.
# ==============================================================================
FROM mcr.microsoft.com/dotnet/nightly/runtime-deps:10.0-noble-chiseled AS runtime

WORKDIR /app

# Copy only the self-contained native binary produced by the build stage.
COPY --from=build /app/publish .

# ── Security posture ──────────────────────────────────────────────────────────
# APP_UID is set by the chiseled base image to the pre-created non-root UID
# (typically 1654).  Running as non-root satisfies CIS Docker Benchmark v1.6
# control DI-5 and is required by most Kubernetes PodSecurityStandards in
# "restricted" mode.
USER $APP_UID

ENTRYPOINT ["./Telemetry.Engine"]
