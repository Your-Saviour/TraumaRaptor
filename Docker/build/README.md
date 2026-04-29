# Velociraptor Build Environment

Docker-based build environment for compiling Velociraptor from source (Linux amd64).
Source is bind-mounted from the host so edits take effect immediately.

## Quick start

```bash
cd Docker/build

# Build the image once (or after go.mod changes)
docker compose build

# Drop into an interactive shell at /src
docker compose run --rm build

# Inside the container
make              # dev binary  -> output/velociraptor
make linux        # release binary
make test_light   # run tests (no race detector)
make test         # run tests with race detector
```

One-shot build without entering the shell:

```bash
docker compose run --rm build make linux
```

## Caching

Go module and build caches are stored in named Docker volumes (`go-mod-cache`,
`go-build-cache`) so they persist across container runs. Incremental rebuilds are fast.

## Extending

To add Windows cross-compile support, install `gcc-mingw-w64-x86-64` in the Dockerfile
and add `mingw_xcompiler` to your `make windows` invocation.

To add GUI/asset builds, install Node.js 20 and `npm` in the Dockerfile, then
run `make assets` before `make`.
