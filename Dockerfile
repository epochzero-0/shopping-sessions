FROM gcc:14-bookworm AS build
WORKDIR /src
COPY src/ ./src/
# pgo/*.gcda: trained on 20M by scripts/pgo.sh with these flags and paths. A stale profile only warns.
COPY pgo/ ./pgo/
RUN g++ -O3 -march=cascadelake -std=c++17 -flto -static -pthread -fprofile-prefix-path=/src -fprofile-use=pgo \
      -Wno-missing-profile -Wno-error=coverage-mismatch -o pipeline src/*.cpp && cp pipeline /pipeline

FROM scratch
COPY --from=build /pipeline /pipeline
USER 1000:1000
CMD ["/pipeline"]
