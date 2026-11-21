FROM gcc:14-bookworm AS build
WORKDIR /src
COPY src/ ./src/
RUN g++ -O3 -march=cascadelake -std=c++17 -flto -static -pthread -o /pipeline src/*.cpp

FROM scratch
COPY --from=build /pipeline /pipeline
USER 1000:1000
CMD ["/pipeline"]
