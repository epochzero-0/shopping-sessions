FROM gcc:14-bookworm AS build
WORKDIR /src
COPY src/ ./src/
RUN g++ -O3 -march=x86-64-v3 -std=c++17 -static -pthread -o /pipeline src/main.cpp

FROM scratch
COPY --from=build /pipeline /pipeline
USER 1000:1000
CMD ["/pipeline"]
