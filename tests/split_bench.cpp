// Parse ablation: for_lines + split<N> vs scan_lines<N> over the 20M events (N=7) and clicks (N=4), nothing else.
// Usage: split_bench DATA_DIR THREADS [MODE]   Prints the min of 5 runs per mode (0 lines, 1 split, 2 scan).
// g++ -O3 -march=cascadelake -std=c++17 -pthread tests/split_bench.cpp
#include <dirent.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <string>
#include <thread>
#include <vector>

#include "../src/csv.hpp"

struct Chunk { const char* b; const char* e; int n; };

static void add_dir(const std::string& d, int n, std::vector<Chunk>& out, size_t& bytes) {
    DIR* dp = opendir(d.c_str());
    for (dirent* de; (de = readdir(dp));) {
        std::string f = de->d_name;
        if (f.size() < 4 || f.substr(f.size() - 4) != ".csv") continue;
        int fd = open((d + "/" + f).c_str(), O_RDONLY);
        struct stat st;
        fstat(fd, &st);
        const char* p = (const char*)mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
        close(fd);
        const char* e = p + st.st_size;
        p = (const char*)memchr(p, '\n', e - p) + 1;
        bytes += e - p;
        while (p < e) {
            const char* q = p + (4 << 20);
            q = q >= e ? e : (const char*)memchr(q, '\n', e - q) + 1;
            out.push_back({p, q, n});
            p = q;
        }
    }
    closedir(dp);
}

template <int N>
static uint64_t sum(const char* const* fp, const uint32_t* fl) {
    uint64_t s = 0;
    for (int i = 0; i < N; i++) s += fl[i] + (uintptr_t)fp[i];
    return s;
}

int main(int argc, char** argv) {
    std::string d = argc > 1 ? argv[1] : "/data";
    int T = argc > 2 ? atoi(argv[2]) : 8;
    std::vector<Chunk> ch;
    size_t bytes = 0;
    add_dir(d + "/events", 7, ch, bytes);
    add_dir(d + "/clicks", 4, ch, bytes);
    printf("%.2f GB, %zu chunks, %d threads\n", bytes / 1e9, ch.size(), T);
    const char* names[] = {"for_lines only", "for_lines+split", "scan_lines"};
    int only = argc > 3 ? atoi(argv[3]) : -1;  // run one mode (for perf stat)
    for (int mode = 0; mode < 3; mode++) {
        if (only >= 0 && mode != only) continue;
        double best = 1e9;
        uint64_t check = 0, lines = 0;
        for (int rep = 0; rep < 5; rep++) {
            std::atomic<size_t> next{0};
            std::atomic<uint64_t> tot{0}, cnt{0};
            auto t0 = std::chrono::steady_clock::now();
            std::vector<std::thread> th;
            for (int t = 0; t < T; t++)
                th.emplace_back([&] {
                    uint64_t s = 0, c = 0;
                    for (size_t k; (k = next++) < ch.size();) {
                        const Chunk& x = ch[k];
                        if (mode == 0) {
                            for_lines(x.b, x.e, [&](const char* b, const char* e) { s += e - b; c++; });
                        } else if (mode == 1) {
                            for_lines(x.b, x.e, [&](const char* b, const char* e) {
                                const char* fp[7];
                                uint32_t fl[7];
                                if (x.n == 7 ? split<7>(b, e, fp, fl) : split<4>(b, e, fp, fl))
                                    s += x.n == 7 ? sum<7>(fp, fl) : sum<4>(fp, fl), c++;
                            });
                        } else if (x.n == 7) {
                            scan_lines<7>(x.b, x.e, [&](const char* const* fp, const uint32_t* fl) { s += sum<7>(fp, fl); c++; });
                        } else {
                            scan_lines<4>(x.b, x.e, [&](const char* const* fp, const uint32_t* fl) { s += sum<4>(fp, fl); c++; });
                        }
                    }
                    tot += s;
                    cnt += c;
                });
            for (auto& x : th) x.join();
            best = std::min(best, std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count());
            check = tot;
            lines = cnt;
        }
        printf("%-16s %.3f s  %.2f GB/s  lines %lu  check %lx\n", names[mode], best, bytes / best / 1e9,
               (unsigned long)lines, (unsigned long)check);
    }
}
