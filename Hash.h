#pragma once

#include <algorithm>
#include <string>

namespace Vague {
constexpr uint64_t rotr(uint64_t v, unsigned int k) {
    return (v >> k) | (v << (8 * sizeof(v) - k));
}

// https://lemire.me/blog/2018/08/15/fast-strongly-universal-64-bit-hashing-everywhere/
inline uint64_t hash64(const uint64_t v) {
    const uint64_t h1 = v * 0x57D2FE972BC49227ULL;
    const uint64_t h2 = rotr(v, 32) * 0xED1792717AE84439ULL;
    return rotr(h1 + h2, 32);
}

// MurmurHash2 by Austin Appleby
// https://github.com/aappleby/smhasher/blob/master/src/MurmurHash2.cpp
inline uint64_t hashbytes(const void *ptr, size_t len) {
    static constexpr uint64_t seed = 0xe17a1465ULL;
    static constexpr uint64_t m = 0xc6a4a7935bd1e995ULL;
    static constexpr int r = 47;

    uint64_t h = seed ^ (len * m);

    const uint64_t *data = (const uint64_t *)ptr;
    const uint64_t *end = data + (len / 8);

    while (data != end) {
        uint64_t k = *data++;

        k *= m;
        k ^= k >> r;
        k *= m;

        h ^= k;
        h *= m;
    }

    const unsigned char *data2 = (const unsigned char *)data;

    switch (len & 7) {
    case 7:
        h ^= uint64_t(data2[6]) << 48;
    case 6:
        h ^= uint64_t(data2[5]) << 40;
    case 5:
        h ^= uint64_t(data2[4]) << 32;
    case 4:
        h ^= uint64_t(data2[3]) << 24;
    case 3:
        h ^= uint64_t(data2[2]) << 16;
    case 2:
        h ^= uint64_t(data2[1]) << 8;
    case 1:
        h ^= uint64_t(data2[0]);
        h *= m;
    };

    h ^= h >> r;
    h *= m;
    h ^= h >> r;

    return hash64(h);
}

template <typename T, typename Enable = void>
struct Hash;

template <typename T>
struct Hash<T, typename std::enable_if<std::is_integral<T>::value>::type> {
    size_t operator()(const T v) const {
        return hash64((uint64_t)v);
    }
};

template <typename T>
struct Hash<T, typename std::enable_if<std::is_floating_point<T>::value>::type> {
    size_t operator()(const T v) const {
        uint64_t n;
        std::memcpy(&n, &v, std::min(sizeof(n), sizeof(v)));
        return hash64(n);
    }
};

template <typename CharT>
struct Hash<std::basic_string<CharT>> {
    size_t operator()(const std::basic_string<CharT> &str) const {
        return hashbytes(str.data(), sizeof(CharT) * str.size());
    }
};

template <>
struct Hash<long double> {
    size_t operator()(const long double &v) const {
        return hashbytes(&v, sizeof(v));
    }
};
} // namespace Vague