#pragma once

#include <cstdint>
#include <string_view>

// Same function as internal/hash/hash.go. The zero byte between the tenant
// and the key keeps the two strings from running together.
inline std::uint32_t KeyHash(std::string_view tenant, std::string_view key) {
    std::uint32_t h = 2166136261u;
    auto mix = [&](std::string_view s) {
        for (unsigned char c : s) {
            h ^= c;
            h *= 16777619u;
        }
    };
    mix(tenant);
    h ^= 0;
    h *= 16777619u;
    mix(key);
    return h;
}

// acme / user-1. internal/hash/hash_test.go checks the same number.
inline constexpr std::uint32_t kKnownHash = 0xaa7a19bcu;
