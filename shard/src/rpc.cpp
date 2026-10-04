#include "engine.hpp"
#include "hash.hpp"

#include <winsock2.h>
#include <ws2tcpip.h>

#include <cstdint>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {

constexpr std::uint8_t kGet = 1;
constexpr std::uint8_t kPut = 2;
constexpr std::uint8_t kScan = 3;
constexpr std::uint8_t kSync = 4;
constexpr std::uint8_t kFence = 5;
constexpr std::uint8_t kDrop = 6;
constexpr std::uint8_t kPull = 7;
constexpr std::uint8_t kInstall = 8;
constexpr std::uint8_t kObserve = 9;

constexpr std::uint8_t kOK = 0;
constexpr std::uint8_t kNotFound = 1;
constexpr std::uint8_t kWrongShard = 2;
constexpr std::uint8_t kStaleEpoch = 3;
constexpr std::uint8_t kUnavailable = 4;
constexpr std::uint8_t kInvalid = 5;
constexpr std::uint8_t kRetry = 6;

constexpr std::size_t kMaxFrame = 4 << 20;

std::uint8_t StatusByte(Failure fail) {
    switch (fail) {
        case Failure::Ok:
            return kOK;
        case Failure::NotFound:
            return kNotFound;
        case Failure::WrongShard:
            return kWrongShard;
        case Failure::StaleEpoch:
            return kStaleEpoch;
        case Failure::Unavailable:
            return kUnavailable;
        case Failure::Invalid:
            return kInvalid;
        case Failure::Retry:
            return kRetry;
    }
    return kUnavailable;
}

void AppendU8(std::string& b, std::uint8_t v) { b.push_back(static_cast<char>(v)); }

void AppendU32(std::string& b, std::uint32_t v) {
    for (int i = 0; i < 4; i++) b.push_back(static_cast<char>((v >> (8 * i)) & 0xff));
}

void AppendU64(std::string& b, std::uint64_t v) {
    for (int i = 0; i < 8; i++) b.push_back(static_cast<char>((v >> (8 * i)) & 0xff));
}

void AppendBytes(std::string& b, const std::string& s) {
    AppendU32(b, static_cast<std::uint32_t>(s.size()));
    b.append(s);
}

class Reader {
public:
    explicit Reader(std::string bytes) : bytes_(std::move(bytes)) {}
    std::size_t Left() const { return bytes_.size() - pos_; }
    bool Empty() const { return pos_ >= bytes_.size(); }

    std::uint8_t U8() { Need(1); return static_cast<std::uint8_t>(bytes_[pos_++]); }

    std::uint32_t U32() {
        Need(4);
        std::uint32_t v = 0;
        for (int i = 0; i < 4; i++) v |= static_cast<std::uint32_t>(static_cast<unsigned char>(bytes_[pos_++])) << (8 * i);
        return v;
    }

    std::uint64_t U64() {
        Need(8);
        std::uint64_t v = 0;
        for (int i = 0; i < 8; i++) v |= static_cast<std::uint64_t>(static_cast<unsigned char>(bytes_[pos_++])) << (8 * i);
        return v;
    }

    std::string Bytes() {
        auto n = U32();
        Need(n);
        std::string s = bytes_.substr(pos_, n);
        pos_ += n;
        return s;
    }

private:
    void Need(std::size_t n) const {
        if (pos_ + n > bytes_.size()) throw std::runtime_error("short frame");
    }
    std::string bytes_;
    std::size_t pos_ = 0;
};

bool RecvAll(SOCKET s, char* dst, int n) {
    int got = 0;
    while (got < n) {
        int r = recv(s, dst + got, n - got, 0);
        if (r <= 0) return false;
        got += r;
    }
    return true;
}

bool SendAll(SOCKET s, const char* src, int n) {
    int sent = 0;
    while (sent < n) {
        int r = send(s, src + sent, n - sent, 0);
        if (r <= 0) return false;
        sent += r;
    }
    return true;
}

bool WriteFrame(SOCKET s, const std::string& body);

bool ReadFrame(SOCKET s, std::string* out) {
    char lenb[4];
    if (!RecvAll(s, lenb, 4)) return false;
    std::uint32_t n = 0;
    for (int i = 0; i < 4; i++) n |= static_cast<std::uint32_t>(static_cast<unsigned char>(lenb[i])) << (8 * i);
    if (n > kMaxFrame) return false;
    out->assign(n, '\0');
    if (n == 0) return true;
    return RecvAll(s, out->data(), static_cast<int>(n));
}

Failure FromStatus(std::uint8_t status) {
    switch (status) {
        case kOK:
            return Failure::Ok;
        case kNotFound:
            return Failure::NotFound;
        case kWrongShard:
            return Failure::WrongShard;
        case kStaleEpoch:
            return Failure::StaleEpoch;
        case kUnavailable:
            return Failure::Unavailable;
        case kInvalid:
            return Failure::Invalid;
        case kRetry:
            return Failure::Retry;
        default:
            return Failure::Unavailable;
    }
}

SOCKET Dial(const std::string& address) {
    auto colon = address.rfind(':');
    if (colon == std::string::npos) return INVALID_SOCKET;
    auto host = address.substr(0, colon);
    auto port = static_cast<unsigned short>(std::stoi(address.substr(colon + 1)));
    SOCKET sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (sock == INVALID_SOCKET) return INVALID_SOCKET;
    DWORD timeout = 10000;
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, reinterpret_cast<char*>(&timeout), sizeof(timeout));
    setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, reinterpret_cast<char*>(&timeout), sizeof(timeout));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1 ||
        connect(sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        closesocket(sock);
        return INVALID_SOCKET;
    }
    return sock;
}

// PullFrom asks the source to scan, then stages each record on this shard.
// The caller is the destination. The worker never sees the bytes.
struct SocketGuard {
    SOCKET sock = INVALID_SOCKET;
    ~SocketGuard() {
        if (sock != INVALID_SOCKET) closesocket(sock);
    }
};

Failure PullFrom(Engine& engine, const std::string& source, const std::string& tenant, std::int64_t start, std::int64_t end,
                 std::uint64_t after, std::uint64_t through, std::int64_t epoch, const std::string& move_id,
                 std::uint64_t* count, std::uint64_t* durable) {
    SocketGuard guard{Dial(source)};
    if (guard.sock == INVALID_SOCKET) return Failure::Unavailable;
    std::string body;
    AppendU8(body, kScan);
    AppendU64(body, static_cast<std::uint64_t>(epoch));
    AppendU64(body, static_cast<std::uint64_t>(start));
    AppendU64(body, static_cast<std::uint64_t>(end));
    AppendU64(body, after);
    AppendU64(body, through);
    AppendBytes(body, tenant);
    if (!WriteFrame(guard.sock, body)) return Failure::Unavailable;
    std::string frame;
    if (!ReadFrame(guard.sock, &frame) || frame.empty()) return Failure::Unavailable;
    auto fail = FromStatus(static_cast<std::uint8_t>(frame[0]));
    if (fail != Failure::Ok) return fail;
    try {
        for (;;) {
            if (!ReadFrame(guard.sock, &frame)) return Failure::Unavailable;
            if (frame.empty()) break;
            Reader rec(frame);
            Record row;
            row.position = rec.U64();
            row.type = rec.U8() == 1 ? RecType::Delete : RecType::Put;
            row.key = rec.Bytes();
            row.value = rec.Bytes();
            row.tenant = tenant;
            row.hash = KeyHash(tenant, row.key);
            if (auto staged = engine.Stage(move_id, row); staged != Failure::Ok) return staged;
        }
    } catch (const std::exception&) {
        return Failure::Invalid;
    }
    if (auto synced = engine.SyncPending(move_id); synced != Failure::Ok) return synced;
    *count = engine.PendingCount(move_id);
    *durable = through;
    return Failure::Ok;
}

bool WriteFrame(SOCKET s, const std::string& body) {
    char lenb[4];
    auto n = static_cast<std::uint32_t>(body.size());
    for (int i = 0; i < 4; i++) lenb[i] = static_cast<char>((n >> (8 * i)) & 0xff);
    if (!SendAll(s, lenb, 4)) return false;
    if (n == 0) return true;
    return SendAll(s, body.data(), static_cast<int>(n));
}

void Handle(Engine& engine, SOCKET client) {
    std::string frame;
    if (!ReadFrame(client, &frame) || frame.empty()) {
        closesocket(client);
        return;
    }
    try {
        Reader r(frame);
        auto method = r.U8();
        std::string reply;
        if (method == kGet) {
            auto epoch = static_cast<std::int64_t>(r.U64());
            auto tenant = r.Bytes();
            auto key = r.Bytes();
            std::string value;
            auto fail = engine.Get(tenant, key, epoch, &value);
            AppendU8(reply, StatusByte(fail));
            if (fail == Failure::Ok) AppendBytes(reply, value);
            WriteFrame(client, reply);
        } else if (method == kPut) {
            auto epoch = static_cast<std::int64_t>(r.U64());
            auto tenant = r.Bytes();
            auto key = r.Bytes();
            auto value = r.Bytes();
            auto fail = engine.Put(tenant, key, std::move(value), epoch);
            AppendU8(reply, StatusByte(fail));
            WriteFrame(client, reply);
        } else if (method == kSync) {
            std::uint64_t pos = 0;
            auto fail = engine.Sync(&pos);
            AppendU8(reply, StatusByte(fail));
            if (fail == Failure::Ok) AppendU64(reply, pos);
            WriteFrame(client, reply);
        } else if (method == kScan) {
            auto epoch = static_cast<std::int64_t>(r.U64());
            auto start = static_cast<std::int64_t>(r.U64());
            auto end = static_cast<std::int64_t>(r.U64());
            auto after = r.U64();
            auto through = r.U64();
            auto tenant = r.Bytes();
            std::vector<Record> records;
            auto fail = engine.Scan(tenant, start, end, after, through, epoch, &records);
            AppendU8(reply, StatusByte(fail));
            if (!WriteFrame(client, reply) || fail != Failure::Ok) {
                closesocket(client);
                return;
            }
            for (const auto& rec : records) {
                std::string body;
                AppendU64(body, rec.position);
                AppendU8(body, rec.type == RecType::Delete ? 1 : 0);
                AppendBytes(body, rec.key);
                AppendBytes(body, rec.value);
                if (!WriteFrame(client, body)) {
                    closesocket(client);
                    return;
                }
            }
            WriteFrame(client, "");
        } else if (method == kFence) {
            auto epoch = static_cast<std::int64_t>(r.U64());
            auto start = static_cast<std::int64_t>(r.U64());
            auto end = static_cast<std::int64_t>(r.U64());
            auto tenant = r.Bytes();
            AppendU8(reply, StatusByte(engine.Fence(tenant, start, end, epoch)));
            WriteFrame(client, reply);
        } else if (method == kDrop) {
            auto epoch = static_cast<std::int64_t>(r.U64());
            auto start = static_cast<std::int64_t>(r.U64());
            auto end = static_cast<std::int64_t>(r.U64());
            auto tenant = r.Bytes();
            AppendU8(reply, StatusByte(engine.DropRange(tenant, start, end, epoch)));
            WriteFrame(client, reply);
        } else if (method == kPull) {
            auto epoch = static_cast<std::int64_t>(r.U64());
            auto start = static_cast<std::int64_t>(r.U64());
            auto end = static_cast<std::int64_t>(r.U64());
            auto after = r.U64();
            auto through = r.U64();
            auto tenant = r.Bytes();
            auto source = r.Bytes();
            auto move_id = r.Bytes();
            std::uint64_t count = 0;
            std::uint64_t durable = 0;
            auto fail = PullFrom(engine, source, tenant, start, end, after, through, epoch, move_id, &count, &durable);
            AppendU8(reply, StatusByte(fail));
            if (fail == Failure::Ok) {
                AppendU64(reply, count);
                AppendU64(reply, durable);
            }
            WriteFrame(client, reply);
        } else if (method == kObserve) {
            AppendU8(reply, StatusByte(engine.Observe()));
            WriteFrame(client, reply);
        } else if (method == kInstall) {
            auto start = static_cast<std::int64_t>(r.U64());
            auto end = static_cast<std::int64_t>(r.U64());
            auto tenant = r.Bytes();
            auto move_id = r.Bytes();
            AppendU8(reply, StatusByte(engine.Install(move_id, tenant, start, end)));
            WriteFrame(client, reply);
        } else {
            AppendU8(reply, kInvalid);
            WriteFrame(client, reply);
        }
    } catch (const std::exception&) {
        std::string reply;
        AppendU8(reply, kInvalid);
        WriteFrame(client, reply);
    }
    closesocket(client);
}

}  // namespace

void Serve(Engine& engine, const std::string& address) {
    WSADATA wsa;
    if (WSAStartup(MAKEWORD(2, 2), &wsa) != 0) {
        throw std::runtime_error("winsock");
    }
    auto colon = address.rfind(':');
    if (colon == std::string::npos) throw std::runtime_error("listen address needs a port");
    auto host = address.substr(0, colon);
    auto port = static_cast<unsigned short>(std::stoi(address.substr(colon + 1)));

    SOCKET listener = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (listener == INVALID_SOCKET) throw std::runtime_error("socket");
    int yes = 1;
    setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<char*>(&yes), sizeof(yes));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) throw std::runtime_error("bad listen host");
    if (bind(listener, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) throw std::runtime_error("bind");
    if (::listen(listener, 16) != 0) throw std::runtime_error("listen");

    for (;;) {
        SOCKET client = accept(listener, nullptr, nullptr);
        if (client == INVALID_SOCKET) continue;
        std::thread([&engine, client] { Handle(engine, client); }).detach();
    }
}
