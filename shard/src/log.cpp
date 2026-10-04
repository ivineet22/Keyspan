#include "log.hpp"

#define WIN32_LEAN_AND_MEAN
#include <windows.h>

#include <stdexcept>
#include <string>
#include <vector>

namespace {

void AppendU8(std::string& b, std::uint8_t v) { b.push_back(static_cast<char>(v)); }

void AppendU32(std::string& b, std::uint32_t v) {
    for (int i = 0; i < 4; i++) {
        b.push_back(static_cast<char>((v >> (8 * i)) & 0xff));
    }
}

void AppendU64(std::string& b, std::uint64_t v) {
    for (int i = 0; i < 8; i++) {
        b.push_back(static_cast<char>((v >> (8 * i)) & 0xff));
    }
}

void AppendBytes(std::string& b, const std::string& s) {
    AppendU32(b, static_cast<std::uint32_t>(s.size()));
    b.append(s);
}

class Reader {
public:
    explicit Reader(std::string bytes) : bytes_(std::move(bytes)) {}

    bool Remaining(std::size_t n) const { return pos_ + n <= bytes_.size(); }

    void Need(std::size_t n) const {
        if (pos_ + n > bytes_.size()) {
            throw std::runtime_error("torn log record");
        }
    }

    std::uint8_t U8() {
        Need(1);
        return static_cast<std::uint8_t>(bytes_[pos_++]);
    }

    std::uint32_t U32() {
        Need(4);
        std::uint32_t v = 0;
        for (int i = 0; i < 4; i++) {
            v |= static_cast<std::uint32_t>(static_cast<unsigned char>(bytes_[pos_++])) << (8 * i);
        }
        return v;
    }

    std::uint64_t U64() {
        Need(8);
        std::uint64_t v = 0;
        for (int i = 0; i < 8; i++) {
            v |= static_cast<std::uint64_t>(static_cast<unsigned char>(bytes_[pos_++])) << (8 * i);
        }
        return v;
    }

    std::string Bytes() {
        auto n = U32();
        if (pos_ + n > bytes_.size()) {
            throw std::runtime_error("torn string in log record");
        }
        std::string s = bytes_.substr(pos_, n);
        pos_ += n;
        return s;
    }

private:
    std::string bytes_;
    std::size_t pos_ = 0;
};

void WriteAll(HANDLE file, const char* data, DWORD n) {
    DWORD done = 0;
    while (done < n) {
        DWORD wrote = 0;
        if (!WriteFile(file, data + done, n - done, &wrote, nullptr) || wrote == 0) {
            throw std::runtime_error("write log");
        }
        done += wrote;
    }
}

std::string ReadFileAll(HANDLE file) {
    LARGE_INTEGER size{};
    if (!GetFileSizeEx(file, &size)) {
        throw std::runtime_error("size log");
    }
    if (size.QuadPart == 0) {
        return {};
    }
    LARGE_INTEGER zero{};
    if (!SetFilePointerEx(file, zero, nullptr, FILE_BEGIN)) {
        throw std::runtime_error("rewind log");
    }
    std::string out(static_cast<std::size_t>(size.QuadPart), '\0');
    DWORD done = 0;
    auto* p = out.data();
    auto total = static_cast<DWORD>(out.size());
    while (done < total) {
        DWORD got = 0;
        if (!ReadFile(file, p + done, total - done, &got, nullptr) || got == 0) {
            throw std::runtime_error("read log");
        }
        done += got;
    }
    return out;
}

}  // namespace

Log::Log(std::wstring path) : path_(std::move(path)) {
    file_ = CreateFileW(path_.c_str(), GENERIC_READ | GENERIC_WRITE, FILE_SHARE_READ, nullptr,
                         OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);
    if (file_ == INVALID_HANDLE_VALUE) {
        throw std::runtime_error("open log");
    }
    auto bytes = ReadFileAll(static_cast<HANDLE>(file_));
    constexpr std::size_t kHeader = 8;
    if (bytes.size() < kHeader) {
        std::string header = "SLOG";
        AppendU32(header, 1);
        LARGE_INTEGER zero{};
        SetFilePointerEx(static_cast<HANDLE>(file_), zero, nullptr, FILE_BEGIN);
        SetEndOfFile(static_cast<HANDLE>(file_));
        WriteAll(static_cast<HANDLE>(file_), header.data(), static_cast<DWORD>(header.size()));
        return;
    }
    if (bytes.substr(0, 4) != "SLOG" || Reader(bytes.substr(4, 4)).U32() != 1) {
        throw std::runtime_error("bad log header");
    }
    std::size_t pos = kHeader;
    std::size_t good = kHeader;
    while (pos + 4 <= bytes.size()) {
        Reader len_reader(bytes.substr(pos, 4));
        auto body = len_reader.U32();
        if (pos + 4 + body > bytes.size()) {
            break;
        }
        Reader body_reader(bytes.substr(pos + 4, body));
        Record rec;
        rec.position = body_reader.U64();
        rec.type = static_cast<RecType>(body_reader.U8());
        last_ = rec.position;
        pos += 4 + body;
        good = pos;
    }
    LARGE_INTEGER end{};
    end.QuadPart = static_cast<LONGLONG>(good);
    if (!SetFilePointerEx(static_cast<HANDLE>(file_), end, nullptr, FILE_BEGIN)) {
        throw std::runtime_error("seek log end");
    }
    if (!SetEndOfFile(static_cast<HANDLE>(file_))) {
        throw std::runtime_error("trim torn log tail");
    }
}

Log::~Log() {
    if (file_ != nullptr && file_ != INVALID_HANDLE_VALUE) {
        CloseHandle(static_cast<HANDLE>(file_));
    }
}

void Log::Append(Record rec) {
    if (rec.position == 0) {
        rec.position = last_ + 1;
    }
    std::string body;
    AppendU64(body, rec.position);
    AppendU8(body, static_cast<std::uint8_t>(rec.type));
    AppendU32(body, rec.hash);
    AppendBytes(body, rec.tenant);
    AppendBytes(body, rec.key);
    AppendBytes(body, rec.value);
    AppendU64(body, static_cast<std::uint64_t>(rec.range_start));
    AppendU64(body, static_cast<std::uint64_t>(rec.range_end));

    std::string frame;
    AppendU32(frame, static_cast<std::uint32_t>(body.size()));
    frame.append(body);
    WriteAll(static_cast<HANDLE>(file_), frame.data(), static_cast<DWORD>(frame.size()));
    last_ = rec.position;
}

void Log::ForEach(const std::function<void(const Record&)>& fn) {
    auto bytes = ReadFileAll(static_cast<HANDLE>(file_));
    LARGE_INTEGER end{};
    end.QuadPart = static_cast<LONGLONG>(bytes.size());
    SetFilePointerEx(static_cast<HANDLE>(file_), end, nullptr, FILE_BEGIN);

    if (bytes.size() < 8) {
        return;
    }
    std::size_t pos = 8;
    while (pos + 4 <= bytes.size()) {
        Reader len_reader(bytes.substr(pos, 4));
        auto body_len = len_reader.U32();
        if (pos + 4 + body_len > bytes.size()) {
            return;
        }
        Reader r(bytes.substr(pos + 4, body_len));
        Record rec;
        rec.position = r.U64();
        rec.type = static_cast<RecType>(r.U8());
        rec.hash = r.U32();
        rec.tenant = r.Bytes();
        rec.key = r.Bytes();
        rec.value = r.Bytes();
        rec.range_start = static_cast<std::int64_t>(r.U64());
        rec.range_end = static_cast<std::int64_t>(r.U64());
        fn(rec);
        pos += 4 + body_len;
    }
}

std::uint64_t Log::Sync() {
    if (!FlushFileBuffers(static_cast<HANDLE>(file_))) {
        throw std::runtime_error("sync log");
    }
    return last_;
}

std::vector<std::wstring> ListFiles(const std::wstring& pattern) {
    std::vector<std::wstring> out;
    WIN32_FIND_DATAW fd;
    HANDLE h = FindFirstFileW(pattern.c_str(), &fd);
    if (h == INVALID_HANDLE_VALUE) {
        return out;
    }
    do {
        out.push_back(fd.cFileName);
    } while (FindNextFileW(h, &fd));
    FindClose(h);
    return out;
}

bool FileExists(const std::wstring& path) {
    auto attr = GetFileAttributesW(path.c_str());
    return attr != INVALID_FILE_ATTRIBUTES && (attr & FILE_ATTRIBUTE_DIRECTORY) == 0;
}

void RemoveFile(const std::wstring& path) {
    if (DeleteFileW(path.c_str())) {
        return;
    }
    if (GetLastError() != ERROR_FILE_NOT_FOUND) {
        throw std::runtime_error("delete file");
    }
}
