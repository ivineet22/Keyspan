#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

// A record in the append-only log.
// Put stores a value. Delete marks a key gone. Drop forgets every key of one
// tenant whose hash falls in [range_start, range_end).
enum class RecType : std::uint8_t { Put = 1, Delete = 2, Drop = 3 };

struct Record {
    std::uint64_t position = 0;
    RecType type = RecType::Put;
    std::uint32_t hash = 0;
    std::string tenant;
    std::string key;
    std::string value;
    std::int64_t range_start = 0;
    std::int64_t range_end = 0;
};

// Log is one file. Records are appended. A process crash can tear the last
// record; Open throws that torn tail away and keeps every complete record.
class Log {
public:
    explicit Log(std::wstring path);
    ~Log();

    Log(const Log&) = delete;
    Log& operator=(const Log&) = delete;

    void Append(Record rec);
    void ForEach(const std::function<void(const Record&)>& fn);
    std::uint64_t Sync();
    std::uint64_t LastPosition() const { return last_; }

private:
    void* file_ = nullptr;  // HANDLE, kept as void* so this header stays free of windows.h
    std::uint64_t last_ = 0;
    std::wstring path_;
};

// ListFiles returns the file names (not full paths) that match a wildcard.
std::vector<std::wstring> ListFiles(const std::wstring& pattern);
bool FileExists(const std::wstring& path);
void RemoveFile(const std::wstring& path);
