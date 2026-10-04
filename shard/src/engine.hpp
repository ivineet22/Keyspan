#pragma once

#include "log.hpp"

#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <utility>
#include <vector>

// Failure is the outcome of one shard call, before it becomes a status byte.
// Retry means the range is fenced: the put was refused and the client should
// call again after the move finishes.
enum class Failure {
    Ok,
    NotFound,
    WrongShard,
    StaleEpoch,
    Unavailable,
    Invalid,
    Retry,
};

// Engine owns the log, the memory index, and the Postgres connection used to
// read which ranges this process is allowed to serve.
class Engine {
public:
    Engine(std::string name, std::wstring data_dir, std::string database_url);
    ~Engine();

    Engine(const Engine&) = delete;
    Engine& operator=(const Engine&) = delete;

    // Replay the log, then drop any key Postgres does not assign to this shard.
    void Recover();

    Failure Get(const std::string& tenant, const std::string& key, std::int64_t epoch, std::string* value);
    Failure Put(const std::string& tenant, const std::string& key, std::string value, std::int64_t epoch);
    Failure Scan(const std::string& tenant, std::int64_t hash_start, std::int64_t hash_end,
                 std::uint64_t after_position, std::uint64_t through_position, std::int64_t epoch,
                 std::vector<Record>* out);
    Failure Sync(std::uint64_t* position);

    // Fence refuses later puts on this span while this shard still owns it.
    Failure Fence(const std::string& tenant, std::int64_t hash_start, std::int64_t hash_end, std::int64_t epoch);
    // Observe re-reads Postgres and keeps a fence only while this shard still
    // owns the span and the job is fenced, committed, or cleaning.
    Failure Observe();
    // Stage appends one scanned record into the pending log for a move.
    // A repeated position is ignored. The record is not served yet.
    Failure Stage(const std::string& move_id, const Record& rec);
    // SyncPending forces the pending log of this move toward disk.
    Failure SyncPending(const std::string& move_id);
    std::uint64_t PendingCount(const std::string& move_id);
    // Install copies the pending log into the serving log once Postgres says
    // this shard owns the span. A missing pending file is success.
    Failure Install(const std::string& move_id, const std::string& tenant, std::int64_t hash_start, std::int64_t hash_end);
    // DropRange deletes the span only after Postgres says someone else owns it
    // at new_epoch or later. Repeating that delete is success.
    Failure DropRange(const std::string& tenant, std::int64_t hash_start, std::int64_t hash_end, std::int64_t new_epoch);

private:
    struct OwnedRange {
        std::string tenant;
        std::int64_t start = 0;
        std::int64_t end = 0;
        std::string owner;
        std::int64_t epoch = 0;
    };
    struct Entry {
        std::string value;
        std::uint32_t hash = 0;
    };

    Failure AuthorizeKey(const std::string& tenant, std::uint32_t hash, std::int64_t epoch);
    Failure AuthorizeSpan(const std::string& tenant, std::int64_t start, std::int64_t end, std::int64_t epoch);
    struct FenceSpan {
        std::string tenant;
        std::int64_t start = 0;
        std::int64_t end = 0;
    };

    void Apply(const Record& rec);
    void Remember(const std::string& tenant, const std::vector<OwnedRange>& rows);
    std::vector<OwnedRange> Load(const std::string& sql, const std::vector<std::string>& params, bool* missing_table);
    std::vector<std::vector<std::string>> Query(const std::string& sql, const std::vector<std::string>& params, bool* missing_table);
    void Reconcile();
    void ReloadFencesLocked();
    void AddFence(const FenceSpan& span);
    bool Fenced(const std::string& tenant, std::uint32_t hash) const;
    bool Owns(const std::string& tenant, std::int64_t start, std::int64_t end);
    std::wstring PendingPath(const std::string& move_id) const;
    Log* OpenPendingLocked(const std::string& move_id);
    void InstallLocked(const std::string& move_id);
    void DiscardLocked(const std::string& move_id);

    std::string name_;
    std::wstring data_dir_;
    Log log_;
    void* pg_ = nullptr;
    std::mutex mu_;
    std::vector<OwnedRange> cache_;
    std::map<std::pair<std::string, std::string>, Entry> index_;
    std::map<std::string, std::unique_ptr<Log>> pending_;
    std::map<std::string, std::set<std::uint64_t>> pending_seen_;
    std::vector<FenceSpan> fences_;
};
