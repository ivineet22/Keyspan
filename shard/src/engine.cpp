#include "engine.hpp"

#include "hash.hpp"

#include <libpq-fe.h>

#include <algorithm>
#include <map>
#include <set>
#include <stdexcept>
#include <tuple>

namespace {

constexpr std::size_t kMaxName = 1024;
constexpr std::size_t kMaxValue = 1 << 20;

}

Engine::Engine(std::string name, std::wstring data_dir, std::string database_url)
    : name_(std::move(name)), data_dir_(std::move(data_dir)), log_(data_dir_ + L"\\log.bin") {
    pg_ = PQconnectdb(database_url.c_str());
    if (pg_ == nullptr || PQstatus(static_cast<PGconn*>(pg_)) != CONNECTION_OK) {
        std::string msg = pg_ ? PQerrorMessage(static_cast<PGconn*>(pg_)) : "no connection";
        if (pg_) {
            PQfinish(static_cast<PGconn*>(pg_));
            pg_ = nullptr;
        }
        throw std::runtime_error("postgres: " + msg);
    }
}

Engine::~Engine() {
    if (pg_ != nullptr) {
        PQfinish(static_cast<PGconn*>(pg_));
    }
}

void Engine::Apply(const Record& rec) {
    auto id = std::make_pair(rec.tenant, rec.key);
    if (rec.type == RecType::Put) {
        index_[id] = Entry{rec.value, rec.hash};
        return;
    }
    if (rec.type == RecType::Delete) {
        index_.erase(id);
        return;
    }
    if (rec.type == RecType::Drop) {
        for (auto it = index_.begin(); it != index_.end();) {
            std::int64_t h = it->second.hash;
            if (it->first.first == rec.tenant && h >= rec.range_start && h < rec.range_end) {
                it = index_.erase(it);
            } else {
                ++it;
            }
        }
    }
}

void Engine::Recover() {
    std::lock_guard<std::mutex> lock(mu_);
    log_.ForEach([this](const Record& rec) { Apply(rec); });

    bool missing = false;
    auto all = Load(
        "SELECT tenant_id, hash_start, hash_end, owner_shard, epoch FROM ranges",
        {}, &missing);
    if (missing) {
        return;
    }

    std::set<std::tuple<std::string, std::int64_t, std::int64_t>> drops;
    for (const auto& [id, entry] : index_) {
        const std::string& tenant = id.first;
        std::int64_t h = entry.hash;
        const OwnedRange* cover = nullptr;
        for (const auto& rg : all) {
            if (rg.tenant == tenant && h >= rg.start && h < rg.end) {
                cover = &rg;
                break;
            }
        }
        if (cover != nullptr && cover->owner == name_) {
            continue;
        }
        if (cover != nullptr) {
            drops.emplace(tenant, cover->start, cover->end);
        } else {
            drops.emplace(tenant, h, h + 1);
        }
    }
    for (const auto& drop : drops) {
        Record rec;
        rec.type = RecType::Drop;
        rec.tenant = std::get<0>(drop);
        rec.range_start = std::get<1>(drop);
        rec.range_end = std::get<2>(drop);
        log_.Append(rec);
        Apply(rec);
    }
    Reconcile();
}

Failure Engine::Get(const std::string& tenant, const std::string& key, std::int64_t epoch, std::string* value) {
    std::lock_guard<std::mutex> lock(mu_);
    auto hash = KeyHash(tenant, key);
    if (auto fail = AuthorizeKey(tenant, hash, epoch); fail != Failure::Ok) {
        return fail;
    }
    auto it = index_.find({tenant, key});
    if (it == index_.end()) {
        return Failure::NotFound;
    }
    *value = it->second.value;
    return Failure::Ok;
}

Failure Engine::Put(const std::string& tenant, const std::string& key, std::string value, std::int64_t epoch) {
    if (tenant.size() > kMaxName || key.size() > kMaxName || value.size() > kMaxValue) {
        return Failure::Invalid;
    }
    std::lock_guard<std::mutex> lock(mu_);
    auto hash = KeyHash(tenant, key);
    if (auto fail = AuthorizeKey(tenant, hash, epoch); fail != Failure::Ok) {
        return fail;
    }
    if (Fenced(tenant, hash)) {
        // The in-memory fence is a cache. A crash before commit voids it, so
        // read Postgres again before refusing the put.
        try {
            ReloadFencesLocked();
        } catch (const std::exception&) {
            return Failure::Unavailable;
        }
        if (Fenced(tenant, hash)) {
            return Failure::Retry;
        }
    }
    Record rec;
    rec.type = RecType::Put;
    rec.hash = hash;
    rec.tenant = tenant;
    rec.key = key;
    rec.value = std::move(value);
    try {
        log_.Append(rec);
    } catch (const std::exception&) {
        return Failure::Unavailable;
    }
    Apply(rec);
    return Failure::Ok;
}

Failure Engine::Scan(const std::string& tenant, std::int64_t hash_start, std::int64_t hash_end,
                     std::uint64_t after_position, std::uint64_t through_position, std::int64_t epoch,
                     std::vector<Record>* out) {
    if (hash_start >= hash_end) {
        return Failure::Invalid;
    }
    std::lock_guard<std::mutex> lock(mu_);
    if (auto fail = AuthorizeSpan(tenant, hash_start, hash_end, epoch); fail != Failure::Ok) {
        return fail;
    }
    // Repeated puts of one key collapse to the latest record in this window.
    // The destination only needs that record, applied in position order.
    std::map<std::string, Record> latest;
    log_.ForEach([&](const Record& rec) {
        if (rec.type != RecType::Put && rec.type != RecType::Delete) {
            return;
        }
        if (rec.tenant != tenant) {
            return;
        }
        std::int64_t h = rec.hash;
        if (h < hash_start || h >= hash_end) {
            return;
        }
        if (rec.position <= after_position) {
            return;
        }
        if (through_position != 0 && rec.position > through_position) {
            return;
        }
        latest[rec.key] = rec;
    });
    std::vector<Record> ordered;
    ordered.reserve(latest.size());
    for (auto& entry : latest) {
        ordered.push_back(std::move(entry.second));
    }
    std::sort(ordered.begin(), ordered.end(), [](const Record& a, const Record& b) {
        return a.position < b.position;
    });
    out->insert(out->end(), ordered.begin(), ordered.end());
    return Failure::Ok;
}

Failure Engine::Sync(std::uint64_t* position) {
    std::lock_guard<std::mutex> lock(mu_);
    try {
        *position = log_.Sync();
    } catch (const std::exception&) {
        return Failure::Unavailable;
    }
    return Failure::Ok;
}

void Engine::Remember(const std::string& tenant, const std::vector<OwnedRange>& rows) {
    std::vector<OwnedRange> next;
    for (const auto& rg : cache_) {
        if (rg.tenant != tenant) {
            next.push_back(rg);
        }
    }
    next.insert(next.end(), rows.begin(), rows.end());
    cache_.swap(next);
}

Failure Engine::AuthorizeKey(const std::string& tenant, std::uint32_t hash, std::int64_t epoch) {
    std::int64_t h = hash;
    for (const auto& rg : cache_) {
        if (rg.tenant == tenant && h >= rg.start && h < rg.end && rg.epoch == epoch && rg.owner == name_) {
            return Failure::Ok;
        }
    }
    bool missing = false;
    std::vector<OwnedRange> rows;
    try {
        rows = Load(
            "SELECT tenant_id, hash_start, hash_end, owner_shard, epoch FROM ranges WHERE tenant_id = $1",
            {tenant}, &missing);
    } catch (const std::exception&) {
        return Failure::Unavailable;
    }
    if (missing) {
        return Failure::Unavailable;
    }
    Remember(tenant, rows);
    const OwnedRange* found = nullptr;
    for (const auto& rg : cache_) {
        if (rg.tenant == tenant && h >= rg.start && h < rg.end) {
            found = &rg;
            break;
        }
    }
    if (found == nullptr || found->owner != name_) {
        return Failure::WrongShard;
    }
    if (found->epoch != epoch) {
        return Failure::StaleEpoch;
    }
    return Failure::Ok;
}

Failure Engine::AuthorizeSpan(const std::string& tenant, std::int64_t start, std::int64_t end, std::int64_t epoch) {
    auto covers = [&](const OwnedRange& rg) {
        return rg.tenant == tenant && rg.start <= start && rg.end >= end && rg.owner == name_ && rg.epoch == epoch;
    };
    for (const auto& rg : cache_) {
        if (covers(rg)) {
            return Failure::Ok;
        }
    }
    bool missing = false;
    std::vector<OwnedRange> rows;
    try {
        rows = Load(
            "SELECT tenant_id, hash_start, hash_end, owner_shard, epoch FROM ranges WHERE tenant_id = $1",
            {tenant}, &missing);
    } catch (const std::exception&) {
        return Failure::Unavailable;
    }
    if (missing) {
        return Failure::Unavailable;
    }
    Remember(tenant, rows);
    for (const auto& rg : cache_) {
        if (covers(rg)) {
            return Failure::Ok;
        }
    }
    for (const auto& rg : cache_) {
        if (rg.tenant == tenant && rg.start <= start && rg.end >= end && rg.owner == name_ && rg.epoch != epoch) {
            return Failure::StaleEpoch;
        }
    }
    return Failure::WrongShard;
}

std::vector<Engine::OwnedRange> Engine::Load(const std::string& sql, const std::vector<std::string>& params, bool* missing_table) {
    *missing_table = false;
    auto* pg = static_cast<PGconn*>(pg_);
    if (PQstatus(pg) != CONNECTION_OK) {
        PQreset(pg);
    }
    std::vector<const char*> values;
    values.reserve(params.size());
    for (const auto& p : params) {
        values.push_back(p.c_str());
    }
    PGresult* res = PQexecParams(pg, sql.c_str(), static_cast<int>(params.size()), nullptr,
                                 values.empty() ? nullptr : values.data(), nullptr, nullptr, 0);
    if (res == nullptr) {
        throw std::runtime_error(PQerrorMessage(pg));
    }
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        const char* state = PQresultErrorField(res, PG_DIAG_SQLSTATE);
        if (state != nullptr && std::string(state) == "42P01") {
            *missing_table = true;
            PQclear(res);
            return {};
        }
        std::string msg = PQerrorMessage(pg);
        PQclear(res);
        throw std::runtime_error(msg);
    }
    std::vector<OwnedRange> rows;
    int n = PQntuples(res);
    for (int i = 0; i < n; i++) {
        OwnedRange rg;
        rg.tenant = PQgetvalue(res, i, 0);
        rg.start = std::stoll(PQgetvalue(res, i, 1));
        rg.end = std::stoll(PQgetvalue(res, i, 2));
        rg.owner = PQgetvalue(res, i, 3);
        rg.epoch = std::stoll(PQgetvalue(res, i, 4));
        rows.push_back(std::move(rg));
    }
    PQclear(res);
    return rows;
}

Failure Engine::Observe() {
    std::lock_guard<std::mutex> lock(mu_);
    try {
        ReloadFencesLocked();
    } catch (const std::exception&) {
        return Failure::Unavailable;
    }
    return Failure::Ok;
}

Failure Engine::Fence(const std::string& tenant, std::int64_t hash_start, std::int64_t hash_end, std::int64_t epoch) {
    if (hash_start >= hash_end) {
        return Failure::Invalid;
    }
    std::lock_guard<std::mutex> lock(mu_);
    if (auto fail = AuthorizeSpan(tenant, hash_start, hash_end, epoch); fail != Failure::Ok) {
        return fail;
    }
    AddFence(FenceSpan{tenant, hash_start, hash_end});
    return Failure::Ok;
}

Failure Engine::Stage(const std::string& move_id, const Record& rec) {
    if (move_id.empty() || rec.position == 0) {
        return Failure::Invalid;
    }
    std::lock_guard<std::mutex> lock(mu_);
    try {
        OpenPendingLocked(move_id);
        auto& seen = pending_seen_[move_id];
        if (seen.count(rec.position) != 0) {
            return Failure::Ok;
        }
        pending_[move_id]->Append(rec);
        seen.insert(rec.position);
    } catch (const std::exception&) {
        return Failure::Unavailable;
    }
    return Failure::Ok;
}

Failure Engine::SyncPending(const std::string& move_id) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = pending_.find(move_id);
    if (it == pending_.end()) {
        return Failure::Ok;
    }
    try {
        it->second->Sync();
    } catch (const std::exception&) {
        return Failure::Unavailable;
    }
    return Failure::Ok;
}

std::uint64_t Engine::PendingCount(const std::string& move_id) {
    std::lock_guard<std::mutex> lock(mu_);
    return pending_seen_[move_id].size();
}

Failure Engine::Install(const std::string& move_id, const std::string& tenant, std::int64_t hash_start, std::int64_t hash_end) {
    std::lock_guard<std::mutex> lock(mu_);
    try {
        if (!Owns(tenant, hash_start, hash_end)) {
            return Failure::Unavailable;
        }
        InstallLocked(move_id);
    } catch (const std::exception&) {
        return Failure::Unavailable;
    }
    return Failure::Ok;
}

Failure Engine::DropRange(const std::string& tenant, std::int64_t hash_start, std::int64_t hash_end, std::int64_t new_epoch) {
    if (hash_start >= hash_end) {
        return Failure::Invalid;
    }
    std::lock_guard<std::mutex> lock(mu_);
    bool missing = false;
    std::vector<OwnedRange> rows;
    try {
        rows = Load(
            "SELECT tenant_id, hash_start, hash_end, owner_shard, epoch FROM ranges WHERE tenant_id = $1",
            {tenant}, &missing);
    } catch (const std::exception&) {
        return Failure::Unavailable;
    }
    if (missing) {
        return Failure::Unavailable;
    }
    const OwnedRange* found = nullptr;
    for (const auto& rg : rows) {
        if (rg.tenant == tenant && rg.start <= hash_start && rg.end >= hash_end) {
            found = &rg;
            break;
        }
    }
    if (found == nullptr) {
        return Failure::Invalid;
    }
    if (found->owner == name_ || found->epoch < new_epoch) {
        return Failure::Unavailable;
    }
    Remember(tenant, rows);
    Record rec;
    rec.type = RecType::Drop;
    rec.tenant = tenant;
    rec.range_start = hash_start;
    rec.range_end = hash_end;
    try {
        log_.Append(rec);
    } catch (const std::exception&) {
        return Failure::Unavailable;
    }
    Apply(rec);
    std::vector<FenceSpan> kept;
    for (const auto& fence : fences_) {
        if (fence.tenant == tenant && fence.start == hash_start && fence.end == hash_end) {
            continue;
        }
        kept.push_back(fence);
    }
    fences_.swap(kept);
    return Failure::Ok;
}

void Engine::AddFence(const FenceSpan& span) {
    for (const auto& fence : fences_) {
        if (fence.tenant == span.tenant && fence.start == span.start && fence.end == span.end) {
            return;
        }
    }
    fences_.push_back(span);
}

bool Engine::Fenced(const std::string& tenant, std::uint32_t hash) const {
    std::int64_t h = hash;
    for (const auto& fence : fences_) {
        if (fence.tenant == tenant && h >= fence.start && h < fence.end) {
            return true;
        }
    }
    return false;
}

bool Engine::Owns(const std::string& tenant, std::int64_t start, std::int64_t end) {
    bool missing = false;
    auto rows = Load(
        "SELECT tenant_id, hash_start, hash_end, owner_shard, epoch FROM ranges WHERE tenant_id = $1",
        {tenant}, &missing);
    if (missing) {
        return false;
    }
    for (const auto& rg : rows) {
        if (rg.tenant == tenant && rg.start <= start && rg.end >= end && rg.owner == name_) {
            return true;
        }
    }
    return false;
}

std::wstring Engine::PendingPath(const std::string& move_id) const {
    std::wstring wide;
    wide.reserve(move_id.size());
    for (unsigned char c : move_id) {
        wide.push_back(static_cast<wchar_t>(c));
    }
    return data_dir_ + L"\\pending_" + wide + L".bin";
}

Log* Engine::OpenPendingLocked(const std::string& move_id) {
    auto it = pending_.find(move_id);
    if (it != pending_.end()) {
        return it->second.get();
    }
    auto lg = std::make_unique<Log>(PendingPath(move_id));
    lg->ForEach([&](const Record& rec) { pending_seen_[move_id].insert(rec.position); });
    auto* raw = lg.get();
    pending_.emplace(move_id, std::move(lg));
    return raw;
}

void Engine::InstallLocked(const std::string& move_id) {
    auto path = PendingPath(move_id);
    if (pending_.find(move_id) == pending_.end() && !FileExists(path)) {
        return;
    }
    auto* plog = OpenPendingLocked(move_id);
    plog->ForEach([&](const Record& rec) {
        Record copy = rec;
        copy.position = 0;
        log_.Append(copy);
        Apply(copy);
    });
    log_.Sync();
    DiscardLocked(move_id);
}

void Engine::DiscardLocked(const std::string& move_id) {
    pending_.erase(move_id);
    pending_seen_.erase(move_id);
    RemoveFile(PendingPath(move_id));
}

void Engine::Reconcile() {
    // A fence that died before commit does not come back. The job returns to
    // the snapshot step with its cursors cleared, so the next run copies again.
    bool missing = false;
    Query(
        "UPDATE moves AS m "
        "SET step = 'snapshotting', snapshot_pos = NULL, fence_pos = NULL, "
        "applied_pos = NULL, records_copied = 0 "
        "FROM ranges AS r "
        "WHERE r.tenant_id = m.tenant_id AND r.hash_start = m.hash_start "
        "AND m.step = 'fenced' AND r.owner_shard = m.source "
        "AND (m.source = $1 OR m.destination = $1) "
        "RETURNING m.id::text",
        {name_}, &missing);

    auto names = ListFiles(data_dir_ + L"\\pending_*.bin");
    for (const auto& file_name : names) {
        std::string name;
        name.reserve(file_name.size());
        for (wchar_t c : file_name) {
            name.push_back(static_cast<char>(c));
        }
        const std::string prefix = "pending_";
        const std::string suffix = ".bin";
        if (name.size() <= prefix.size() + suffix.size() || name.compare(0, prefix.size(), prefix) != 0 ||
            name.compare(name.size() - suffix.size(), suffix.size(), suffix) != 0) {
            continue;
        }
        auto move_id = name.substr(prefix.size(), name.size() - prefix.size() - suffix.size());
        bool missing = false;
        std::vector<std::vector<std::string>> rows;
        try {
            rows = Query(
                "SELECT m.destination, r.owner_shard "
                "FROM moves m "
                "JOIN ranges r ON r.tenant_id = m.tenant_id AND r.hash_start = m.hash_start "
                "WHERE m.id = $1::uuid",
                {move_id}, &missing);
        } catch (const std::exception&) {
            throw;
        }
        bool keep = !missing && !rows.empty() && rows[0].size() >= 2 && rows[0][0] == name_ && rows[0][1] == name_;
        if (keep) {
            InstallLocked(move_id);
        } else if (!missing) {
            DiscardLocked(move_id);
        }
    }

    ReloadFencesLocked();
}

void Engine::ReloadFencesLocked() {
    fences_.clear();
    bool missing = false;
    auto fences = Query(
        "SELECT m.tenant_id, m.hash_start::text, m.hash_end::text "
        "FROM moves m "
        "JOIN ranges r ON r.tenant_id = m.tenant_id AND r.hash_start = m.hash_start "
        "WHERE m.source = $1 "
        "AND m.step IN ('fenced', 'committed', 'cleaning') "
        "AND r.owner_shard = $1",
        {name_}, &missing);
    if (missing) {
        return;
    }
    for (const auto& row : fences) {
        if (row.size() < 3) {
            continue;
        }
        AddFence(FenceSpan{row[0], std::stoll(row[1]), std::stoll(row[2])});
    }
}

std::vector<std::vector<std::string>> Engine::Query(const std::string& sql, const std::vector<std::string>& params, bool* missing_table) {
    *missing_table = false;
    auto* pg = static_cast<PGconn*>(pg_);
    if (PQstatus(pg) != CONNECTION_OK) {
        PQreset(pg);
    }
    std::vector<const char*> values;
    values.reserve(params.size());
    for (const auto& p : params) {
        values.push_back(p.c_str());
    }
    PGresult* res = PQexecParams(pg, sql.c_str(), static_cast<int>(params.size()), nullptr,
                                 values.empty() ? nullptr : values.data(), nullptr, nullptr, 0);
    if (res == nullptr) {
        throw std::runtime_error(PQerrorMessage(pg));
    }
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        const char* state = PQresultErrorField(res, PG_DIAG_SQLSTATE);
        if (state != nullptr && std::string(state) == "42P01") {
            *missing_table = true;
            PQclear(res);
            return {};
        }
        std::string msg = PQerrorMessage(pg);
        PQclear(res);
        throw std::runtime_error(msg);
    }
    std::vector<std::vector<std::string>> rows;
    int n = PQntuples(res);
    int cols = PQnfields(res);
    for (int i = 0; i < n; i++) {
        std::vector<std::string> row;
        for (int c = 0; c < cols; c++) {
            if (PQgetisnull(res, i, c)) {
                row.emplace_back();
            } else {
                row.emplace_back(PQgetvalue(res, i, c));
            }
        }
        rows.push_back(std::move(row));
    }
    PQclear(res);
    return rows;
}
