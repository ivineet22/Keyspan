#include "engine.hpp"
#include "hash.hpp"

#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <shellapi.h>

void Serve(Engine& engine, const std::string& listen);

namespace {

std::string Utf8(const std::wstring& w) {
    if (w.empty()) {
        return {};
    }
    int n = WideCharToMultiByte(CP_UTF8, 0, w.data(), static_cast<int>(w.size()), nullptr, 0, nullptr, nullptr);
    std::string out(n, '\0');
    WideCharToMultiByte(CP_UTF8, 0, w.data(), static_cast<int>(w.size()), out.data(), n, nullptr, nullptr);
    return out;
}

std::wstring ArgValue(const std::vector<std::wstring>& args, const std::wstring& flag) {
    for (std::size_t i = 0; i + 1 < args.size(); i++) {
        if (args[i] == flag) {
            return args[i + 1];
        }
    }
    return {};
}

void PrepareLibpq() {
    wchar_t buf[MAX_PATH];
    DWORD n = GetEnvironmentVariableW(L"POSTGRES_BIN", buf, MAX_PATH);
    if (n > 0 && n < MAX_PATH) {
        SetDllDirectoryW(buf);
        return;
    }
    SetDllDirectoryW(L"C:\\Program Files\\PostgreSQL\\17\\bin");
}

}  // namespace

int main() {
    if (KeyHash("acme", "user-1") != kKnownHash) {
        std::cerr << "hash function does not match the Go router\n";
        return 1;
    }
    PrepareLibpq();

    int argc = 0;
    wchar_t** argv = CommandLineToArgvW(GetCommandLineW(), &argc);
    std::vector<std::wstring> args;
    for (int i = 0; i < argc; i++) {
        args.emplace_back(argv[i]);
    }
    LocalFree(argv);

    auto name = ArgValue(args, L"--name");
    auto listen = ArgValue(args, L"--listen");
    auto data = ArgValue(args, L"--data");
    auto database = ArgValue(args, L"--database-url");
    if (name.empty() || listen.empty() || data.empty() || database.empty()) {
        std::cerr << "usage: shard --name shard-1 --listen 127.0.0.1:50051 --data DIR --database-url URL\n";
        return 1;
    }
    CreateDirectoryW(data.c_str(), nullptr);

    try {
        Engine engine(Utf8(name), data, Utf8(database));
        engine.Recover();
        std::cerr << Utf8(name) << " listening on " << Utf8(listen) << "\n";
        Serve(engine, Utf8(listen));
    } catch (const std::exception& ex) {
        std::cerr << ex.what() << "\n";
        return 1;
    }
    return 0;
}
