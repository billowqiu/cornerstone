/**
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  The ASF licenses
* this file to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

#include "../include/cornerstone.hxx"
#include <iostream>
#include <cassert>

using namespace cornerstone;

extern void cleanup(const std::string& folder);

#ifdef _WIN32
extern int mkdir(const char* path, int mode);
extern int rmdir(const char* path);
#else
#define LOG_INDEX_FILE "/store.idx"
#define LOG_DATA_FILE "/store.dat"
#define LOG_START_INDEX_FILE "/store.sti"
#define LOG_INDEX_FILE_BAK "/store.idx.bak"
#define LOG_DATA_FILE_BAK "/store.dat.bak"
#define LOG_START_INDEX_FILE_BAK "/store.sti.bak"

#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#endif

ptr<asio_service> asio_svc_;
// std::condition_variable stop_cv1;
// std::mutex lock1;
// std::mutex stop_test_lock1;
// std::condition_variable stop_test_cv1;

class simple_state_mgr: public state_mgr{
public:
    simple_state_mgr(int32 srv_id)
        : srv_id_(srv_id) {
        store_path_ = sstrfmt("store%d").fmt(srv_id_);
    }

public:
    virtual ptr<cluster_config> load_config() {
        ptr<cluster_config> conf = cs_new<cluster_config>();
        conf->get_servers().push_back(cs_new<srv_config>(1, "tcp://127.0.0.1:9001"));
        conf->get_servers().push_back(cs_new<srv_config>(2, "tcp://127.0.0.1:9002"));
        conf->get_servers().push_back(cs_new<srv_config>(3, "tcp://127.0.0.1:9003"));
        return conf;
    }

    virtual void save_config(const cluster_config&) {}
    virtual void save_state(const srv_state&) {}
    virtual ptr<srv_state> read_state() {
        return cs_new<srv_state>();
    }

    virtual ptr<log_store> load_log_store() {
        mkdir(store_path_.c_str(), 0766);
        return cs_new<fs_log_store>(store_path_);
    }

    virtual int32 server_id() {
        return srv_id_;
    }

    virtual void system_exit(const int exit_code) {
        std::cout << "system exiting with code " << exit_code << std::endl;
    }

private:
    int32 srv_id_;
    std::string store_path_;
};

class console_logger : public logger {
public:
    console_logger(const std::string& name) : name_(name) {}

    __nocopy__(console_logger)
public:
    virtual void debug(const std::string& log_line) {
        printf("%s %s %s\n", "DEBUG", this->name_.c_str(), log_line.c_str());
    }

    virtual void info(const std::string& log_line) {
        printf("%s %s %s\n", "INFO", this->name_.c_str(), log_line.c_str());
    }

    virtual void warn(const std::string& log_line) {
        printf("%s %s %s\n", "WARN", this->name_.c_str(), log_line.c_str());
    }

    virtual void err(const std::string& log_line) {
        printf("%s %s %s\n", "ERROR", this->name_.c_str(), log_line.c_str());
    }

private:
    std::string name_;
};

class echo_state_machine : public state_machine {
public:
    echo_state_machine() : lock_() {}
public:
    virtual void commit(const ulong, buffer& data, const uptr<log_entry_cookie>&) {
        auto_lock(lock_);
        std::cout << "commit message:" << reinterpret_cast<const char*>(data.data()) << std::endl;
    }

    virtual void pre_commit(const ulong, buffer& data, const uptr<log_entry_cookie>&) {
        auto_lock(lock_);
        std::cout << "pre-commit: " << reinterpret_cast<const char*>(data.data()) << std::endl;
    }

    virtual void rollback(const ulong, buffer& data, const uptr<log_entry_cookie>&) {
        auto_lock(lock_);
        std::cout << "rollback: " << reinterpret_cast<const char*>(data.data()) << std::endl;
    }

    virtual void save_snapshot_data(snapshot&, const ulong, buffer&) {}
    virtual bool apply_snapshot(snapshot&) {
        return false;
    }

    virtual int read_snapshot_data(snapshot& , const ulong, buffer&) {
        return 0;
    }

    virtual ulong last_commit_index() {
        return 0;
    }

    virtual ptr<snapshot> last_snapshot() {
        return ptr<snapshot>();
    }

    virtual void create_snapshot(snapshot&, async_result<bool>::handler_type&) {}
private:
    std::mutex lock_;
};

class test_event_listener: public raft_event_listener {
public:
    test_event_listener(int id) : raft_event_listener(), srv_id_(id){}

public:
    virtual void on_event(raft_event event) override {
        switch (event)
        {
        case raft_event::become_follower:
            std::cout << srv_id_ << " becomes a follower" << std::endl;
            break;
        case raft_event::become_leader:
            std::cout << srv_id_ << " becomes a leader" << std::endl;
            break;
        case raft_event::logs_catch_up:
            std::cout << srv_id_ << " catch up all logs" << std::endl;
            break;
        }
    }

private:
    int srv_id_;
};

int main(int argc, const char* argv[]) 
{
    if (argc != 2)
    {
        std::cout << "usage: " << argv[0] << " server_id\n";
        return 0;
    }
    int srv_id = atoi(argv[1]);
    
    asio_svc_ = cs_new<asio_service>();

    std::cout << "waiting for leader election..." << std::endl;

    ptr<logger> l(asio_svc_->create_logger(asio_service::log_level::debug, sstrfmt("log%d.log").fmt(srv_id)));
    ptr<rpc_listener> listener(asio_svc_->create_rpc_listener((ushort)(9000 + srv_id), l));
    ptr<state_mgr> smgr(cs_new<simple_state_mgr>(srv_id));
    ptr<state_machine> smachine(cs_new<echo_state_machine>());
    raft_params* params(new raft_params());
    (*params).with_election_timeout_lower(200)
        .with_election_timeout_upper(400)
        .with_hb_interval(100)
        .with_max_append_size(100)
        .with_rpc_failure_backoff(50);
    ptr<delayed_task_scheduler> scheduler = asio_svc_;
    ptr<rpc_client_factory> rpc_cli_factory = asio_svc_;
    context* ctx(new context(smgr, smachine, listener, l, rpc_cli_factory, scheduler, cs_new<test_event_listener>(srv_id), params));
    ptr<raft_server> server(cs_new<raft_server>(ctx));
    listener->listen(server);    
    while (true)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(5000));
    }

    return 0;
}


