
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

int main(int argc, const char* argv[]) 
{
    asio_svc_ = cs_new<asio_service>();

    ptr<rpc_client> client(asio_svc_->create_client("tcp://127.0.0.1:9001"));
    ptr<req_msg> msg = cs_new<req_msg>(0, msg_type::client_request, 0, 1, 0, 0, 0);
    bufptr buf = buffer::alloc(100);
    buf->put("hello");
    buf->pos(0);
    msg->log_entries().push_back(cs_new<log_entry>(0, std::move(buf)));
    rpc_handler handler = (rpc_handler)([&client](ptr<resp_msg>& rsp, const ptr<rpc_exception>& err) -> void {
        if (err) {
            std::cout << err->what() << std::endl;
        }

        assert(!err);
        assert(rsp->get_accepted() || rsp->get_dst() > 0);
        if (!rsp->get_accepted()) {
            client = asio_svc_->create_client(sstrfmt("tcp://127.0.0.1:900%d").fmt(rsp->get_dst()));
            ptr<req_msg> msg = cs_new<req_msg>(0, msg_type::client_request, 0, 1, 0, 0, 0);
            bufptr buf = buffer::alloc(100);
            buf->put("hello");
            buf->pos(0);
            msg->log_entries().push_back(cs_new<log_entry>(0, std::move(buf)));
            rpc_handler handler = (rpc_handler)([client](ptr<resp_msg>& rsp1, const ptr<rpc_exception>&) -> void {
                assert(rsp1->get_accepted());
            });
            client->send(msg, handler);
        }
        else {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
    });

    client->send(msg, handler);
    std::cout << "client sent... \n";
    while (true)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(5000));
    }
    return 0;
}