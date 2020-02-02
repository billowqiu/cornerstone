
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
    if (argc < 2)
    {
        std::cout << "Usage: " << argv[0] << " server_address msg_type params" << std::endl;
        return -1;
    }
    std::string server_address(argv[1]);
    asio_svc_ = cs_new<asio_service>();
    ptr<rpc_client> client(asio_svc_->create_client("tcp://"+server_address));

    std::string msg_type(argv[2]);
    if (msg_type == "client_request")
    {
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
            // 发给了非leader，回包的dst为leader_id，重新连接发送
            if (!rsp->get_accepted()) {
                client = asio_svc_->create_client(sstrfmt("tcp://127.0.0.1:900%d").fmt(rsp->get_dst()));
                ptr<req_msg> msg = cs_new<req_msg>(0, msg_type::client_request, 0, 10001/*客户端ID*/, 0, 0, 0);
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
                std::cout << "resp: " << rsp->to_string() << std::endl;
            }
        });

        client->send(msg, handler);
    }
    else if(msg_type == "add_server_request")
    {
        std::cout << "add server request" << std::endl;
        if (argc < 5)
        {
            std::cout << "Usage: " << argv[0] << " server_address msg_type id new_server_address" << std::endl;
            return -1;
        }
        // ptr<async_result<bool>> raft_server::add_srv(const srv_config& srv) {
        int32_t svr_id = atoi(argv[3]);
        std::string new_server_address = argv[4];
        srv_config srv(svr_id, "tcp://"+new_server_address);
        bufptr buf(srv.serialize());

        ptr<log_entry> log(cs_new<log_entry>(0, std::move(buf), log_val_type::cluster_server));
        ptr<req_msg> req(cs_new<req_msg>((ulong)0, msg_type::add_server_request, 0, 0, (ulong)0, (ulong)0, (ulong)0));
        req->log_entries().push_back(log);
        rpc_handler handler = (rpc_handler)([&client](ptr<resp_msg>& rsp, const ptr<rpc_exception>& err) -> void {
            if (err)
                std::cout << "err: " << err->what() << std::endl;
            std::cout << "resp: " << rsp->to_string() << std::endl;
        });
        client->send(req, handler);
    }

    while (true)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(5000));
    }
    return 0;
}