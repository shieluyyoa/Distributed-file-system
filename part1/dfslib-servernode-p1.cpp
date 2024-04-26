#include <map>
#include <chrono>
#include <cstdio>
#include <string>
#include <thread>
#include <errno.h>
#include <iostream>
#include <fstream>
#include <getopt.h>
#include <dirent.h>
#include <sys/stat.h>
#include <grpcpp/grpcpp.h>

#include "src/dfs-utils.h"
#include "dfslib-shared-p1.h"
#include "dfslib-servernode-p1.h"
#include "proto-src/dfs-service.grpc.pb.h"

using grpc::Status;
using grpc::Server;
using grpc::StatusCode;
using grpc::ServerReader;
using grpc::ServerWriter;
using grpc::ServerContext;
using grpc::ServerBuilder;

using dfs_service::DFSService;
using dfs_service::StoreRequest;
using dfs_service::FileRequest;
using dfs_service::FileList;
using dfs_service::FileDetails;
using dfs_service::Chunk;
using dfs_service::Empty;

using namespace std;


//
// STUDENT INSTRUCTION:
//
// DFSServiceImpl is the implementation service for the rpc methods
// and message types you defined in the `dfs-service.proto` file.
//
// You should add your definition overrides here for the specific
// methods that you created for your GRPC service protocol. The
// gRPC tutorial described in the readme is a good place to get started
// when trying to understand how to implement this class.
//
// The method signatures generated can be found in `proto-src/dfs-service.grpc.pb.h` file.
//
// Look for the following section:
//
//      class Service : public ::grpc::Service {
//
// The methods returning grpc::Status are the methods you'll want to override.
//
// In C++, you'll want to use the `override` directive as well. For example,
// if you have a service method named MyMethod that takes a MyMessageType
// and a ServerWriter, you'll want to override it similar to the following:
//
//      Status MyMethod(ServerContext* context,
//                      const MyMessageType* request,
//                      ServerWriter<MySegmentType> *writer) override {
//
//          /** code implementation here **/
//      }
//
class DFSServiceImpl final : public DFSService::Service {

private:

    /** The mount path for the server **/
    std::string mount_path;

    /**
     * Prepend the mount path to the filename.
     *
     * @param filepath
     * @return
     */
    const std::string WrapPath(const std::string &filepath) {
        return this->mount_path + filepath;
    }


public:

    DFSServiceImpl(const std::string &mount_path): mount_path(mount_path) {
    }

    ~DFSServiceImpl() {}

    //
    // STUDENT INSTRUCTION:
    //
    // Add your additional code here, including
    // implementations of your protocol service methods
    //
    Status StoreFile(ServerContext* context, ServerReader<StoreRequest>* reader, dfs_service::Empty* response) override {
        StoreRequest store_request;

        ofstream out;

        try {

            while (reader->Read(&store_request)) {

                if (!out.is_open()) {
                    dfs_log(LL_DEBUG) << "Storing File : " << store_request.filename();
                    std::string filepath = WrapPath(store_request.filename());
                    out.open(filepath);
                }

                if (context->IsCancelled()) {
                    return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded or Client cancelled, abandoning.");
                }
                
                dfs_log(LL_DEBUG) << "Bytes received: " << sizeof(store_request.chunk());
                out << store_request.chunk();
            }

            out.close();

        } catch (std::exception& e) {
            dfs_log(LL_ERROR) << "Exception: " << e.what();
            return Status(StatusCode::CANCELLED, e.what());
        }

        return Status::OK;

    };

    Status FetchFile(ServerContext* context, const dfs_service::FileRequest* request, ServerWriter<Chunk>* writer) override {
        dfs_log(LL_DEBUG) << "Fetching File : " << request->filename();

        Chunk chunk;
        string file_path = WrapPath(request->filename());

        ifstream file(file_path);
        int file_size = GetFileSize(file_path);
        if (file_size < 0) {
            dfs_log(LL_ERROR) << "File not found: " << file_path;
            return Status(StatusCode::NOT_FOUND, "File not found");
        }

        int bytes_sent = 0;
        try {
            while (bytes_sent < file_size) {
                dfs_log(LL_DEBUG) << "Bytes sent: " << bytes_sent;

                if (context->IsCancelled()) {
                    return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded or Client cancelled, abandoning.");
                }
                
                char buffer[DFS_BUFFERSIZE];
                int bytes_ready = min(file_size - bytes_sent, DFS_BUFFERSIZE);
                file.read(buffer, bytes_ready);
                chunk.set_chunk(buffer, bytes_ready);
                writer->Write(chunk);
                bytes_sent += file.gcount();
            }

            file.close();
        } catch (std::exception& e) {
            dfs_log(LL_ERROR) << "Exception: " << e.what();
            return Status(StatusCode::CANCELLED, e.what());
        }

        dfs_log(LL_DEBUG) << "Total Bytes sent: " << bytes_sent;

        return Status::OK;

    }


    Status ListFile(ServerContext* context, const dfs_service::Empty* request, FileList* response) override {
        dfs_log(LL_DEBUG) << "Listing Files";
        DIR *dir;
        struct dirent *ent;

        dir = opendir(this->mount_path.c_str());
        if (dir == NULL) {
            dfs_log(LL_ERROR) << "Unable to open directory";
            return Status(StatusCode::CANCELLED, "Unable to open directory");
        }

        while ((ent = readdir(dir))!= NULL) {
            if (context->IsCancelled()) {
                return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded or Client cancelled, abandoning.");
            }

            if (ent->d_type == DT_REG) {
                struct stat result;
                const string file_path = WrapPath(ent->d_name);
                if (stat(file_path.c_str(), &result) == 0) {
                    // string filename(ent->d_name);
                    dfs_log(LL_DEBUG) << "File: " << ent->d_name << " Size: " << result.st_size;
                    (*response->mutable_file_status())[ent->d_name] = result.st_mtime;
                }
            }
        }

        closedir(dir);

        return Status::OK;
    }

    Status DeleteFile(ServerContext* context, const dfs_service::FileRequest* request, dfs_service::Empty* response) override {
        dfs_log(LL_DEBUG) << "Deleting File : " << request->filename();
        if (context->IsCancelled()) {
            return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded or Client cancelled, abandoning.");
        }

        struct stat result;
        string file_path = WrapPath(request->filename());

        if (stat(file_path.c_str(), &result)!= 0) {
            return Status(StatusCode::NOT_FOUND, "File not found");
        }

        if (remove(file_path.c_str())!= 0) {
            return Status(StatusCode::CANCELLED, "Unable to delete file");
        }

        return Status::OK;


    }

    Status FileStat(ServerContext* context, const dfs_service::FileRequest* request, dfs_service::FileDetails* response) override {
        dfs_log(LL_DEBUG) << "File Status : " << request->filename();
        if (context->IsCancelled()) {
            return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded or Client cancelled, abandoning.");
        }

        struct stat result;
        string file_path = WrapPath(request->filename());
        if (stat(file_path.c_str(), &result) == 0) {
            response->set_size(result.st_size);
            response->set_mtime(result.st_mtime);
            return Status::OK;
        } else {
            return Status(StatusCode::NOT_FOUND, "File not found");
        }



    }
};

//
// STUDENT INSTRUCTION:
//
// The following three methods are part of the basic DFSServerNode
// structure. You may add additional methods or change these slightly,
// but be aware that the testing environment is expecting these three
// methods as-is.
//
/**
 * The main server node constructor
 *
 * @param server_address
 * @param mount_path
 */
DFSServerNode::DFSServerNode(const std::string &server_address,
        const std::string &mount_path,
        std::function<void()> callback) :
    server_address(server_address), mount_path(mount_path), grader_callback(callback) {}

/**
 * Server shutdown
 */
DFSServerNode::~DFSServerNode() noexcept {
    dfs_log(LL_SYSINFO) << "DFSServerNode shutting down";
    this->server->Shutdown();
}

/** Server start **/
void DFSServerNode::Start() {
    DFSServiceImpl service(this->mount_path);
    ServerBuilder builder;
    builder.AddListeningPort(this->server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    this->server = builder.BuildAndStart();
    dfs_log(LL_SYSINFO) << "DFSServerNode server listening on " << this->server_address;
    this->server->Wait();
}

//
// STUDENT INSTRUCTION:
//
// Add your additional DFSServerNode definitions here
//
