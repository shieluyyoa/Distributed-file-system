#include <algorithm>
#include <regex>
#include <vector>
#include <string>
#include <thread>
#include <cstdio>
#include <chrono>
#include <errno.h>
#include <csignal>
#include <iostream>
#include <sstream>
#include <fstream>
#include <iomanip>
#include <getopt.h>
#include <unistd.h>
#include <limits.h>
#include <sys/inotify.h>
#include <grpcpp/grpcpp.h>

#include "dfslib-shared-p1.h"
#include "dfslib-clientnode-p1.h"
#include "proto-src/dfs-service.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientWriter;
using grpc::Status;
using grpc::StatusCode;

using dfs_service::Chunk;
using dfs_service::DFSService;
using dfs_service::Empty;
using dfs_service::FileDetails;
using dfs_service::FileList;
using dfs_service::FileRequest;
using dfs_service::StoreRequest;

using std::chrono::milliseconds;
using std::chrono::system_clock;
using namespace std;

//
// STUDENT INSTRUCTION:
//
// You may want to add aliases to your namespaced service methods here.
// All of the methods will be under the `dfs_service` namespace.
//
// For example, if you have a method named MyMethod, add
// the following:
//
//      using dfs_service::MyMethod
//

DFSClientNodeP1::DFSClientNodeP1() : DFSClientNode() {}

DFSClientNodeP1::~DFSClientNodeP1() noexcept {}

StatusCode DFSClientNodeP1::Store(const std::string &filename)
{

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to store a file here. This method should
    // connect to your gRPC service implementation method
    // that can accept and store a file.
    //
    // When working with files in gRPC you'll need to stream
    // the file contents, so consider the use of gRPC's ClientWriter.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::NOT_FOUND - if the file cannot be found on the client
    // StatusCode::CANCELLED otherwise
    //
    dfs_log(LL_DEBUG) << "Storing File : " << filename;
    ClientContext context;
    StoreRequest store_request;
    Empty empty;

    context.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::milliseconds(deadline_timeout));

    const string filepath = WrapPath(filename);
    store_request.set_filename(filename);

    std::unique_ptr<ClientWriter<StoreRequest>> writer(service_stub->StoreFile(&context, &empty));
    ifstream file(filepath);

    int file_size = GetFileSize(filepath);
    if (file_size < 0)
    {
        dfs_log(LL_ERROR) << "File not found: " << filepath;
        return StatusCode::NOT_FOUND;
    }

    int bytes_sent = 0;

    dfs_log(LL_DEBUG) << "File size: " << file_size;

    try
    {
        while (bytes_sent < file_size)
        {
            dfs_log(LL_DEBUG) << "Bytes sent: " << bytes_sent;
            char buffer[DFS_BUFFERSIZE];
            int bytes_ready = min(file_size - bytes_sent, DFS_BUFFERSIZE);
            file.read(buffer, bytes_ready);
            store_request.set_chunk(buffer, bytes_ready);
            writer->Write(store_request);
            bytes_sent += file.gcount();
        }

        file.close();
    }
    catch (std::exception &e)
    {
        dfs_log(LL_ERROR) << "Exception: " << e.what();
        return StatusCode::CANCELLED;
    }

    dfs_log(LL_DEBUG) << "Total Bytes sent: " << bytes_sent;
    writer->WritesDone();
    Status status = writer->Finish();
    if (status.ok())
    {
        return StatusCode::OK;
    }

    return status.error_code();
}

StatusCode DFSClientNodeP1::Fetch(const std::string &filename)
{

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to fetch a file here. This method should
    // connect to your gRPC service implementation method
    // that can accept a file request and return the contents
    // of a file from the service.
    //
    // As with the store function, you'll need to stream the
    // contents, so consider the use of gRPC's ClientReader.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::NOT_FOUND - if the file cannot be found on the server
    // StatusCode::CANCELLED otherwise
    //
    //
    dfs_log(LL_DEBUG) << "Fetching File : " << filename;
    ClientContext context;
    FileRequest file_request;
    Chunk chunk;
    string file_path = WrapPath(filename);

    context.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::milliseconds(deadline_timeout));

    file_request.set_filename(filename);
    std::unique_ptr<ClientReader<Chunk>> reader(service_stub->FetchFile(&context, file_request));

    ofstream out(file_path);

    while (reader->Read(&chunk))
    {
        dfs_log(LL_DEBUG) << "Bytes received: " << chunk.chunk().length();

        out << chunk.chunk();
    }

    out.close();

    Status status = reader->Finish();

    if (status.ok())
    {
        return StatusCode::OK;
    }

    return status.error_code();
}

StatusCode DFSClientNodeP1::Delete(const std::string &filename)
{

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to delete a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::NOT_FOUND - if the file cannot be found on the server
    // StatusCode::CANCELLED otherwise
    //
    dfs_log(LL_DEBUG) << "Deleting File : " << filename;
    ClientContext context;
    Empty empty;
    FileRequest file_request;

    context.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::milliseconds(deadline_timeout));

    file_request.set_filename(filename);

    Status status = service_stub->DeleteFile(&context, file_request, &empty);
    if (status.ok())
    {
        return StatusCode::OK;
    }

    return status.error_code();
}

StatusCode DFSClientNodeP1::List(std::map<std::string, int> *file_map, bool display)
{

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to list all files here. This method
    // should connect to your service's list method and return
    // a list of files using the message type you created.
    //
    // The file_map parameter is a simple map of files. You should fill
    // the file_map with the list of files you receive with keys as the
    // file name and values as the modified time (mtime) of the file
    // received from the server.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::CANCELLED otherwise
    //
    //

    dfs_log(LL_DEBUG) << "Listing Files";
    ClientContext context;
    Empty empty;
    FileList file_list;

    context.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::milliseconds(deadline_timeout));

    Status status = service_stub->ListFile(&context, empty, &file_list);
    if (status.ok())
    {
        for (const auto &stat : file_list.file_status())
        {
            file_map->insert(pair<string, int>(stat.first, stat.second));
            if (display)
            {
                dfs_log(LL_DEBUG) << "File: " << stat.first << " Mtime: " << stat.second;
            }
        }
        return StatusCode::OK;
    }

    return status.error_code();
}

StatusCode DFSClientNodeP1::Stat(const std::string &filename, void *file_status)
{

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to get the status of a file here. This method should
    // retrieve the status of a file on the server. Note that you won't be
    // tested on this method, but you will most likely find that you need
    // a way to get the status of a file in order to synchronize later.
    //
    // The status might include data such as name, size, mtime, crc, etc.
    //
    // The file_status is left as a void* so that you can use it to pass
    // a message type that you defined. For instance, may want to use that message
    // type after calling Stat from another method.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::NOT_FOUND - if the file cannot be found on the server
    // StatusCode::CANCELLED otherwise
    //
    //

    dfs_log(LL_DEBUG) << "Getting File Status";
    ClientContext context;
    FileRequest file_request;
    FileDetails file_details;

    context.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::milliseconds(deadline_timeout));

    file_request.set_filename(filename);

    Status status = service_stub->FileStat(&context, file_request, &file_details);

    if (status.ok())
    {
        dfs_log(LL_DEBUG) << "File: " << file_details.size() << " Mtime: " << file_details.mtime();
        return StatusCode::OK;
    }

    file_status = &file_details;

    return status.error_code();
}

//
// STUDENT INSTRUCTION:
//
// Add your additional code here, including
// implementations of your client methods
//
