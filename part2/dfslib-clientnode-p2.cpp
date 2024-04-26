#include <regex>
#include <mutex>
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
#include <utime.h>

#include "src/dfs-utils.h"
#include "src/dfslibx-clientnode-p2.h"
#include "dfslib-shared-p2.h"
#include "dfslib-clientnode-p2.h"
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
using dfs_service::StoreResponse;

using namespace std;

extern dfs_log_level_e DFS_LOG_LEVEL;

//
// STUDENT INSTRUCTION:
//
// Change these "using" aliases to the specific
// message types you are using to indicate
// a file request and a listing of files from the server.
//
using FileRequestType = FileRequest;
using FileListResponseType = FileList;

DFSClientNodeP2::DFSClientNodeP2() : DFSClientNode() {}
DFSClientNodeP2::~DFSClientNodeP2() {}

grpc::StatusCode DFSClientNodeP2::RequestWriteAccess(const std::string &filename)
{

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to obtain a write lock here when trying to store a file.
    // This method should request a write lock for the given file at the server,
    // so that the current client becomes the sole creator/writer. If the server
    // responds with a RESOURCE_EXHAUSTED response, the client should cancel
    // the current file storage
    //
    // The StatusCode response should be:
    //
    // OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::RESOURCE_EXHAUSTED - if a write lock cannot be obtained
    // StatusCode::CANCELLED otherwise
    //
    //
    dfs_log(LL_DEBUG) << "RequestWriteAccess: " << filename;
    ClientContext context;
    FileRequest file_request;
    Empty empty;

    context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout));

    file_request.set_name(filename);
    file_request.set_client_id(client_id);

    Status status = service_stub->RequestWriteLock(&context, file_request, &empty);

    if (status.ok())
    {
        dfs_log(LL_DEBUG) << "RequestWriteAccess: " << filename << " OK";
        return StatusCode::OK;
    }

    return status.error_code();
}

grpc::StatusCode DFSClientNodeP2::Store(const std::string &filename)
{

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to store a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You can start with your Part 1 implementation. However, you will
    // need to adjust this method to recognize when a file trying to be
    // stored is the same on the server (i.e. the ALREADY_EXISTS gRPC response).
    //
    // You will also need to add a request for a write lock before attempting to store.
    //
    // If the write lock request fails, you should return a status of RESOURCE_EXHAUSTED
    // and cancel the current operation.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::ALREADY_EXISTS - if the local cached file has not changed from the server version
    // StatusCode::RESOURCE_EXHAUSTED - if a write lock cannot be obtained
    // StatusCode::CANCELLED otherwise
    //
    //

    dfs_log(LL_DEBUG) << "Storing File: " << filename;
    ClientContext context;
    StoreRequest store_request;
    StoreResponse store_response;

    context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout));

    // Acquire write lock
    StatusCode write_lock_status = RequestWriteAccess(filename);
    if (write_lock_status != StatusCode::OK)
    {
        return StatusCode::RESOURCE_EXHAUSTED;
    }

    // Read file
    const string filepath = WrapPath(filename);
    uint32_t crc = dfs_file_checksum(filepath, &(this->crc_table));
    context.AddMetadata("filename", filename);
    context.AddMetadata("checksum", to_string(crc));
    context.AddMetadata("client_id", client_id);

    std::unique_ptr<ClientWriter<StoreRequest>> writer(service_stub->StoreFile(&context, &store_response));
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
            bytes_sent += bytes_ready;
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
    if (!status.ok())
    {
        if (status.error_code() != StatusCode::ALREADY_EXISTS)
        {
            return status.error_code();
        }
    }

    // Match local file mtime with server file mtime
    struct stat st;

    if (stat(filepath.c_str(), &st) != 0)
    {
        dfs_log(LL_ERROR) << "File not found: " << filepath;
        return StatusCode::NOT_FOUND;
    }
    
    dfs_log(LL_DEBUG) << "Original mtime :" << st.st_mtime;

    struct utimbuf newtimes;
    newtimes.actime = st.st_atime;
    newtimes.modtime = store_response.mtime();

    if (utime(filepath.c_str(), &newtimes) != 0)
    {
        dfs_log(LL_ERROR) << "Failed to set mtime: " << filepath;
        return StatusCode::CANCELLED;
    }

    // log mtime of the file
    dfs_log(LL_DEBUG) << "Response mtime: " << store_response.mtime();
    dfs_log(LL_DEBUG) << "Local mtime: " << st.st_mtime;

    return status.ok() ? StatusCode::OK : StatusCode::ALREADY_EXISTS;
}

grpc::StatusCode DFSClientNodeP2::Fetch(const std::string &filename)
{

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to fetch a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You can start with your Part 1 implementation. However, you will
    // need to adjust this method to recognize when a file trying to be
    // fetched is the same on the client (i.e. the files do not differ
    // between the client and server and a fetch would be unnecessary.
    //
    // The StatusCode response should be:
    //
    // OK - if all went well
    // DEADLINE_EXCEEDED - if the deadline timeout occurs
    // NOT_FOUND - if the file cannot be found on the server
    // ALREADY_EXISTS - if the local cached file has not changed from the server version
    // CANCELLED otherwise
    //
    // Hint: You may want to match the mtime on local files to the server's mtime
    //
    dfs_log(LL_DEBUG) << "Fetching File : " << filename;
    ClientContext context;
    FileRequest file_request;
    Chunk chunk;
    string file_path = WrapPath(filename);

    context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout));
    
    // Check if file exists on client
    struct stat st;
    if (stat(file_path.c_str(), &st) == 0)
    {
        // Get file checksum from server
        FileDetails file_details;
        uint32_t crc = dfs_file_checksum(file_path, &(this->crc_table));
        StatusCode file_status = Stat(filename, static_cast<void*>(&file_details));
        if (file_status != StatusCode::OK)
        {
            return file_status;
        }

        // compare file checksum 
        if (crc == file_details.checksum())
        {
            dfs_log(LL_DEBUG) << "File already exists on client: " << filename;
            // if client file mtime does not match with server file mtime, align it
            if (st.st_mtime != file_details.mtime())
            {
                dfs_log(LL_DEBUG) << "File has changed on server: " << filename;
                // align mtime
                struct utimbuf newtimes;
                newtimes.actime = st.st_atime;
                newtimes.modtime = file_details.mtime();

                if (utime(file_path.c_str(), &newtimes) != 0)
                {
                    dfs_log(LL_ERROR) << "Failed to set mtime: " << filename;
                    return StatusCode::CANCELLED;
                }
                
            }

            return StatusCode::ALREADY_EXISTS;
        }
    }
    
    file_request.set_name(filename);
    file_request.set_client_id(client_id);
    std::unique_ptr<ClientReader<Chunk>> reader(service_stub->FetchFile(&context, file_request));

    ofstream out(file_path);

    while (reader->Read(&chunk))
    {
        dfs_log(LL_DEBUG) << "Bytes received: " << chunk.chunk().length();
        out << chunk.chunk();
    }

    out.close();

    Status status = reader->Finish();

    if (!status.ok())
    {
        return status.error_code();
    }

    // match file mtime to server mtime
    if (stat(file_path.c_str(), &st) != 0)
    {
        dfs_log(LL_ERROR) << "File not found: " << file_path;
        return StatusCode::NOT_FOUND;
    }

    struct utimbuf newtimes;
    newtimes.actime = st.st_atime;
    newtimes.modtime = chunk.mtime();

    if (utime(file_path.c_str(), &newtimes) != 0)
    {
        dfs_log(LL_ERROR) << "Failed to set mtime: " << file_path;
        return StatusCode::CANCELLED;
    }

    return StatusCode::OK;
}

grpc::StatusCode DFSClientNodeP2::Delete(const std::string &filename)
{

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to delete a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You will also need to add a request for a write lock before attempting to delete.
    //
    // If the write lock request fails, you should return a status of RESOURCE_EXHAUSTED
    // and cancel the current operation.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::RESOURCE_EXHAUSTED - if a write lock cannot be obtained
    // StatusCode::CANCELLED otherwise
    //
    //

    dfs_log(LL_DEBUG) << "Deleting File : " << filename;
    ClientContext context;
    Empty empty;
    FileRequest file_request;

    context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout));
    
    file_request.set_name(filename);
    file_request.set_client_id(client_id);

    StatusCode write_lock_status = RequestWriteAccess(filename);

    if (write_lock_status != StatusCode::OK)
    {
        return StatusCode::RESOURCE_EXHAUSTED;
    }

    Status status = service_stub->DeleteFile(&context, file_request, &empty);

    if (status.ok())
    {
        return StatusCode::OK;
    }

    return status.error_code();

}

grpc::StatusCode DFSClientNodeP2::List(std::map<std::string, int> *file_map, bool display)
{

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to list files here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You can start with your Part 1 implementation and add any additional
    // listing details that would be useful to your solution to the list response.
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
    FileRequest file_request;
    FileList file_list;

    context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout));

    Status status = service_stub->ListFiles(&context, file_request, &file_list);

    if (!status.ok())
    {
        return status.error_code();
    }

    for (const auto &file : file_list.file_details())
    {
        file_map->insert(pair<string, int>(file.name(), file.mtime()));
        if (display)
        {
            dfs_log(LL_DEBUG) << "File: " << file.name() << " Mtime: " << file.mtime();

            // log checksum
            dfs_log(LL_DEBUG) << "Checksum: " << file.checksum();
        }
    }

    return StatusCode::OK;

}

grpc::StatusCode DFSClientNodeP2::Stat(const std::string &filename, void *file_status)
{

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to get the status of a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You can start with your Part 1 implementation and add any additional
    // status details that would be useful to your solution.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::NOT_FOUND - if the file cannot be found on the server
    // StatusCode::CANCELLED otherwise
    //
    //

    dfs_log(LL_DEBUG) << "Getting File Status : " << filename;
    ClientContext context;
    FileRequest file_request;
    FileDetails file_details;

    context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout));

    file_request.set_name(filename);

    Status status = service_stub->GetFileStat(&context, file_request, &file_details);

    if (!status.ok())
    {
        return status.error_code();
    }

    FileDetails *updated_file_details = static_cast<FileDetails*>(file_status);
    updated_file_details->set_name(file_details.name());
    updated_file_details->set_mtime(file_details.mtime());
    updated_file_details->set_ctime(file_details.ctime());
    updated_file_details->set_checksum(file_details.checksum());
    

    return StatusCode::OK;

}

void DFSClientNodeP2::InotifyWatcherCallback(std::function<void()> callback)
{

    //
    // STUDENT INSTRUCTION:
    //
    // This method gets called each time inotify signals a change
    // to a file on the file system. That is every time a file is
    // modified or created.
    //
    // You may want to consider how this section will affect
    // concurrent actions between the inotify watcher and the
    // asynchronous callbacks associated with the server.
    //
    // The callback method shown must be called here, but you may surround it with
    // whatever structures you feel are necessary to ensure proper coordination
    // between the async and watcher threads.
    //
    // Hint: how can you prevent race conditions between this thread and
    // the async thread when a file event has been signaled?
    //
    file_directory_mutex.lock();
    callback();
    file_directory_mutex.unlock();
}

//
// STUDENT INSTRUCTION:
//
// This method handles the gRPC asynchronous callbacks from the server.
// We've provided the base structure for you, but you should review
// the hints provided in the STUDENT INSTRUCTION sections below
// in order to complete this method.
//
void DFSClientNodeP2::HandleCallbackList()
{

    void *tag;

    bool ok = false;

    //
    // STUDENT INSTRUCTION:
    //
    // Add your file list synchronization code here.
    //
    // When the server responds to an asynchronous request for the CallbackList,
    // this method is called. You should then synchronize the
    // files between the server and the client based on the goals
    // described in the readme.
    //
    // In addition to synchronizing the files, you'll also need to ensure
    // that the async thread and the file watcher thread are cooperating. These
    // two threads could easily get into a race condition where both are trying
    // to write or fetch over top of each other. So, you'll need to determine
    // what type of locking/guarding is necessary to ensure the threads are
    // properly coordinated.
    //

    // Block until the next result is available in the completion queue.
    while (completion_queue.Next(&tag, &ok))
    {
        {
            //
            // STUDENT INSTRUCTION:
            //
            // Consider adding a critical section or RAII style lock here
            //

            // The tag is the memory location of the call_data object
            AsyncClientData<FileListResponseType> *call_data = static_cast<AsyncClientData<FileListResponseType> *>(tag);

            dfs_log(LL_DEBUG2) << "Received completion queue callback";

            // Verify that the request was completed successfully. Note that "ok"
            // corresponds solely to the request for updates introduced by Finish().
            // GPR_ASSERT(ok);
            if (!ok)
            {
                dfs_log(LL_ERROR) << "Completion queue callback not ok.";
            }

            if (ok && call_data->status.ok())
            {

                dfs_log(LL_DEBUG3) << "Handling async callback ";

                //
                // STUDENT INSTRUCTION:
                //
                // Add your handling of the asynchronous event calls here.
                // For example, based on the file listing returned from the server,
                // how should the client respond to this updated information?
                // Should it retrieve an updated version of the file?
                // Send an update to the server?
                // Do nothing?
                //

                file_directory_mutex.lock();

                // iterate through the returned file list
                set<string> server_file_list;
                FileListResponseType *file_list = &call_data->reply;
                for (const auto &server_file : file_list->file_details()) {
                    // check if the file exists in the client
                    struct stat st;
                    string filename = server_file.name();
                    string file_path = WrapPath(filename);

                    // add file name to file list set
                    server_file_list.insert(filename);

                    if (stat(file_path.c_str(), &st) != 0) {
                        // fetch file from server
                        Fetch(filename);
                        continue;
                    }

                    // check if the file is modified

                    // client checksum == server checksum, align mtime and contine
                    uint32_t client_checksum = dfs_file_checksum(file_path, &(this->crc_table));
                    if (server_file.checksum() == client_checksum) {
  
                        // if mtime matches, continue
                        if (server_file.mtime() == st.st_mtime) {
                            continue;
                        }
                        
                        struct utimbuf newtimes;
                        newtimes.actime = st.st_atime;
                        newtimes.modtime = server_file.mtime();

                        if (utime(file_path.c_str(), &newtimes) != 0)
                        {
                            dfs_log(LL_ERROR) << "Failed to set mtime: " << file_path;
                            
                        }
                        continue;
                    }

                    // if client checksum != server checksum, compare mtime

                    // client mtime > server mtime, Store
                    if (st.st_mtime > server_file.mtime()) {
                        Store(filename);
                        continue;
                    }
                    // client mtime < server mtime, Fetch
                    if (st.st_mtime < server_file.mtime()) {
                        Fetch(filename);
                        continue;
                    }



                }

                // Check if client has any files that are not on the server and delete it
                set<string> client_file_list = this->GetLocalFileList();

                set<string> file_list_to_delete;

                set_difference(client_file_list.begin(), client_file_list.end(), server_file_list.begin(), server_file_list.end(), inserter(file_list_to_delete, file_list_to_delete.end()));

                for (const auto &file_name : file_list_to_delete) {
                    string file_path = WrapPath(file_name);
                    if (remove(file_path.c_str())!= 0) {
                        dfs_log(LL_ERROR) << "Failed to delete file: " << file_path;
                    }
                }


                file_directory_mutex.unlock();
            }
            else
            {
                dfs_log(LL_ERROR) << "Status was not ok. Will try again in " << DFS_RESET_TIMEOUT << " milliseconds.";
                dfs_log(LL_ERROR) << call_data->status.error_message();
                std::this_thread::sleep_for(std::chrono::milliseconds(DFS_RESET_TIMEOUT));
            }

            // Once we're complete, deallocate the call_data object.
            delete call_data;

            //
            // STUDENT INSTRUCTION:
            //
            // Add any additional syncing/locking mechanisms you may need here
        }

        // Start the process over and wait for the next callback 
        auto start = std::chrono::high_resolution_clock::now();

        std::this_thread::sleep_for(std::chrono::milliseconds(10000));

        auto end = std::chrono::high_resolution_clock::now();

        auto time_waited = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

        dfs_log(LL_DEBUG) << "Time waited: " << time_waited.count();

        dfs_log(LL_DEBUG) << "Calling InitCallbackList";
        // sleep for 5000 milliseconds before calling InitCallbackList again
        InitCallbackList();
    }
}

/**
 * This method will start the callback request to the server, requesting
 * an update whenever the server sees that files have been modified.
 *
 * We're making use of a template function here, so that we can keep some
 * of the more intricate workings of the async process out of the way, and
 * give you a chance to focus more on the project's requirements.
 */
void DFSClientNodeP2::InitCallbackList()
{
    CallbackList<FileRequestType, FileListResponseType>();
}

//
// STUDENT INSTRUCTION:
//
// Add any additional code you need to here
//
set<string> DFSClientNodeP2::GetLocalFileList()
{
    DIR *dir;
    struct dirent *ent;
    dir = opendir(mount_path.c_str());
    if (dir == NULL)
    {
        dfs_log(LL_ERROR) << "Failed to open directory: " << mount_path;
        return {};
    }

    set<string> file_list;
    while ((ent = readdir(dir))!= NULL)
    {
        if (ent->d_type == DT_REG)
        {
            file_list.insert(ent->d_name);
        }
    }

    return file_list;
}