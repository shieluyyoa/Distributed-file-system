#include <map>
#include <mutex>
#include <shared_mutex>
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

#include "proto-src/dfs-service.grpc.pb.h"
#include "src/dfslibx-call-data.h"
#include "src/dfslibx-service-runner.h"
#include "dfslib-shared-p2.h"
#include "dfslib-servernode-p2.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerWriter;
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
//
// STUDENT INSTRUCTION:
//
// Change these "using" aliases to the specific
// message types you are using in your `dfs-service.proto` file
// to indicate a file request and a listing of files from the server
//
using FileRequestType = FileRequest;
using FileListResponseType = FileList;

using Filename = std::string;
using ClientId = std::string;
using FileMutex = std::shared_timed_mutex;

extern dfs_log_level_e DFS_LOG_LEVEL;

//
// STUDENT INSTRUCTION:
//
// As with Part 1, the DFSServiceImpl is the implementation service for the rpc methods
// and message types you defined in your `dfs-service.proto` file.
//
// You may start with your Part 1 implementations of each service method.
//
// Elements to consider for Part 2:
//
// - How will you implement the write lock at the server level?
// - How will you keep track of which client has a write lock for a file?
//      - Note that we've provided a preset client_id in DFSClientNode that generates
//        a client id for you. You can pass that to the server to identify the current client.
// - How will you release the write lock?
// - How will you handle a store request for a client that doesn't have a write lock?
// - When matching files to determine similarity, you should use the `file_checksum` method we've provided.
//      - Both the client and server have a pre-made `crc_table` variable to speed things up.
//      - Use the `file_checksum` method to compare two files, similar to the following:
//
//          std::uint32_t server_crc = dfs_file_checksum(filepath, &this->crc_table);
//
//      - Hint: as the crc checksum is a simple integer, you can pass it around inside your message types.
//
class DFSServiceImpl final : public DFSService::WithAsyncMethod_CallbackList<DFSService::Service>,
                             public DFSCallDataManager<FileRequestType, FileListResponseType>
{

private:
    /** The runner service used to start the service and manage asynchronicity **/
    DFSServiceRunner<FileRequestType, FileListResponseType> runner;

    /** The mount path for the server **/
    std::string mount_path;

    /** Mutex for managing the queue requests **/
    std::mutex queue_mutex;

    /** The vector of queued tags used to manage asynchronous requests **/
    std::vector<QueueRequest<FileRequestType, FileListResponseType>> queued_tags;

    /**
     * Prepend the mount path to the filename.
     *
     * @param filepath
     * @return
     */
    const std::string WrapPath(const std::string &filepath)
    {
        return this->mount_path + filepath;
    }

    /** CRC Table kept in memory for faster calculations **/
    CRC::Table<std::uint32_t, 32> crc_table;

    /** A map of file names to client ids that have a write lock on the file **/
    map<Filename, ClientId> file_write_locks_map;

    /** A shared mutex for managing the file write locks map **/
    shared_timed_mutex file_write_locks_map_mutex;

    /** A shared mutex for mount directory */
    shared_timed_mutex file_directory_mutex;

    /** A map of filename to file mutex */
    map<Filename, FileMutex> file_mutex_map;

    /** A shared mutx for file mutex map */
    shared_timed_mutex file_mutex_map_mutex;

public:
    DFSServiceImpl(const std::string &mount_path, const std::string &server_address, int num_async_threads) : mount_path(mount_path), crc_table(CRC::CRC_32())
    {

        this->runner.SetService(this);
        this->runner.SetAddress(server_address);
        this->runner.SetNumThreads(num_async_threads);
        this->runner.SetQueuedRequestsCallback([&]
                                               { this->ProcessQueuedRequests(); });
    }

    ~DFSServiceImpl()
    {
        this->runner.Shutdown();
    }

    void Run()
    {
        this->runner.Run();
    }

    /**
     * Request callback for asynchronous requests
     *
     * This method is called by the DFSCallData class during
     * an asynchronous request call from the client.
     *
     * Students should not need to adjust this.
     *
     * @param context
     * @param request
     * @param response
     * @param cq
     * @param tag
     */
    void RequestCallback(grpc::ServerContext *context,
                         FileRequestType *request,
                         grpc::ServerAsyncResponseWriter<FileListResponseType> *response,
                         grpc::ServerCompletionQueue *cq,
                         void *tag)
    {

        std::lock_guard<std::mutex> lock(queue_mutex);
        this->queued_tags.emplace_back(context, request, response, cq, tag);
    }

    /**
     * Process a callback request
     *
     * This method is called by the DFSCallData class when
     * a requested callback can be processed. You should use this method
     * to manage the CallbackList RPC call and respond as needed.
     *
     * See the STUDENT INSTRUCTION for more details.
     *
     * @param context
     * @param request
     * @param response
     */
    void ProcessCallback(ServerContext *context, FileRequestType *request, FileListResponseType *response)
    {

        //
        // STUDENT INSTRUCTION:
        //
        // You should add your code here to respond to any CallbackList requests from a client.
        // This function is called each time an asynchronous request is made from the client.
        //
        // The client should receive a list of files or modifications that represent the changes this service
        // is aware of. The client will then need to make the appropriate calls based on those changes.
        //
        Status status = CallbackList(context, request, response);

        if (!status.ok())
        {
            dfs_log(LL_ERROR) << "Error processing callback request: " << status.error_code();
        }

        return;
    }

    /**
     * Processes the queued requests in the queue thread
     */
    void ProcessQueuedRequests()
    {
        while (true)
        {

            //
            // STUDENT INSTRUCTION:
            //
            // You should add any synchronization mechanisms you may need here in
            // addition to the queue management. For example, modified files checks.
            //
            // Note: you will need to leave the basic queue structure as-is, but you
            // may add any additional code you feel is necessary.
            //

            // Guarded section for queue
            {
                dfs_log(LL_DEBUG2) << "Waiting for queue guard";
                std::lock_guard<std::mutex> lock(queue_mutex);

                for (QueueRequest<FileRequestType, FileListResponseType> &queue_request : this->queued_tags)
                {
                    this->RequestCallbackList(queue_request.context, queue_request.request,
                                              queue_request.response, queue_request.cq, queue_request.cq, queue_request.tag);
                    queue_request.finished = true;
                }

                // any finished tags first
                this->queued_tags.erase(std::remove_if(
                                            this->queued_tags.begin(),
                                            this->queued_tags.end(),
                                            [](QueueRequest<FileRequestType, FileListResponseType> &queue_request)
                                            { return queue_request.finished; }),
                                        this->queued_tags.end());
            }
        }
    }

    Status RequestWriteLock(ServerContext *context, const FileRequestType *request, Empty *response) override
    {
        dfs_log(LL_DEBUG) << "RequestWriteLock";
        string filename = request->name();
        string client_id = request->client_id();

        if (context->IsCancelled())
        {
            dfs_log(LL_DEBUG) << "Client cancelled write lock request";
            return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded");
        }

        file_write_locks_map_mutex.lock_shared();
        auto file_write_lock_it = file_write_locks_map.find(filename);
        if (file_write_lock_it == file_write_locks_map.end())
        {
            file_write_locks_map_mutex.unlock_shared();
            file_write_locks_map_mutex.lock();
            file_write_locks_map[filename] = client_id;
            file_write_locks_map_mutex.unlock();
            return Status::OK;
        }

        if (file_write_lock_it->second != client_id)
        {
            file_write_locks_map_mutex.unlock_shared();
            return Status(StatusCode::RESOURCE_EXHAUSTED, "File already locked by another client");
        }


        file_write_locks_map_mutex.unlock_shared();

        return Status::OK;


    }



    Status StoreFile(ServerContext *context, ServerReader<StoreRequest> *reader, StoreResponse *response) override
    {
        StoreRequest store_request;
        ofstream out;
        multimap<grpc::string_ref, grpc::string_ref> metadata = context->client_metadata();

        auto filename_ref = metadata.find("filename");
        auto client_id_ref = metadata.find("client_id");
        auto checksum_ref = metadata.find("checksum");

        string filename = string(filename_ref->second.data(), filename_ref->second.length());
        string client_id = string(client_id_ref->second.data(), client_id_ref->second.length());
        string checksum = string(checksum_ref->second.data(), checksum_ref->second.length());

        dfs_log(LL_DEBUG) << "StoreFile: " << filename;

        // Check if the client has the write lock on the file
        // if not, return RESOURCE_EXHAUSTED and release the lock
        file_write_locks_map_mutex.lock_shared();
        if (file_write_locks_map.find(filename) == file_write_locks_map.end())
        {
            return Status(StatusCode::RESOURCE_EXHAUSTED, "Writer lock does not exist for this file");
        }

        if (file_write_locks_map[filename] != client_id)
        {
            dfs_log(LL_DEBUG2) << "File already locked by another client : " << filename;
            return Status(StatusCode::RESOURCE_EXHAUSTED, "File already locked by another client");
        }
        file_write_locks_map_mutex.unlock_shared();


        dfs_log(LL_DEBUG) << "Create file mutex if it does not exist";
        // Create file mutex if it does not exist
        file_mutex_map_mutex.lock();
        if (file_mutex_map.find(filename) == file_mutex_map.end())
        {
            file_mutex_map[filename];
        }

        file_mutex_map_mutex.unlock();

        // Check if the client checksum matches file
        // if matches, return ALREADY_EXISTS and release the lock
        dfs_log(LL_DEBUG) << "Check if file checksum matches client checksum";
        string filepath = WrapPath(filename);
        uint32_t server_file_checksum = dfs_file_checksum(filepath, &(this->crc_table));
        if (server_file_checksum == stoul(checksum))
        {
            struct stat st;

            if (stat(filepath.c_str(), &st) == 0)
            {
                response->set_mtime(st.st_mtime);
            }

            return Status(StatusCode::ALREADY_EXISTS, "File already exists");
        }

        // Write Lock the mount directory
        // Write Lock the file
        file_directory_mutex.lock();
        file_mutex_map_mutex.lock_shared();
        file_mutex_map[filename].lock();
        // Write the file to the mount directory
        try
        {

            while (reader->Read(&store_request))
            {

                if (!out.is_open())
                {
                    dfs_log(LL_DEBUG) << "Storing File : " << filename;
                    std::string filepath = WrapPath(filename);
                    out.open(filepath);
                }

                if (context->IsCancelled())
                {
                    dfs_log(LL_DEBUG) << "Client cancelled, abandoning.";
                    file_mutex_map[filename].unlock();
                    file_mutex_map_mutex.unlock_shared();
                    file_directory_mutex.unlock();
                    return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded.");
                }

                dfs_log(LL_DEBUG) << "Bytes received: " << sizeof(store_request.chunk());
                out << store_request.chunk();
            }

            out.close();
        }
        catch (std::exception &e)
        {
            dfs_log(LL_ERROR) << "Exception: " << e.what();
            file_mutex_map[filename].unlock();
            file_mutex_map_mutex.unlock_shared();
            file_directory_mutex.unlock();
            return Status(StatusCode::CANCELLED, e.what());
        }

        // Check if the stored file matches the checksum
        // if not, return CANCELLED and delete the file and release the lock
        dfs_log(LL_DEBUG) << "Checking file checksum";
        // Write Unlock the file
        // Write Unlock the mount directory
        file_mutex_map[filename].unlock();
        file_mutex_map_mutex.unlock_shared();
        file_directory_mutex.unlock();

        dfs_log(LL_DEBUG) << "Deleting write lock";

        // Delete write lock from the map
        file_write_locks_map_mutex.lock();
        file_write_locks_map.erase(filename);
        file_write_locks_map_mutex.unlock();

        dfs_log(LL_DEBUG) << "File stored successfully";

        // Return filemtime to the client and return OK
        struct stat st;
        if (stat(filepath.c_str(), &st) != 0)
        {
            dfs_log(LL_ERROR) << "File not found";
            return Status(StatusCode::CANCELLED, "File not found");
        }

        // show mtime
        dfs_log(LL_DEBUG) << "File mtime: " << st.st_mtime;
        response->set_mtime(st.st_mtime);

        return Status::OK;
    }


    Status FetchFile(ServerContext *context, const FileRequestType *request, ServerWriter<Chunk> *writer) override
    {
        dfs_log(LL_DEBUG) << "Fetching file : " << request->name();
        Chunk chunk;
        string filename = request->name();
        string file_path = WrapPath(request->name());

        // Check if the file exists
        // Acquire Read LOck
        file_directory_mutex.lock_shared();
        file_mutex_map_mutex.lock_shared();
        shared_timed_mutex *file_mutex = &file_mutex_map[filename];
        file_mutex->lock_shared();
        int file_size = GetFileSize(file_path);
        if (file_size < 0) {
            dfs_log(LL_ERROR) << "File not found";
            file_mutex->unlock_shared();
            file_mutex_map_mutex.unlock_shared();
            file_directory_mutex.unlock_shared();
            return Status(StatusCode::NOT_FOUND, "File not found");
        }

        // Send File Chunk
        int bytes_sent = 0;
        ifstream file(file_path);

        // set mtime
        struct stat st;
        if (stat(file_path.c_str(), &st) != 0)
        {
            dfs_log(LL_ERROR) << "File not found";
            file_mutex->unlock_shared();
            file_mutex_map_mutex.unlock_shared();
            file_directory_mutex.unlock_shared();
            return Status(StatusCode::CANCELLED, "File not found");
        }

        chunk.set_mtime(st.st_mtime);

        try {
            while (bytes_sent < file_size)
            {
                if (context->IsCancelled())
                {
                    dfs_log(LL_DEBUG) << "Client cancelled, abandoning.";
                    file_mutex->unlock_shared();
                    file_mutex_map_mutex.unlock_shared();
                    file_directory_mutex.unlock_shared();
                    return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded."); 
                }

                char buffer[DFS_BUFFERSIZE];
                int bytes_ready = min(file_size - bytes_sent, DFS_BUFFERSIZE);
                file.read(buffer, bytes_ready);
                chunk.set_chunk(buffer, bytes_ready);
                writer->Write(chunk);
                bytes_sent += file.gcount();
            }
        } catch(std::exception &e) {
            dfs_log(LL_ERROR) << "Exception: " << e.what();
            file_mutex->unlock_shared();
            file_mutex_map_mutex.unlock_shared();
            file_directory_mutex.unlock_shared();
            return Status(StatusCode::CANCELLED, e.what());
        }

        dfs_log(LL_DEBUG) << "Total bytes sent: " << bytes_sent;

        // Unlock Read Lock
        file_mutex->unlock_shared();
        file_mutex_map_mutex.unlock_shared();
        file_directory_mutex.unlock_shared();
        return Status::OK;

    }

    Status DeleteFile(ServerContext *context, const FileRequestType *request, Empty *response) override
    {
        dfs_log(LL_DEBUG) << "Deleting file : " << request->name();
        string filename = request->name();
        string file_path = WrapPath(request->name());
        string client_id = request->client_id();

        if (context->IsCancelled())
        {
            dfs_log(LL_DEBUG) << "Client cancelled, abandoning.";
            return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded or Client cancelled, abandoning.");
        }

        // Check if client has obtained write lock
        file_write_locks_map_mutex.lock_shared();
        if (file_write_locks_map.find(filename) == file_write_locks_map.end())
        {
            return Status(StatusCode::RESOURCE_EXHAUSTED, "Writer lock does not exist for this file");
        }

        if (file_write_locks_map[filename] != client_id)
        {
            dfs_log(LL_DEBUG2) << "File already locked by another client : " << filename;
            return Status(StatusCode::RESOURCE_EXHAUSTED, "File already locked by another client");
        }
        file_write_locks_map_mutex.unlock_shared();
        
        file_mutex_map_mutex.lock_shared();
        shared_timed_mutex *file_mutex = &file_mutex_map[filename];
        // Check if file exists
        file_directory_mutex.lock_shared();
        file_mutex->lock_shared();

        struct stat result;
        
        if (stat(file_path.c_str(), &result)!= 0) {
            file_mutex->unlock_shared();
            file_mutex_map_mutex.unlock_shared();
            file_directory_mutex.unlock_shared();
            return Status(StatusCode::NOT_FOUND, "File not found");
        }

        file_mutex->unlock_shared();
        file_mutex_map_mutex.unlock_shared();
        file_directory_mutex.unlock_shared();

        // Delete the file
        file_mutex_map_mutex.lock_shared();
        file_directory_mutex.lock();
        file_mutex->lock();

        if (remove(file_path.c_str())!= 0) {
            file_mutex->unlock();
            file_mutex_map_mutex.unlock_shared();
            file_directory_mutex.unlock();
            return Status(StatusCode::CANCELLED, "Unable to delete file");
        }

        file_mutex->unlock();
        file_mutex_map_mutex.unlock_shared();
        file_directory_mutex.unlock();

        // Delete write lock from the map
        file_write_locks_map_mutex.lock();
        file_write_locks_map.erase(filename);
        file_write_locks_map_mutex.unlock();

        dfs_log(LL_DEBUG) << "File deleted successfully :" << filename ;

        return Status::OK;

    }

    Status ListFiles(ServerContext *context, const FileRequestType *request, FileListResponseType *response) override
    {
        dfs_log(LL_DEBUG) << "Listing files";
        DIR *dir;
        struct dirent *ent;

        file_directory_mutex.lock_shared();
        dir = opendir(mount_path.c_str());
        if (dir == NULL)
        {
            file_directory_mutex.unlock_shared();
            return Status(StatusCode::CANCELLED, "Unable to open directory");
        }

        file_mutex_map_mutex.lock_shared();
        while ((ent = readdir(dir))!= NULL)
        {
            if (context->IsCancelled()) {
                file_mutex_map_mutex.unlock_shared();
                file_directory_mutex.unlock_shared();
                return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded or Client cancelled, abandoning.");
            }

            if (ent->d_type == DT_REG)
            {
                struct stat st;
                const string file_path = WrapPath(ent->d_name);

                shared_timed_mutex *file_mutex = &file_mutex_map[ent->d_name];
                file_mutex->lock_shared();
                if (stat(file_path.c_str(), &st) == 0)
                {
                    dfs_log(LL_DEBUG) << "Adding file : " << ent->d_name;
                    uint32_t crc = dfs_file_checksum(file_path, &(this->crc_table));
                    FileDetails *file_details = response->add_file_details(); 
                    file_details->set_name(ent->d_name);
                    file_details->set_mtime(st.st_mtime);
                    file_details->set_ctime(st.st_ctime);
                    file_details->set_checksum(crc);

                    // log checksum
                    dfs_log(LL_DEBUG) << "File : " << ent->d_name << " checksum : " << crc;
                }
                file_mutex->unlock_shared();
            }
        }
        file_mutex_map_mutex.unlock_shared();
        file_directory_mutex.unlock_shared();
        closedir(dir);

        return Status::OK;

    }

    Status GetFileStat(ServerContext *context, const FileRequestType *request, FileDetails *response) override
    {
        dfs_log(LL_DEBUG) << "Getting file stat : " << request->name();
        string filename = request->name();
        string file_path = WrapPath(filename);
        string client_id = request->client_id();

        if (context->IsCancelled()) {
            return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded or Client cancelled, abandoning.");
        }

        // Check if file exists
        file_directory_mutex.lock_shared();
        file_mutex_map_mutex.lock_shared();
        shared_timed_mutex *file_mutex = &file_mutex_map[filename];
        file_mutex->lock_shared();

        struct stat result;
        if (stat(file_path.c_str(), &result)!= 0) {
            file_mutex->unlock_shared();
            file_mutex_map_mutex.unlock_shared();
            file_directory_mutex.unlock_shared();
            return Status(StatusCode::NOT_FOUND, "File not found");
        }

        // Read file stat from file
        response->set_name(filename);
        response->set_mtime(result.st_mtime);
        response->set_ctime(result.st_ctime);
        response->set_checksum(dfs_file_checksum(file_path, &(this->crc_table)));

        file_mutex->unlock_shared();
        file_mutex_map_mutex.unlock_shared();
        file_directory_mutex.unlock_shared();

        return Status::OK;
    }

    Status CallbackList(ServerContext *context, const FileRequestType *request, FileListResponseType *response) override
    {
        dfs_log(LL_DEBUG) << "Callback Listing files";
        DIR *dir;
        struct dirent *ent;

        file_directory_mutex.lock_shared();
        dir = opendir(mount_path.c_str());
        if (dir == NULL)
        {
            file_directory_mutex.unlock_shared();
            return Status(StatusCode::CANCELLED, "Unable to open directory");
        }

        file_mutex_map_mutex.lock_shared();
        while ((ent = readdir(dir))!= NULL)
        {
            // if (context->IsCancelled()) {
            //     file_mutex_map_mutex.unlock_shared();
            //     file_directory_mutex.unlock_shared();
            //     return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded or Client cancelled, abandoning.");
            // }

            if (ent->d_type == DT_REG)
            {
                struct stat st;
                const string file_path = WrapPath(ent->d_name);

                shared_timed_mutex *file_mutex = &file_mutex_map[ent->d_name];
                file_mutex->lock_shared();
                if (stat(file_path.c_str(), &st) == 0)
                {
                    // dfs_log(LL_DEBUG) << "Adding file : " << ent->d_name;
                    uint32_t crc = dfs_file_checksum(file_path, &(this->crc_table));
                    FileDetails *file_details = response->add_file_details(); 
                    file_details->set_name(ent->d_name);
                    file_details->set_mtime(st.st_mtime);
                    file_details->set_ctime(st.st_ctime);
                    file_details->set_checksum(crc);

                    // log checksum
                    // dfs_log(LL_DEBUG) << "File : " << ent->d_name << " checksum : " << crc;
                }
                file_mutex->unlock_shared();
            }
        }
        file_mutex_map_mutex.unlock_shared();
        file_directory_mutex.unlock_shared();
        closedir(dir);

        return Status::OK;
    }
};

//
// STUDENT INSTRUCTION:
//
// The following three methods are part of the basic DFSServerNode
// structure. You may add additional methods or change these slightly
// to add additional startup/shutdown routines inside, but be aware that
// the basic structure should stay the same as the testing environment
// will be expected this structure.
//
/**
 * The main server node constructor
 *
 * @param mount_path
 */
DFSServerNode::DFSServerNode(const std::string &server_address,
                             const std::string &mount_path,
                             int num_async_threads,
                             std::function<void()> callback) : server_address(server_address),
                                                               mount_path(mount_path),
                                                               num_async_threads(num_async_threads),
                                                               grader_callback(callback) {}
/**
 * Server shutdown
 */
DFSServerNode::~DFSServerNode() noexcept
{
    dfs_log(LL_SYSINFO) << "DFSServerNode shutting down";
}

/**
 * Start the DFSServerNode server
 */
void DFSServerNode::Start()
{
    DFSServiceImpl service(this->mount_path, this->server_address, this->num_async_threads);

    dfs_log(LL_SYSINFO) << "DFSServerNode server listening on " << this->server_address;
    service.Run();
}

//
// STUDENT INSTRUCTION:
//
// Add your additional definitions here
//
