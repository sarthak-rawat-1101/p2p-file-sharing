#include <iostream>
#include <string.h>
#include <vector>
#include <queue>
#include <map>
#include <set>
#include <algorithm>
#include <sys/types.h>
#include <stdlib.h>
#include <climits>
#include <unistd.h>
#include <cmath>

#include <sys/stat.h>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "pthread.h"

#define BUFFSIZE 524288
#define SERVER_BACKLOG 100

using namespace std;

long est_connection(long port_num);

static pthread_mutex_t mut_dload_queue = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t cond_dload_queue = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t mut_groups = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t mut_users = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t mut_cout = PTHREAD_MUTEX_INITIALIZER;

struct file_dload_util_args
{
    string group_id;
    string file_name;
};

struct seeder_manager_args
{
    long port_num;
    char group_id[256];
    char file_name[256];
    // seeder_manager_args(long _pn, string _fn, string _gid){
    //     port_num  = _pn;
    //     file_name = _fn;
    //     group_id  = _gid;
    // }
};

class Chunk
{
public:
    string sha1;
    long size;
    long status; // ready - downloading - downloaded

    Chunk(string _sha1, long _size, long _status)
    {
        sha1 = "";
        size = _size;
        status = _status;
    }

    Chunk()
    {
        sha1 = "";
        size = 0;
        status = 0;
    }
};

class File
{
public:
    string name;
    string path;
    long size;    // number of bytes
    long nchunks; // number of chunks
    long status;  // ready - downloading - downloaded
    vector<Chunk> chunks;
    queue<long> ready_queue;
    set<long> seeders; // port number of seeders of this file

    File(string _name, string _path, long _size, long _status)
    {
        name = _name;
        path = _path;
        size = _size;
        nchunks = ceil(size * 1.0 / BUFFSIZE); // BUFFSIZE = 512KB
        status = _status;                      // ready = 0
        long lcs = size % BUFFSIZE;
        for (long i = 0; i < nchunks - 1; ++i)
        {
            chunks.push_back(Chunk("", BUFFSIZE, status));
        }
        if (nchunks != 0)
            chunks.push_back(Chunk("", lcs, status));
    }
    File()
    {
        name = "";
        path = "";
        size = 0;
        nchunks = 0; // BUFFSIZE = 512KB
        status = 0;  // ready = 0
    }
};

class User
{
public:
    string name;
    string pass;
    set<string> groups;
    set<string> users_groups;
    set<pair<string, string>> files_shared;

    User(string _name, string _pass)
    {
        name = _name;
        pass = _pass;
    }

    User()
    {
        name = "";
        pass = "";
    }
};

class Group
{
public:
    string id;
    map<string, File> files; // shared by user

    Group(string _id)
    {
        id = _id;
    }
    Group()
    {
        id = "";
    }
};

map<string, User> users;
map<string, Group> groups;
queue<pair<string, string>> download_queue; // group_id, file_name

string user_name = "";
string user_pass = "";

vector<string> parse_message(const char *buffer, long len)
{
    vector<string> message;
    long i = -1, r = 0;
    while (r < len)
    {
        message.push_back("");
        ++i;
        while (r < len and (buffer[r] != ' ' and buffer[r] != '\n'))
        {
            message[i].push_back(buffer[r++]);
        }
        while (r < len and (buffer[r] == ' ' or buffer[r] == '\n'))
            ++r;
    }
    return message;
}

long get_file_size(string file_path)
{
    struct stat stat_buf;
    long rc = stat(file_path.c_str(), &stat_buf);
    return rc == 0 ? stat_buf.st_size : -1;
}

string extract_file_name(string file_path)
{
    return file_path.substr(file_path.find_last_of("/") + 1);
}

long setup_socket(long port_num, long backlog)
{
    struct sockaddr_in server_addr;
    socklen_t size;
    long socfd;

    if ((socfd = socket(AF_INET, SOCK_STREAM, 0)) < 0)
    {
        cerr << "Error establishing socket" << endl;
        exit(1);
    }

    bzero(&server_addr, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    server_addr.sin_port = htons(port_num);

    if (bind(socfd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0)
    {
        cerr << "Error binding" << endl;
        exit(1);
    }

    if (listen(socfd, backlog) < 0)
    {
        cerr << "Error listen" << endl;
        exit(1);
    }

    return socfd;
}

long accept_connections(long socfd)
{
    long connfd;
    if ((connfd = accept(socfd, (struct sockaddr *)NULL, NULL)) < 0)
    {
        cout << "Error accepting" << endl;
        return (0); // todo : dont exit, programm should continue running
    }
    return connfd;
}

long est_connection(long port_num)
{
    struct sockaddr_in server_addr;
    long connfd;
    /*  Esatblish connection with the tracker */
    if ((connfd = socket(AF_INET, SOCK_STREAM, 0)) < 0)
    {
        cerr << "Error establishing socket" << endl;
        return -1;
    }

    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port_num);

    if (inet_pton(AF_INET, "127.0.0.1", &server_addr.sin_addr) <= 0)
    {
        cerr << "Invalid address Address not supported" << endl;
        return -1;
    }

    if (connect(connfd, (struct sockaddr *)&server_addr, sizeof(server_addr)) != 0)
    {
        cerr << "Error connecting" << endl;
        return -1;
    }

    return connfd;
}

void *handle_uploads(void *arg)
{
    long connfd = (long)arg;
    char buffer[BUFFSIZE];
    vector<string> message;
    long cnt;

    cnt = read(connfd, buffer, BUFFSIZE);

    message = parse_message(buffer, cnt);

    if (message.size() != 4)
    {
        // send err message
        sprintf(buffer, "Invalid message\n");
        return NULL;
    }
    string group_id = message[1];
    string file_name = message[2];
    long chunk_number = stoi(message[3]); // try catch

    if (groups.find(group_id) == groups.end() or groups[group_id].files.find(file_name) == groups[group_id].files.end() or groups[group_id].files[file_name].nchunks <= chunk_number or groups[group_id].files[file_name].status != 2)
    {
        // send err message
        sprintf(buffer, "Invalid group_id or file_name or chunk number out of bound\n");
        pthread_exit(NULL);
    }
    else
    {
        // cout << "request recieved " << group_id << " " << file_name << " " << chunk_number << endl;
        File &file = groups[group_id].files[file_name];
        FILE *fd = fopen(file.path.c_str(), "rb");
        fseek(fd, BUFFSIZE * chunk_number, SEEK_SET);
        fread(buffer, 1, file.chunks[chunk_number].size, fd);
        int cnt = write(connfd, buffer, file.chunks[chunk_number].size);
        // cout << "sent:" << cnt << endl;
        fclose(fd);
    }

    pthread_exit(NULL);
}

void *handle_upload_reqs(void *arg)
{
    long socfd = (long)arg;
    long connfd;
    pthread_t thread;
    while (1)
    {
        connfd = accept_connections(socfd);
        if (pthread_create(&thread, NULL, handle_uploads, (void *)connfd) != 0)
        {
            cerr << "Error creating thread for upload request" << endl; /* exit(1) */
            ;
        }
    }
    pthread_exit(NULL);
}

void *seeder_manager(void *arg)
{
    /* 
        arguments-
        1 seeder port number
        2 group_id
        3 file_name
    */
    string group_id = ((seeder_manager_args *)arg)->group_id;
    string file_name = ((seeder_manager_args *)arg)->file_name;
    long port_num = ((seeder_manager_args *)arg)->port_num;
    free(arg);

    // cout << "Manager:" << port_num << " " << file_name << " " << group_id << " " << endl;

    char buffer[BUFFSIZE];
    long connfd;
    long cnt;

    // pick a chunk from queue
    // set the status to downloading
    // make a connection
    // request the chunk
    // set the status to downloaded
    File &file = groups[group_id].files[file_name];
    while (1)
    {
        if (file.ready_queue.empty())
            break;
        // cout << "Connecting to seeder..." << endl;
        connfd = est_connection(port_num);
        if (connfd <= 0)
            break;
        // cout << "Connected to seeder" << endl;
        long crr_chunk = file.ready_queue.front();
        file.ready_queue.pop();

        file.chunks[crr_chunk].status = 1;

        // cout << "Opening file..." << file.path << endl;
        FILE *fd = fopen(file.path.c_str(), "a+");

        if (fd == NULL)
        {
            pthread_exit(NULL);
            close(connfd);
        }
        fclose(fd);
        fd = fopen(file.path.c_str(), "rb+");
        fseek(fd, BUFFSIZE * crr_chunk, SEEK_SET);

        // cout << "Downloading chunk:" << crr_chunk << " " << file.chunks[crr_chunk].size << endl;

        sprintf(buffer, "get %s %s %ld", group_id.c_str(), file_name.c_str(), crr_chunk);
        write(connfd, buffer, strlen(buffer));

        long dloaded = 0;
        while (dloaded < file.chunks[crr_chunk].size)
        {
            cnt = read(connfd, buffer, BUFFSIZE);
            dloaded += cnt;
            cnt = fwrite(buffer, 1, cnt, fd);
        }

        fclose(fd);

        if (dloaded == file.chunks[crr_chunk].size)
        {
            file.chunks[crr_chunk].status = 2;
            // cout << "successfully downloaded the chunk" << endl;
        }
        else
        {
            // cout << "downloaded chunk size = " << dloaded << endl;
            // cout << "actual chunk size = " << file.chunks[crr_chunk].size << endl;

            file.ready_queue.push(crr_chunk);
            file.chunks[crr_chunk].status = 0;
        }

        close(connfd);
    }
    pthread_exit(NULL);
}

//  handle dloads ----->  file_dload_util -------> seeder_manager

void *file_dload_util(void *arg)
{
    string group_id = ((file_dload_util_args *)arg)->group_id;
    string file_name = ((file_dload_util_args *)arg)->file_name;
    free(arg);
    // cout << "downloading: " << group_id << " " << file_name << endl;

    if (groups.find(group_id) == groups.end() or groups[group_id].files.find(file_name) == groups[group_id].files.end())
    {
        cerr << "File not in DB" << endl;
    }
    else
    {
        File &file = groups[group_id].files[file_name];
        // set the state of this file as Downloading
        file.status = 1;
        while (!file.ready_queue.empty())
            file.ready_queue.pop();

        // push all the un-downloaded chunks to the ready queue
        if (file.chunks.size() != file.nchunks)
        {
            cerr << "unequal sizes";
            exit(1);
        }

        for (long i = 0; i < file.chunks.size(); ++i)
        {
            if (file.chunks[i].status == 0)
                file.ready_queue.push(i);
        }

        vector<pthread_t> threads;
        // start a manager for each seeder
        for (auto &seeder : file.seeders)
        {
            pthread_t thread;
            seeder_manager_args *arg1 = (seeder_manager_args *)malloc(sizeof(seeder_manager_args));
            arg1->port_num = seeder;
            strncpy(arg1->file_name, file_name.c_str(), file_name.length());
            strncpy(arg1->group_id, group_id.c_str(), group_id.length());

            if (pthread_create(&thread, NULL, seeder_manager, (void *)arg1) != 0)
            {
                cerr << "Error creating thread for manager util" << endl;
                continue;
            }

            threads.push_back(thread);
        }

        void *ret;
        for (long i = 0; i < threads.size(); ++i)
        {
            if (pthread_join(threads[i], &ret) == 0)
            {
                ;
            }
        }

        long i;
        for (i = 0; i < file.nchunks; ++i)
        {
            // cout << "chunk " << i << " status = " << file.chunks[i].status << endl;
            if (file.chunks[i].status != 2)
                break;
        }
        if (i != file.nchunks)
        {
            file.status = 0;
            pthread_mutex_lock(&mut_dload_queue);
            download_queue.push({group_id, file_name});
            pthread_mutex_unlock(&mut_dload_queue);
            pthread_cond_signal(&cond_dload_queue);
        }
        else
        {
            file.status = 2;
        }
    }

    pthread_exit(NULL);
}

void *handle_downloads(void *arg)
{
    while (1)
    {
        file_dload_util_args *arg = (file_dload_util_args *)malloc(sizeof(file_dload_util_args));
        pthread_t thread;

        // lock queue
        pthread_mutex_lock(&mut_dload_queue);

        // wait if queue is empty
        while (download_queue.empty())
        {
            pthread_cond_wait(&cond_dload_queue, &mut_dload_queue);
        }

        // fetch ready file
        pair<string, string> file = download_queue.front();
        download_queue.pop();

        pthread_mutex_unlock(&mut_dload_queue);

        // start file-download-util for that file
        arg->group_id = file.first;
        arg->file_name = file.second;

        if (pthread_create(&thread, NULL, file_dload_util, (void *)arg) != 0)
        {
            cerr << "Error creating thread for dload util" << endl;
            continue;
        }
    }
    return NULL;
}

string send_command_to_tracker(long tracker_port_num, string message)
{
    long connfd = est_connection(tracker_port_num);
    char buffer[BUFFSIZE];
    long cnt;
    string res = "";

    strncpy(buffer, message.c_str(), message.length());
    write(connfd, buffer, message.length());

    while ((cnt = read(connfd, buffer, BUFFSIZE)) > 0)
    {
        res.append(buffer, cnt);
    }

    close(connfd);
    return res;
}

signed main(int argc, char **argv)
{
    FILE *tracker_txt = fopen(argv[3], "r");
    char tracker_ip[256];
    int tracker_port;
    cout << argv[3] << endl;
    if (tracker_txt == NULL)
        exit(1);
    fscanf(tracker_txt, "%s %d", tracker_ip, &tracker_port);
    fclose(tracker_txt);

    string cl_port_s = argv[2];

    long cl_port = stoi(cl_port_s);
    long tr_port = 42069;
    long server_socfd = setup_socket(cl_port, SERVER_BACKLOG);
    pthread_t thread;

    // cout << cl_port << " " << tr_port << endl;

    if (pthread_create(&thread, NULL, handle_upload_reqs, (void *)server_socfd) != 0)
    {
        cerr << "Error creating thread for upload handler" << endl;
        exit(1);
    }
    /*
    * accept connection
    * read message "get [user_name] group_id file_name chunk_number"
    * read file -- seek to correct position
    * send chunk of size 512KB
    * exit 
    **/

    /* 
    *   Download handler-
    *   for each un-downloaded file
    *       set its state to downloading
    *       push all the un-downloaded chunks to the ready queue
    *       start the downloader(download manager) for each seeder
    **/
    if (pthread_create(&thread, NULL, handle_downloads, (void *)NULL) != 0)
    {
        cerr << "Error creating thread for download handler" << endl;
        exit(1);
    }

    while (1)
    {
        /* 
        *   read command
        *   parse command
        *   send command to the server
        *   receive the response from the server
        *   act accordingly
        * */
        cout << "> ";
        string command;
        getline(cin, command);
        vector<string> command_parts;
        command_parts = parse_message(command.c_str(), command.length());

        if (command_parts[0] == "create_user")
        {
            // create_user user_name user_pass
            if (command_parts.size() != 3)
            {
                cout << "Invalid number of arguments: create_user user_name user_pass" << endl;
            }
            else
            {
                command = "signup " + command_parts[1] + " " + command_parts[2];
                string res = send_command_to_tracker(tr_port, command);
                cout << "response:" << res << endl;
                if (res == "signup successfull\n")
                {
                    users.insert({command_parts[1], User(command_parts[1], command_parts[2])});
                }
            }
        }
        else if (command_parts[0] == "login")
        {
            // login <user_id> <passwd>
            if (command_parts.size() != 3)
            {
                cout << "Invalid number of arguments: login user_name user_pass" << endl;
            }
            else
            {
                command = "signin " + command_parts[1] + " " + command_parts[2] + " " + to_string(cl_port);

                string res = send_command_to_tracker(tr_port, command);
                cout << res << endl;
                if (res == "signin successfull\n")
                {
                    user_name = command_parts[1];
                    user_pass = command_parts[2];

                    if (users.find(user_name) == users.end())
                    {
                        users.insert({user_name, User(user_name, user_pass)});
                    }
                }
            }
        }
        else if (user_name == "")
        {
            cout << "not logged in" << endl;
        }
        else if (command_parts[0] == "create_group")
        {
            // create_group <group_id>
            if (command_parts.size() != 2)
            {
                cout << "Invalid number of arguments: create_group <group_id>" << endl;
            }
            else
            {
                command = command_parts[0] + " " + user_name + " " + user_pass + " " + command_parts[1];

                string res = send_command_to_tracker(tr_port, command);
                cout << res << endl;
                if (res == "group created\n")
                {
                    users[user_name].groups.insert(command_parts[1]);
                    users[user_name].users_groups.insert(command_parts[1]);
                }
            }
        }
        else if (command_parts[0] == "join_group")
        {
            // join_group <group_id>
            if (command_parts.size() != 2)
            {
                cout << "Invalid number of arguments: join_group <group_id>" << endl;
            }
            else
            {
                command = command_parts[0] + " " + user_name + " " + user_pass + " " + command_parts[1];

                string res = send_command_to_tracker(tr_port, command);
                cout << res << endl;
                if (res == "request succesfully added\n")
                {
                    users[user_name].groups.insert(command_parts[1]);
                }
            }
        }
        else if (command_parts[0] == "leave_group")
        {
            // leave_group <group_id>
            if (command_parts.size() != 2)
            {
                cout << "Invalid number of arguments: leave_group <group_id>" << endl;
            }
            else
            {
                command = command_parts[0] + " " + user_name + " " + user_pass + " " + command_parts[1];

                string res = send_command_to_tracker(tr_port, command);
                cout << res << endl;
                if (res == "successfully left the group\n")
                {
                    users[user_name].groups.erase(command_parts[1]);
                }
            }
        }
        else if (command_parts[0] == "list_requests")
        {
            // list_requests <group_id>
            if (command_parts.size() != 2)
            {
                cout << "Invalid number of arguments: list_requests <group_id>" << endl;
            }
            else
            {
                command = "fetch_pending_requests " + user_name + " " + user_pass + " " + command_parts[1];

                string res = send_command_to_tracker(tr_port, command);
                cout << res << endl;
            }
        }
        else if (command_parts[0] == "accept_request")
        {
            // accept_request <group_id> <user_id>
            if (command_parts.size() != 3)
            {
                cout << "Invalid number of arguments: accept_request <group_id> <user_id>" << endl;
            }
            else
            {
                command = "accept_join_request " + user_name + " " + user_pass + " " + command_parts[1] + " " + command_parts[2];

                string res = send_command_to_tracker(tr_port, command);
                cout << res << endl;
            }
        }
        else if (command_parts[0] == "list_groups")
        {
            // list_groups
            if (command_parts.size() != 1)
            {
                cout << "Invalid number of arguments: list_groups" << endl;
            }
            else
            {
                command = "fetch_groups";
                string res = send_command_to_tracker(tr_port, command);
                cout << res << endl;
            }
        }
        else if (command_parts[0] == "list_files")
        {
            // list_files <group_id>
            if (command_parts.size() != 2)
            {
                cout << "Invalid number of arguments: list_files <group_id>" << endl;
            }
            else
            {
                command = "fetch_files_list " + user_name + " " + user_pass + " " + command_parts[1];

                string res = send_command_to_tracker(tr_port, command);
                cout << res << endl;
            }
        }
        else if (command_parts[0] == "upload_file")
        {
            // upload_file <file_path> <group_id>
            if (command_parts.size() != 3)
            {
                cout << "Invalid number of arguments: upload_file <file_path> <group_id>" << endl;
            }
            else
            {

                string file_path = command_parts[1];
                string file_name = extract_file_name(file_path);
                string group_id = command_parts[2];
                long file_size = get_file_size(file_path);

                if (file_size == -1)
                {
                    cout << "Error reading file" << endl;
                    continue;
                }

                command = "share_file " + user_name + " " + user_pass + " " + group_id + " " + file_name + " " + to_string(file_size);

                string res = send_command_to_tracker(tr_port, command);
                cout << res << endl;

                /* 
                * update the data in the client
                **/

                if (groups.find(group_id) == groups.end())
                    groups.insert({group_id, Group(group_id)});

                // groups[group_id].files.insert({file_name, File(file_name, file_path, file_size, 2)});
                groups[group_id].files[file_name] = File(file_name, file_path, file_size, 2);

                users[user_name].files_shared.insert({group_id, file_name});
            }
        }
        else if (command_parts[0] == "download_file")
        {
            // download_file <group_id> <file_name> <destination_path>
            if (command_parts.size() != 4)
            {
                cout << "Invalid number of arguments: download_file <group_id> <file_name> <destination_path>" << endl;
            }
            else
            {
                string group_id = command_parts[1];
                string file_name = command_parts[2];
                string dest_path = command_parts[3];

                command = "get_seeders_for_file " + user_name + " " + user_pass + " " + group_id + " " + file_name;

                string res = send_command_to_tracker(tr_port, command);
                // cout << res << endl;

                if (res == "username and password did not match\n" or res == "invalid group_id\n" or res == "unauthorized access\n" or res == "file not found\n")
                {
                    cout << res << endl;
                }
                else
                {
                    vector<string> seeders;
                    seeders = parse_message(res.c_str(), res.length());
                    // cout << seeders.size() << endl;
                    int file_size = stoi(seeders[1]);
                    groups[group_id].files.insert(
                        {file_name,
                         File(file_name, dest_path + "/" + file_name, file_size, 0)});
                    users[user_name].files_shared.insert({group_id, file_name});
                    groups[group_id].files[file_name].seeders.clear();

                    for (long i = 3; i < seeders.size(); i += 2)
                    {
                        // cout << "adding seeder:" << seeders[i] << endl;
                        groups[group_id].files[file_name].seeders.insert(stoi(seeders[i]));
                    }

                    pthread_mutex_lock(&mut_dload_queue);
                    download_queue.push({group_id, file_name});
                    pthread_mutex_unlock(&mut_dload_queue);
                    pthread_cond_signal(&cond_dload_queue);

                    command = "share_file " + user_name + " " + user_pass + " " + group_id + " " + file_name + " " + to_string(file_size);
                    string res = send_command_to_tracker(tr_port, command);
                    cout << res << endl;
                }
            }
        }
        else if (command_parts[0] == "logout")
        {
            // logout
            if (command_parts.size() != 1)
            {
                cout << "Invalid number of arguments: logout" << endl;
            }
            else
            {
                command = "logout " + user_name + " " + user_pass;

                string res = send_command_to_tracker(tr_port, command);
                cout << res << endl;

                /* 
                update the data in the client
                 */
            }
        }
        else if (command_parts[0] == "show_downloads")
        {
            for (auto &gr : users[user_name].groups)
            {
                for (auto &file : groups[gr].files)
                {
                    char ds = 'D'; // download status
                    if (file.second.status == 2)
                        ds = 'C';
                    printf("[%c]%s %s\n", ds, gr.c_str(), file.second.name.c_str());
                }
            }
        }
        else if (command_parts[0] == "stop_share")
        {
            // stop_share <group_id> <file_name>
            if (command_parts.size() != 2)
            {
                cout << "Invalid number of arguments: stop_share <group_id> <file_name>" << endl;
            }
            else
            {
                command = "stop_sharing_file " + user_name + " " + user_pass + " " + command_parts[1] + " " + command_parts[2];

                string res = send_command_to_tracker(tr_port, command);
                cout << res << endl;

                /* 
                update the data in the client
                 */
                users[user_name].files_shared.erase({command_parts[1], command_parts[2]});
            }
        }
        else
        {
            cout << "unknown command" << endl;
        }
    }

    return 0;
}