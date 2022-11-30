#include <string.h>
#include <vector>
#include <map>
#include <set>
#include <deque>
#include <algorithm>
#include <climits>

// #include <sys/types.h>
// #include <stdlib.h>
#include <iostream>
#include <unistd.h>
// #include <fstream>
#include <stdexcept>

#include <sys/socket.h>
// #include <netinet/in.h>
#include <arpa/inet.h>

#include "pthread.h"

#define SERVER_BACKLOG 100

using namespace std;

int setup_socket(int port_num, int backlog);
int accept_connections(int connfd);
void *handle_connection(void *connfd);

vector<string> parse_message(char *buffer, int len);

static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t mut_users = PTHREAD_MUTEX_INITIALIZER;
static pthread_rwlock_t rw_lock_users = PTHREAD_RWLOCK_INITIALIZER;
static pthread_rwlock_t rw_lock_groups = PTHREAD_RWLOCK_INITIALIZER;

const int BUFFSIZE = 524288; // 512 * 1024 Bytes ie 512 KB

class User
{
public:
    string name;
    string password;
    int port;

    set<string> groups_joined;
    map<string, set<string>> shared_files; // g1: index.html, server.cpp, ...

    User(string _name, string _pass, int _port)
    {
        name = _name;
        password = _pass;
        port = _port;
    }
    User()
    {
        name = "";
        password = "";
        port = 0;
    }
};

class File
{
public:
    string name;
    int size;
    set<string> members;
    File(string _name, int _size)
    {
        name = _name;
        size = _size;
    }
    File()
    {
        name = "";
        size = 0;
    }
};

class Group
{
public:
    string id;
    string admin;
    set<string> members;
    map<string, File> files;
    set<string> pending_requests;
    Group(string g_id, string g_admin)
    {
        id = g_id;
        admin = g_admin;
    }
    Group()
    {
        id = "";
        admin = "";
    }
};

map<string, User> users;
map<string, Group> groups;

/* NOTE TO SELF : using iterators for reading data might be a good choice WRT race condition */

signed main(int argc, char **argv)
{
    FILE *tracker_txt = fopen(argv[1], "r");
    char tracker_ip[256];
    int tracker_port;
    cout << argv[1] << endl;
    if (tracker_txt == NULL)
        exit(1);
    fscanf(tracker_txt, "%s %d", tracker_ip, &tracker_port);
    fclose(tracker_txt);
    int socfd = setup_socket(tracker_port, SERVER_BACKLOG);
    cout << "started server port" << endl;
    while (1)
    {
        long connfd = accept_connections(socfd);
        if (connfd == 0)
            continue;
        cout << "accepted connection" << endl;
        pthread_t thread;
        if (pthread_create(&thread, NULL, handle_connection, (void *)connfd) != 0)
        {
            cerr << "Error creating thread" << endl; /* exit(1); */
        }
    }

    return 0;
}

int setup_socket(int port_num, int backlog)
{
    struct sockaddr_in server_addr;
    socklen_t size;
    int socfd;

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

int accept_connections(int socfd)
{
    int connfd;
    if ((connfd = accept(socfd, (struct sockaddr *)NULL, NULL)) < 0)
    {
        cout << "Error accepting" << endl;
        return (0); // todo : dont exit, programm should continue running
    }
    return connfd;
}

bool user_auth(string user_name, string user_pass)
{
    // read lock
    bool res = (users.find(user_name) == users.end() or users[user_name].password == user_pass);
    // read unlock
    return res;
}

void signup(char *buffer, string user_name, string password)
{
    if (users.find(user_name) != users.end())
    {
        sprintf(buffer, "username already exists\n");
    }
    else
    {
        users.insert({user_name, User(user_name, password, 0)});
        sprintf(buffer, "signup successfull\n");
    }
}

void signin(char *buffer, string user_name, string user_pass, string port)
{
    if (!user_auth(user_name, user_pass))
    {
        sprintf(buffer, "username and password did not match\n");
    }
    else
    {
        try
        {
            users[user_name].port = stoi(port); // Throws: no conversion
            map<string, set<string>> &shared_files = users[user_name].shared_files;
            for (auto &grs : shared_files)
                for (auto &file : grs.second)
                    groups[grs.first].files[file].members.insert(user_name);

            sprintf(buffer, "signin successfull\n");
        }
        catch (invalid_argument const &ex)
        {
            cerr << ex.what() << '\n';
            sprintf(buffer, "wrong port number\n");
        }
    }
}

void logout(char *buffer, string user_name, string user_pass)
{
    if (!user_auth(user_name, user_pass))
    {
        sprintf(buffer, "username or password incorrect\n");
    }
    else
    {
        // remove all shared files from groups data
        map<string, set<string>> &shared_files = users[user_name].shared_files;
        for (auto &grs : shared_files)
            for (auto &file : grs.second)
                groups[grs.first].files[file].members.erase(user_name);

        sprintf(buffer, "logout successfull\n");
    }
}

void create_group(char *buffer, string user_name, string user_pass, string group_id)
{
    if (!user_auth(user_name, user_pass))
    {
        sprintf(buffer, "username or password incorrect\n");
    }
    else
    {
        if (groups.find(group_id) != groups.end())
        {
            sprintf(buffer, "group_id already exists\n");
        }
        else
        {
            groups.insert({group_id, Group(group_id, user_name)});
            groups[group_id].members.insert(user_name);
            users[user_name].groups_joined.insert(group_id);
            sprintf(buffer, "group created\n");
        }
    }
}

void fetch_groups(char *buffer)
{
    string res = "";
    for (auto &group : groups)
    {
        res += group.second.id + " " + group.second.admin + " " + to_string(users[group.second.admin].port) + "\n";
    }
    sprintf(buffer, "%s", res.c_str());
}

void join_group(char *buffer, string user_name, string user_pass, string group_id)
{
    if (!user_auth(user_name, user_pass))
    {
        sprintf(buffer, "username and password did not match\n");
    }
    else if (groups.find(group_id) == groups.end())
    {
        sprintf(buffer, "invalid group_id\n");
    }
    else
    {
        groups[group_id].pending_requests.insert(user_name);
        sprintf(buffer, "request succesfully added\n");
    }
}

void leave_group(char *buffer, string user_name, string user_pass, string group_id)
{
    if (!user_auth(user_name, user_pass))
    {
        sprintf(buffer, "username and password did not match\n");
    }
    else if (groups.find(group_id) == groups.end())
    {
        sprintf(buffer, "invalid group_id\n");
    }
    else
    {
        groups[group_id].members.erase(user_name);
        for (auto &file : users[user_name].shared_files[group_id])
            groups[group_id].files[file].members.erase(user_name);
        users[user_name].shared_files.erase(group_id);
        sprintf(buffer, "successfully left the group\n");
    }
}

void fetch_pending_requests(char *buffer, string user_name, string user_pass, string group_id)
{
    if (!user_auth(user_name, user_pass))
    {
        sprintf(buffer, "username and password did not match\n");
    }
    else if (groups.find(group_id) == groups.end())
    {
        sprintf(buffer, "invalid group_id\n");
    }
    else if (groups[group_id].admin != user_name)
    {
        sprintf(buffer, "you are not the admin *****\n");
    }
    else
    {
        string res = "";
        for (auto &preq : groups[group_id].pending_requests)
            res += preq + "\n";
        sprintf(buffer, "%s", res.c_str());
    }
}

void accept_join_request(char *buffer, string user_name, string user_pass, string group_id, string member_name)
{
    if (!user_auth(user_name, user_pass))
    {
        sprintf(buffer, "username and password did not match\n");
    }
    else if (groups.find(group_id) == groups.end())
    {
        sprintf(buffer, "invalid group_id\n");
    }
    else if (groups[group_id].admin != user_name)
    {
        sprintf(buffer, "you are not the admin *****\n");
    }
    else if (users.find(member_name) == users.end())
    {
        sprintf(buffer, "invalid member_name\n");
    }
    else
    {
        groups[group_id].members.insert(member_name);
        sprintf(buffer, "successfully added the user to the group\n");
    }
}

void reject_join_request(char *buffer, string user_name, string user_pass, string group_id, string member_name)
{
    if (!user_auth(user_name, user_pass))
    {
        sprintf(buffer, "username and password did not match\n");
    }
    else if (groups.find(group_id) == groups.end())
    {
        sprintf(buffer, "invalid group_id\n");
    }
    else if (groups[group_id].admin != user_name)
    {
        sprintf(buffer, "you are not the admin *****\n");
    }
    else if (users.find(member_name) == users.end())
    {
        sprintf(buffer, "invalid member_name\n");
    }
    else
    {
        groups[group_id].pending_requests.erase(member_name);
        sprintf(buffer, "successfully added the user to the group\n");
    }
}

void share_file(char *buffer, string user_name, string user_pass, string group_id, string file_name, string file_size)
{
    if (!user_auth(user_name, user_pass))
    {
        sprintf(buffer, "username and password did not match\n");
    }
    else if (groups.find(group_id) == groups.end())
    {
        sprintf(buffer, "invalid group_id\n");
    }
    else if (groups[group_id].members.find(user_name) == groups[group_id].members.end())
    {
        sprintf(buffer, "unauthorized access\n");
    }
    else
    {
        try
        {
            map<string, File> &files = groups[group_id].files;

            if (files.find(file_name) == files.end())
                files.insert({file_name, File(file_name, stoi(file_size))});

            files[file_name].members.insert(user_name);

            users[user_name].shared_files[group_id].insert(file_name);

            sprintf(buffer, "added file: %s %s\n", file_name.c_str(), file_size.c_str());
        }
        catch (invalid_argument const &ex)
        {
            cerr << ex.what() << '\n';
            sprintf(buffer, "invalid size\n");
        }
    }
}

void stop_sharing_file(char *buffer, string user_name, string user_pass, string group_id, string file_name)
{
    if (!user_auth(user_name, user_pass))
    {
        sprintf(buffer, "username and password did not match\n");
    }
    else if (groups.find(group_id) == groups.end())
    {
        sprintf(buffer, "invalid group_id\n");
    }
    else if (groups[group_id].members.find(user_name) == groups[group_id].members.end())
    {
        sprintf(buffer, "unauthorized access\n");
    }
    else
    {
        try
        {
            map<string, File> &files = groups[group_id].files;
            files[file_name].members.erase(user_name);
            users[user_name].shared_files[group_id].erase(file_name);
        }
        catch (invalid_argument const &ex)
        {
            cerr << ex.what() << '\n';
            sprintf(buffer, "invalid size\n");
        }
    }
}

void fetch_files_list(char *buffer, string user_name, string user_pass, string group_id)
{
    if (!user_auth(user_name, user_pass))
    {
        sprintf(buffer, "username and password did not match\n");
    }
    else if (groups.find(group_id) == groups.end())
    {
        sprintf(buffer, "invalid group_id\n");
    }
    else if (groups[group_id].members.find(user_name) == groups[group_id].members.end())
    {
        sprintf(buffer, "unauthorized access\n");
    }
    else
    {
        map<string, File> &files = groups[group_id].files;
        string res = "";
        for (auto &file : files)
            res += file.second.name + " " + to_string(file.second.size) + "\n";
        sprintf(buffer, "%s", res.c_str());
    }
}

void get_seeders_for_file(char *buffer, string user_name, string user_pass, string group_id, string file_name)
{
    if (!user_auth(user_name, user_pass))
    {
        sprintf(buffer, "username and password did not match\n");
    }
    else if (groups.find(group_id) == groups.end())
    {
        sprintf(buffer, "invalid group_id\n");
    }
    else if (groups[group_id].members.find(user_name) == groups[group_id].members.end())
    {
        sprintf(buffer, "unauthorized access\n");
    }
    else if (groups[group_id].files.find(file_name) == groups[group_id].files.end())
    {
        sprintf(buffer, "file not found\n");
    }
    else
    {
        string res = file_name + " " + to_string(groups[group_id].files[file_name].size) + "\n";
        for (auto &memb : groups[group_id].files[file_name].members)
            res += memb + " " + to_string(users[memb].port) + "\n";
        sprintf(buffer, "%s", res.c_str());
    }
}

void *handle_connection(void *p_connfd)
{
    char buffer[BUFFSIZE];
    int connfd = (long)p_connfd;
    int cnt;

    cnt = read(connfd, buffer, BUFFSIZE);

    pthread_mutex_lock(&mutex);
    // pthread_mutex_lock(&mutex);
    // cout << cnt << " "<< buffer << endl;
    // pthread_mutex_unlock(&mutex);
    vector<string> message;
    message = parse_message(buffer, cnt);

    if (message.size() == 0)
        pthread_exit(NULL);
    // pthread_rwlock_wrlock(&rw_lock_users);
    // pthread_rwlock_wrlock(&rw_lock_groups);
    if (message[0] == "signup")
    {
        if (message.size() != 3)
            sprintf(buffer, "enter username and possword: message count = %ld\n", message.size());
        else
            signup(buffer, message[1], message[2]);
    }
    else if (message[0] == "signin")
    {
        if (message.size() != 4)
            sprintf(buffer, "enter username, password and port_number:\n");
        else
            signin(buffer, message[1], message[2], message[3]);
    }
    else if (message[0] == "logout")
    {
        if (message.size() != 3)
            sprintf(buffer, "enter username password");
        else
            logout(buffer, message[1], message[2]);
    }
    else if (message[0] == "create_group")
    {
        if (message.size() != 4)
            sprintf(buffer, "enter username password group_id");
        else
            create_group(buffer, message[1], message[2], message[3]);
    }
    else if (message[0] == "fetch_groups")
    {
        if (message.size() != 1)
            sprintf(buffer, "invalid request");
        else
            fetch_groups(buffer);
    }
    else if (message[0] == "join_group")
    {
        if (message.size() != 4)
            sprintf(buffer, "enter username password group_id");
        else
            join_group(buffer, message[1], message[2], message[3]);
    }
    else if (message[0] == "leave_group")
    {
        if (message.size() != 4)
            sprintf(buffer, "enter username password group_id");
        else
            leave_group(buffer, message[1], message[2], message[3]);
    }
    else if (message[0] == "fetch_pending_requests")
    {
        if (message.size() != 4)
            sprintf(buffer, "enter username password group_id");
        else
            fetch_pending_requests(buffer, message[1], message[2], message[3]);
    }
    else if (message[0] == "accept_join_request")
    {
        if (message.size() != 5)
            sprintf(buffer, "invalid arguments; enter user_name user_pass group_id member_name\n");
        else
            accept_join_request(buffer, message[1], message[2], message[3], message[4]);
    }
    else if (message[0] == "reject_join_request")
    {
        if (message.size() != 5)
            sprintf(buffer, "invalid arguments; enter user_name user_pass group_id member_name\n");
        else
            reject_join_request(buffer, message[1], message[2], message[3], message[4]);
    }
    else if (message[0] == "share_file")
    {
        if (message.size() != 6)
            sprintf(buffer, "invalid arguments; enter user_name user_pass group_id file_name file_size\n");
        else
            share_file(buffer, message[1], message[2], message[3], message[4], message[5]);
    }
    else if (message[0] == "stop_sharing_file")
    {
        if (message.size() != 5)
            sprintf(buffer, "invalid arguments; enter user_name user_pass group_id file_name\n");
        else
            stop_sharing_file(buffer, message[1], message[2], message[3], message[4]);
    }
    else if (message[0] == "fetch_files_list")
    {
        if (message.size() != 4)
            sprintf(buffer, "enter username password group_id");
        else
            fetch_files_list(buffer, message[1], message[2], message[3]);
    }
    else if (message[0] == "get_seeders_for_file")
    {
        if (message.size() != 5)
            sprintf(buffer, "invalid arguments; enter user_name user_pass group_id file_name\n");
        else
            get_seeders_for_file(buffer, message[1], message[2], message[3], message[4]);
    }
    else
    {
        cout << "wrong message:" << message[0] << endl;
        sprintf(buffer, "HTTP/1.0 200 OK \r\n\r\nwrong message:%s\n", message[0].c_str());
    }
    // pthread_rwlock_unlock(&rw_lock_users);
    // pthread_rwlock_unlock(&rw_lock_groups);

    pthread_mutex_unlock(&mutex);

    write(connfd, buffer, strlen(buffer));
    close(connfd);
    pthread_exit(NULL);
}

vector<string> parse_message(char *buffer, int len)
{
    vector<string> message;
    int i = -1, r = 0;
    while (r < len)
    {
        message.push_back("");
        ++i;
        while (r < len and buffer[r] != ' ')
        {
            message[i].push_back(buffer[r++]);
        }
        while (r < len and buffer[r] == ' ')
            ++r;
    }
    return message;
}