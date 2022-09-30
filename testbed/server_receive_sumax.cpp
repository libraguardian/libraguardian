#include<stdio.h>
#include<iostream>
#include<cstring>
#include<vector>
#include<unordered_map>
#include<set>
#include<mutex>
#include<thread>
#include<fstream>
#include<string>
#include<sys/fcntl.h>
#include<sys/socket.h>
#include<unistd.h>
#include<netinet/in.h>
#include<errno.h>
#include<sys/types.h>
#include <arpa/inet.h>
// #include "MurmurHash3.h"
using namespace std;

typedef pair<int,int> sumMax;
typedef pair<uint16_t,uint16_t> idRange;

int server_port = 10000;
int server_ip = 2130706433;     //127.0.0.1
int totalSwitch = 4;
const string path = "./out.txt";
unordered_map<int, vector<idRange>> idRange_map;

class Log {
        private:
                mutex m_lock;
                ofstream m_file_stream;
                string m_path;
        public:
                Log(const string & path):
                m_path(path) {
                        m_file_stream.open(m_path.c_str());
                        if(!m_file_stream.is_open() || !m_file_stream.good()) {
                                cout<<"file open error" << endl;
                        }
                }

                void write(const string & log) {
                        lock_guard<mutex> lock(m_lock);
                        m_file_stream << log << endl;
                }
};

Log logger(path);



vector<idRange> getIDRange(int switchID) {
    if(switchID >= totalSwitch) {
        return vector<idRange>{};
    }
    else {
        for(auto p:idRange_map[switchID]) {
            cout<< "get: " << p.first <<" "<< p.second <<endl;
        }
        return idRange_map[switchID];
    }
}


int receiveSumMax(int sockfd, vector<sumMax> & res) {
    int n = 0;
    char buf[512] = {};
    memset(buf,0, sizeof(buf));
    if(recv(sockfd, buf, sizeof(n),0) <= 0) {
        printf("receive error\n");
        return -1;
    }

    //receive n
    memcpy(&n, buf, sizeof(int));
    int packet_num = (n-1)/64 + 1;
    printf("n: %d\n",n);

    //receive data
    for(int i = 0; i < packet_num; i += 1) {
        memset(buf, 0, 512);
        int len = 0;
        if(i == packet_num - 1) {
            len = (n-1)%64 + 1;
        }
        else {
            len = 64;
        }
        printf("receive turn %d, len %d\n", i,len);

        if(recv(sockfd, buf, len*8, 0) <= 0) {
            printf("receive error\n");
            return -1;
        }

        int p_buffer = 0;
        int tmp_sum, tmp_max;
        for(int j = 0; j < len; ++j){
            memcpy(&tmp_sum, buf+p_buffer, sizeof(int));
            memcpy(&tmp_max, buf+p_buffer+sizeof(int), sizeof(int));
            p_buffer += 2*sizeof(int);
            res.push_back(make_pair(tmp_sum,tmp_max));
        }
    }
    return 0;

}

// int sendHashRange(int switchID, int sockfd) {
//     //idRange: 2 bytes minRange + 2 bytes maxRange
//     vector<idRange> data = getIDRange(switchID);
//     int n = data.size();
//     if(n == 0) {
//         return -1;
//     }
//     printf("n: %d\n",n);

//     int p_vector = 0, p_buffer = 0;      //vector pointer
//     //256 bytes, 64 pairs
//     int packet_num = (n-1)/64 + 1;
//     char buf[256] = {};


//     //send packet num
//     memset(buf, 0, 256);
//     memcpy(buf,&n, sizeof(n));
//     if(send(sockfd, buf, sizeof(n), 0) <= 0) {
//         printf("send num error\n");
//         return -1;
//     }

//     //send packets
//     for(int i = 0; i < packet_num; i += 1) {
//         int len = 0;
//         p_buffer = 0;
//         memset(buf, 0, 256);
//         if(i == packet_num - 1) {
//             len = (n-1)%64 + 1;
//         }
//         else {
//             len = 64;
//         }
        
//         printf("send turn %d, len %d\n", i,len);

//         for(int j = 0; j < len; ++j) {
//             memcpy(buf+p_buffer, &(data[p_vector].first), sizeof(uint16_t));
//             memcpy(buf+p_buffer+sizeof(uint16_t), &(data[p_vector].second), sizeof(uint16_t));
//             p_vector++;
//             p_buffer += 2*sizeof(uint16_t);
//         }

//         if(send(sockfd, buf, len*4, 0) <= 0) {
//             printf("send error\n");
//             return -1;
//         }
//     }
    
//     return 0;
// }

// void calculateHashRange() {
//     uint32_t seed = 100;

//     //switchID-hashValue
//     set<uint32_t> hashData;
//     unordered_map<int, uint32_t> idHash;

//     //calculate idHash
//     for(int i = 0; i < totalSwitch; i += 1){
//         uint32_t res;
//         MurmurHash3_x86_32(&i, sizeof(int), seed, &res);
//         res %= (1<<16);
//         idHash[i] = res;
//         hashData.insert(res);
//     }

//     //init range
//     for(auto p:idHash) {
//         auto iter = hashData.find(p.second);
//         uint32_t curNum = *(iter);
//         iter++;
//         if(iter == hashData.end()) {
//             idRange_map[p.first] = vector<idRange>{};
//             idRange_map[p.first].push_back(make_pair(curNum, 0xffff));
//             cout<<"add: "<<curNum<<" "<<0xffff<<endl;
//             idRange_map[p.first].push_back(make_pair(0,*(hashData.begin())));
//             cout<<"add: "<<0<<" "<<*(hashData.begin())<<endl;
//         }
//         else {
//             idRange_map[p.first] = vector<idRange>{};
//             idRange_map[p.first].push_back(make_pair(curNum, *(iter)));
//             cout<<"add: "<<curNum<<" "<<*(iter)<<endl;
//         }
//     }
// }

void server_thread(int clientfd) {
    while(true) {
        vector<sumMax> res;
        int recv_res = receiveSumMax(clientfd, res);

        if(recv_res == -1) {
            break;
        }
        
        for(auto p: res) {
            string data_line = to_string(p.first) + " " + to_string(p.second);
            logger.write(data_line);
        }
    }
    close(clientfd);
}

int main() {
    int client_num = 0;
    //hashRange init
    // calculateHashRange();

    //listen
    int listenfd = socket(AF_INET, SOCK_STREAM, 0);
    if(listenfd == -1) {
        printf("socket create fail\n");
    }

    //bind ip/port
    struct sockaddr_in serveraddr;
    memset(&serveraddr, 0, sizeof(serveraddr));
    serveraddr.sin_family = AF_INET;
    serveraddr.sin_addr.s_addr = htonl(INADDR_ANY);
    serveraddr.sin_port = htons(server_port);
    if(bind(listenfd, (struct sockaddr *)&serveraddr, sizeof(serveraddr)) != 0) {
        printf("bind failed\n");
        return -1;
    }

    //listen
    if(listen(listenfd, 10) != 0) {
        printf("Listen failed\n");
        close(listenfd);
        return -1;
    }

    while(true) {
        int clientfd;
        int socklen = sizeof(struct sockaddr_in);
        struct sockaddr_in client_addr;
        clientfd = accept(listenfd, (struct sockaddr*)&client_addr, (socklen_t *)&socklen);
        if(clientfd == -1) {
            printf("connect failed\n");
        }
        else {
            // sendHashRange(client_num++ , clientfd);
            thread(server_thread, clientfd);
        }
    }
    close(listenfd);
    return 0;
}