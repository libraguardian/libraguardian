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
#include "MurmurHash3.h"
using namespace std;

typedef pair<int,int> sumMax;
typedef uint32_t ip;

//user set
vector<pair<ip,ip>> src_dst_pairs{pair<ip,ip>{1,1},pair<ip,ip>{2,3}};
int server_port = 10000;
int server_ip = 2886731405;     //127.0.0.1
int totalSwitch = 4;
const string path = "./out.txt";

//12bytes: srcIP[i]-dstIP[i]-ipRange[i].first-ipRange[i].second
class idRange{
    public:
        uint32_t src_ip;
        uint32_t dst_ip;
        uint16_t range_begin;
        uint16_t range_end;

        idRange(uint32_t src_ip_, uint32_t dst_ip_, uint16_t range_begin_, uint16_t range_end_):
            src_ip(src_ip_),dst_ip(dst_ip_),range_begin(range_begin_),range_end(range_end_) {}

};

//switchID-vector(idRange)
unordered_map<int, vector<idRange>> idRange_map;


vector<idRange> getIDRange(int switchID) {
    if(switchID >= totalSwitch) {
        return vector<idRange>{};
    }
    else {
        return idRange_map[switchID];
    }
}

int sendHashRange(int switchID, int sockfd) {
    //4 srcip + 4 dstip + 2 bytes minRange + 2 bytes maxRange
    vector<idRange> data = getIDRange(switchID);
    int n = data.size();
    if(n == 0) {
        return -1;
    }
    printf("n: %d\n",n);

    int p_vector = 0, p_buffer = 0;      //vector pointer
    //384 bytes, 32 pairs
    int packet_num = (n-1)/32 + 1;
    char buf[384] = {};


    //send packet num
    memset(buf, 0, 384);
    memcpy(buf,&n, sizeof(n));
    if(send(sockfd, buf, sizeof(n), 0) <= 0) {
        printf("send num error\n");
        return -1;
    }

    //send packets
    for(int i = 0; i < packet_num; i += 1) {
        int len = 0;
        p_buffer = 0;
        memset(buf, 0, 384);
        if(i == packet_num - 1) {
            len = (n-1)%32 + 1;
        }
        else {
            len = 32;
        }
        
        printf("send turn %d, len %d\n", i,len);

        for(int j = 0; j < len; ++j) {
            memcpy(buf+p_buffer, &(data[p_vector].src_ip), sizeof(uint32_t));
            memcpy(buf+p_buffer+sizeof(uint32_t), &(data[p_vector].dst_ip), sizeof(uint32_t));
            memcpy(buf+p_buffer+2*sizeof(uint32_t), &(data[p_vector].range_begin), sizeof(uint16_t));
            memcpy(buf+p_buffer+2*sizeof(uint32_t)+sizeof(uint16_t), &(data[p_vector].range_end), sizeof(uint16_t));
            p_vector++;
            p_buffer += 2*sizeof(uint16_t)+2*sizeof(uint32_t);
        }

        if(send(sockfd, buf, len*12, 0) <= 0) {
            printf("send error\n");
            return -1;
        }
    }
    
    return 0;
}



void calculateHashRange(int switchNum) {        //switchNum: total number of switches
    uint32_t seed = 100;
    int flowNum = src_dst_pairs.size();


    for(int f = 0; f < flowNum; f++) {
        //switchID-hashValue
        set<uint32_t> hashData;     //hashValue set
        unordered_map<int, uint32_t> idHash;        //id-hashValue

        //calculate idHash
        for(int i = 0; i < switchNum; i += 1){
            uint32_t res;
            MurmurHash3_x86_32(&i, sizeof(int), seed, &res);
            res %= (1<<16);
            idHash[i] = res;
            hashData.insert(res);
        }

        //init range
        for(auto p:idHash) {
            auto iter = hashData.find(p.second);
            uint32_t curNum = *(iter);
            iter++;
            if(iter == hashData.end()) {
                idRange_map[p.first].push_back(idRange{src_dst_pairs[f].first,src_dst_pairs[f].second, (uint16_t)curNum, (uint16_t)0xffff});
                cout<<"add: "<<src_dst_pairs[f].first<<" "<<src_dst_pairs[f].second<<" "<<curNum<<" "<<0xffff<<endl;

                idRange_map[p.first].push_back(idRange{src_dst_pairs[f].first,src_dst_pairs[f].second, 0,(uint16_t)(*hashData.begin())});
                cout<<"add: "<<src_dst_pairs[f].first<<" "<<src_dst_pairs[f].second<<" "<<0<<" "<<*(hashData.begin())<<endl;
            }
            else {
                idRange_map[p.first].push_back(idRange{src_dst_pairs[f].first,src_dst_pairs[f].second, (uint16_t)curNum, (uint16_t)(*(iter))});
                cout<<"add: "<<src_dst_pairs[f].first<<" "<<src_dst_pairs[f].second<<" "<<curNum<<" "<<*(iter)<<endl;
            }
        }
    
        seed += 100;
    }
}

void server_thread(int clientfd, int switchID) {
    sendHashRange(switchID,clientfd);
    close(clientfd);
}

int main() {
    int client_num = 0;
    //hashRange init
    calculateHashRange(totalSwitch);

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
            thread(server_thread, clientfd, client_num++);
        }
    }
    close(listenfd);
    return 0;
}