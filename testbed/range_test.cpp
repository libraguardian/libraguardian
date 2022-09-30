#include <bf_rt/bf_rt_info.hpp>
#include <bf_rt/bf_rt_init.hpp>
#include <bf_rt/bf_rt_common.h>
#include <bf_rt/bf_rt_table_key.hpp>
#include <bf_rt/bf_rt_table_data.hpp>
#include <bf_rt/bf_rt_table_operations.hpp>
#include <bf_rt/bf_rt_table.hpp>
#include <getopt.h>
#include <unistd.h>
#include <iostream>
#include <fstream>
#include <chrono>
#include <thread>
#include<vector>
#include <arpa/inet.h>
extern "C"
{
#include <bf_pm/bf_pm_intf.h>
#include <pkt_mgr/pkt_mgr_intf.h>
#include <bf_switchd/bf_switchd.h>
}
#define ALL_PIPES 0xffff
#define HH_num 0x10000
#define flow_table_size 1024
#define counter_num 0x10000

uint64_t flow_reg_index = 0;

uint16_t sum_counter[3][counter_num];
uint16_t max_counter[3][counter_num];

int flow_table_index = 0;

/******debug test********/
uint64_t HH_reg_addr = 10;
uint64_t HH_reg_counter = 20;
/*****************/

typedef std::pair<int,int> sumMax;
// typedef pair<uint16_t,uint16_t> idRange;

std::vector<sumMax> SuMax_vector;
int server_port;
uint32_t server_ip = 2886731405;
const int switchID = 0;


std::unordered_map<uint64_t, uint64_t> HH_flow;
std::vector<uint64_t> flow_reg_f1;

bf_rt_target_t dev_tgt;

std::shared_ptr<bfrt::BfRtSession> session;


class idRange{
    public:
        idRange(uint32_t src_ip_, uint32_t dst_ip_, uint16_t range_begin_, uint16_t range_end_):
            src_ip(src_ip_),dst_ip(dst_ip_),range_begin(range_begin_),range_end(range_end_) {}
    // private:
        uint32_t src_ip;
        uint32_t dst_ip;
        uint16_t range_begin;
        uint16_t range_end;
};


void init_ports(){
  dev_tgt.dev_id = 0;
  dev_tgt.pipe_id = ALL_PIPES;
  bf_pm_port_add_all(dev_tgt.dev_id, BF_SPEED_40G, BF_FEC_TYP_NONE);
  bf_pm_port_enable_all(dev_tgt.dev_id);
  if(bf_pkt_is_inited(0)){
    printf("bf_pkt is initialized");
  }
}

void init_tables(){
  system("./bfshell -b /mnt/onl/data/hashing/init_table.py");
}

uint64_t flag = 0;

std::vector<std::unique_ptr<bfrt::BfRtTableKey>> HH_reg_key(HH_num);
std::vector<std::unique_ptr<bfrt::BfRtTableData>> HH_reg_data(HH_num);
const bfrt::BfRtTable *HH_reg_table = nullptr;
bf_rt_id_t HH_reg_data_id = 0;
bf_rt_id_t HH_reg_data_id2 = 0;
std::unique_ptr<bfrt::BfRtTableOperations> HH_reg_to = nullptr;
std::unique_ptr<bfrt::BfRtTableOperations> HH_reg_to1 = nullptr;

std::vector<std::unique_ptr<bfrt::BfRtTableKey>> sumax_1_key(counter_num);
std::vector<std::unique_ptr<bfrt::BfRtTableData>> sumax_1_data(counter_num);
const bfrt::BfRtTable *SuMax_1_table = nullptr;
bf_rt_id_t sumax_1_data_id = 0;
bf_rt_id_t sumax_1_data_id2 = 0;
std::unique_ptr<bfrt::BfRtTableOperations> sumax_1_to = nullptr;

std::vector<std::unique_ptr<bfrt::BfRtTableKey>> sumax_2_key(counter_num);
std::vector<std::unique_ptr<bfrt::BfRtTableData>> sumax_2_data(counter_num);
const bfrt::BfRtTable *SuMax_2_table = nullptr;
bf_rt_id_t sumax_2_data_id = 0;
bf_rt_id_t sumax_2_data_id2 = 0;
std::unique_ptr<bfrt::BfRtTableOperations> sumax_2_to = nullptr;

std::vector<std::unique_ptr<bfrt::BfRtTableKey>> sumax_3_key(counter_num);
std::vector<std::unique_ptr<bfrt::BfRtTableData>> sumax_3_data(counter_num);
const bfrt::BfRtTable *SuMax_3_table = nullptr;
bf_rt_id_t sumax_3_data_id = 0;
bf_rt_id_t sumax_3_data_id2 = 0;
std::unique_ptr<bfrt::BfRtTableOperations> sumax_3_to = nullptr;

std::vector<std::unique_ptr<bfrt::BfRtTableKey>> flow_reg_key(flow_table_size);
std::vector<std::unique_ptr<bfrt::BfRtTableData>> flow_reg_data(flow_table_size);
const bfrt::BfRtTable *flow_reg_table = nullptr;
bf_rt_id_t flow_reg_data_id = 0;
std::unique_ptr<bfrt::BfRtTableOperations> flow_reg_to = nullptr;


std::unique_ptr<bfrt::BfRtTableKey> flow_table_key;
std::unique_ptr<bfrt::BfRtTableData> flow_table_data;
const bfrt::BfRtTable *flow_table = nullptr;
bf_rt_id_t flow_table_key_id = 0;
bf_rt_id_t flow_table_data_id = 1234;
bf_rt_id_t flow_table_add_id = 0;

std::unique_ptr<bfrt::BfRtTableKey> cons_hash_key;
std::unique_ptr<bfrt::BfRtTableData> cons_hash_data;
const bfrt::BfRtTable *cons_hash_table = nullptr;
bf_rt_id_t cons_hash_key_addr = 0;
bf_rt_id_t cons_hash_key_index = 0;
bf_rt_id_t cons_hash_action_id = 0;
bf_rt_id_t cons_hash_data_id = 0;

namespace bfrt{
  namespace examples {
    namespace cons_hash{

      const bfrt::BfRtInfo *bfrtInfo = nullptr;

      void init(){
        auto &devMgr = bfrt::BfRtDevMgr::getInstance();
        devMgr.bfRtInfoGet(dev_tgt.dev_id, "dp_v5", &bfrtInfo);

        //create a session object
        session = bfrt::BfRtSession::sessionCreate();

        bfrtInfo->bfrtTableFromNameGet("Ingress.HH_reg", &HH_reg_table);//Get a BfRtTable obj from its fully qualified name
        
        bfrtInfo->bfrtTableFromNameGet("Ingress.flow_table", &flow_table);
        flow_table->keyFieldIdGet("hdr.ipv4.dst_addr", &flow_table_key_id);
        flow_table->actionIdGet("Ingress.flow_count", &flow_table_add_id);
        flow_table->dataFieldIdGet("index", flow_table_add_id, &flow_table_data_id);

        bfrtInfo->bfrtTableFromNameGet("Ingress.con_hash_t", &cons_hash_table);
        cons_hash_table->keyFieldIdGet("hdr.ipv4.dst_addr", &cons_hash_key_addr);
        cons_hash_table->keyFieldIdGet("meta.index_1", &cons_hash_key_index);
        cons_hash_table->actionIdGet("Ingress.con_hash_a", &cons_hash_action_id);
        cons_hash_table->dataFieldIdGet("match", cons_hash_action_id, &cons_hash_data_id);   

        bfrtInfo->bfrtTableFromNameGet("Ingress.sumax_1", &SuMax_1_table);
        bfrtInfo->bfrtTableFromNameGet("Ingress.sumax_2", &SuMax_2_table);
        bfrtInfo->bfrtTableFromNameGet("Ingress.sumax_3", &SuMax_3_table);

        bfrtInfo->bfrtTableFromNameGet("Ingress.flow_reg", &flow_reg_table);
      }


      void register_init(const bfrt::BfRtTable* table, std::vector<std::unique_ptr<BfRtTableKey>>& keys, std::vector<std::unique_ptr<BfRtTableData>>& data, std::string data_name, uint32_t entry_num, bf_rt_id_t &data_id){
        table->dataFieldIdGet(data_name, &data_id);
        BfRtTable::keyDataPairs key_data_pairs;

        for(unsigned i = 0; i < entry_num; i++){
          table->keyAllocate(&keys[i]);
          table->dataAllocate(&data[i]);
        }

        for(unsigned i = 1; i < entry_num; i++){
          key_data_pairs.push_back(std::make_pair(keys[i].get(), data[i].get()));
        }

        auto flag = bfrt::BfRtTable::BfRtTableGetFlag::GET_FROM_HW;
        table->tableEntryGetFirst(*session, dev_tgt, flag, keys[0].get(), data[0].get());
        session->sessionCompleteOperations();

        if(entry_num > 1){
          uint32_t num_returned = 0;
          table->tableEntryGetNext_n(*session, dev_tgt, *keys[0].get(), entry_num -1, flag, &key_data_pairs,&num_returned);
          session->sessionCompleteOperations();
        }
      }

      void register_init_counter(const bfrt::BfRtTable * table, std::vector<std::unique_ptr<BfRtTableKey>>& keys, std::vector<std::unique_ptr<BfRtTableData>> &data, 
            std::string data_name, std::string data_name2, uint32_t entry_num, bf_rt_id_t &data_id, bf_rt_id_t& data_id2)
      {
        table->dataFieldIdGet(data_name, &data_id);
        table->dataFieldIdGet(data_name2, &data_id2);

        BfRtTable::keyDataPairs key_data_pairs;

        for(unsigned i = 0; i < entry_num; ++i){
          table->keyAllocate(&keys[i]);
          table->dataAllocate(&data[i]);
        }

        for(unsigned i = 1; i < entry_num; ++i){
          key_data_pairs.push_back(std::make_pair(keys[i].get(), data[i].get()));
        }

        auto flag = bfrt::BfRtTable::BfRtTableGetFlag::GET_FROM_HW;

        table->tableEntryGetFirst(*session, dev_tgt, flag, keys[0].get(), data[0].get());//Get the first entry of the table

        session->sessionCompleteOperations();//Wait for all operations to complete under this session

        if(entry_num > 1){
          uint32_t num_return = 0;
          table->tableEntryGetNext_n(*session, dev_tgt, *keys[0].get(), entry_num -1, flag, &key_data_pairs, &num_return);
          session->sessionCompleteOperations();
        }

      }

      void read_register(const bfrt::BfRtTable* table, std::vector<std::unique_ptr<BfRtTableKey>>& keys, std::vector<std::unique_ptr<BfRtTableData>>& data, bf_rt_id_t& data_id, uint32_t entry_num){
        std::vector<uint64_t> dt;
        for(uint32_t i = 0; i < entry_num; i++){
          table->dataAllocate(&data[i]);
          table->tableEntryGet(*session, dev_tgt, *(keys[i].get()), bfrt::BfRtTable::BfRtTableGetFlag::GET_FROM_SW, data[i].get());
        }
        session->sessionCompleteOperations();
        for(uint32_t i = 0; i < entry_num; i ++){
          data[i]->getValue(data_id, &dt);//??
          std::cout<<"read register dt size: "<<dt.size()<<std::endl;
          if(i == 0){
            std::cout<<dt[0]<<std::endl;
          }
          dt.clear();
        }
      }

      void read_sumax_counter(const bfrt::BfRtTable* table, std::vector<std::unique_ptr<BfRtTableKey>>& keys, std::vector<std::unique_ptr<BfRtTableData>>& data, bf_rt_id_t& sum_id, bf_rt_id_t& max_id, uint32_t entry_num, int layer){
        std::vector<uint64_t> dt;

        for(uint32_t i = 0; i < entry_num; i++){
          table->dataAllocate(&data[i]);
          table->tableEntryGet(*session, dev_tgt, *(keys[i].get()),bfrt::BfRtTable::BfRtTableGetFlag::GET_FROM_SW,data[i].get());
        }

        session->sessionCompleteOperations();

        for(uint32_t i = 0; i < entry_num; i++){
          data[i]->getValue(sum_id, &dt);
          sum_counter[layer][i] = dt[0];
          data[i]->getValue(max_id, &dt);
          max_counter[layer][i] = dt[2];
          int sum_val = (int)dt[0];
          int max_val = (int)dt[2];
          SuMax_vector.push_back(std::make_pair(sum_val, max_val));
        }

        printf("Sumax layer: %d", layer);

        table->tableClear(*session, dev_tgt);
        session->sessionCompleteOperations();

      }

      void read_HH_reg_counter(const bfrt::BfRtTable* table, std::vector<std::unique_ptr<BfRtTableKey>>& keys, std::vector<std::unique_ptr<BfRtTableData>>& data, bf_rt_id_t& data_id, bf_rt_id_t& data_id2, uint32_t entry_num){
        std::vector<uint64_t> dt;

        for (uint32_t i=0;i<entry_num;i++)
        {
        table->dataAllocate(&data[i]);
        table->tableEntryGet(*session,dev_tgt,*(keys[i].get()),bfrt::BfRtTable::BfRtTableGetFlag::GET_FROM_SW,data[i].get());
        }
          
        session->sessionCompleteOperations();
        for (uint32_t i=0;i<entry_num;i++)
        {
          data[i]->getValue(data_id,&dt);

          data[i]->getValue(data_id2,&dt);

          uint64_t key_value = dt[0];
          if(key_value != 0){
            HH_flow[key_value] = dt[2];
          }

          dt.clear();

        }
        std::cout<<"HH_flow vector size: "<<HH_flow.size()<<std::endl;

        
        table->tableClear(*session, dev_tgt);
        session->sessionCompleteOperations();

      }

      void flow_reg_update(const bfrt::BfRtTable* table, std::vector<std::unique_ptr<BfRtTableKey>>& keys, std::vector<std::unique_ptr<BfRtTableData>>& data, 
                            bf_rt_id_t& data_id, uint32_t entry_num, std::vector<uint64_t>& flow_table_counter, uint64_t flow_table_index){
        for (uint32_t i=0;i<entry_num;i++)
        {
        table->dataAllocate(&data[i]);
        table->tableEntryGet(*session,dev_tgt,*(keys[i].get()),bfrt::BfRtTable::BfRtTableGetFlag::GET_FROM_SW,data[i].get());
        }

        for (uint32_t i = 0; i < flow_table_counter.size(); i++){
          uint64_t flow_table_value = flow_table_counter[i];
          data[flow_table_index]->setValue(data_id, flow_table_value);
          table->tableEntryMod(*session, dev_tgt, *(keys[flow_table_index].get()), *(data[flow_table_index].get()));
          flow_table_index ++;
        }
        session->sessionCompleteOperations();
      }


      void stats_update_cb_HH_reg_to(const bf_rt_target_t &dev_tgt, void *cookie){
          (void) dev_tgt;
          (void) cookie;
          read_HH_reg_counter(HH_reg_table, HH_reg_key, HH_reg_data, HH_reg_data_id, HH_reg_data_id2, HH_num);//??
          return;
      }

      void stats_update_cb_sumax1_reg_to(const bf_rt_target_t &dev_tgt, void *cookie){
        (void) dev_tgt;
        (void) cookie;
        read_sumax_counter(SuMax_1_table, sumax_1_key, sumax_1_data, sumax_1_data_id, sumax_1_data_id2, counter_num, 0);
      }

      void stats_update_cb_sumax2_reg_to(const bf_rt_target_t &dev_tgt, void *cookie){
        (void) dev_tgt;
        (void) cookie;
        read_sumax_counter(SuMax_2_table, sumax_2_key, sumax_2_data, sumax_2_data_id, sumax_2_data_id2, counter_num, 1);
      }

      void stats_update_cb_sumax3_reg_to(const bf_rt_target_t &dev_tgt, void *cookie){
        (void) dev_tgt;
        (void) cookie;
        read_sumax_counter(SuMax_3_table, sumax_3_key, sumax_3_data, sumax_3_data_id, sumax_3_data_id2, counter_num, 2);
      }

      void stats_update_cb_flow_reg_to(const bf_rt_target_t &dev_tgt, void *cookie){
        (void) dev_tgt;
        (void) cookie;
        // read_sumax_counter(SuMax_3_table, sumax_3_key, sumax_3_data, sumax_3_data_id, sumax_3_data_id2, counter_num, 2);
        flow_reg_update(flow_reg_table, flow_reg_key, flow_reg_data, flow_reg_data_id, flow_table_size, flow_reg_f1, flow_reg_index);
      }

      void cb_init(){
          HH_reg_table->operationsAllocate(bfrt::TableOperationsType::REGISTER_SYNC, &HH_reg_to);
          HH_reg_to->registerSyncSet(*session, dev_tgt, stats_update_cb_HH_reg_to, NULL);

          SuMax_1_table->operationsAllocate(bfrt::TableOperationsType::REGISTER_SYNC, &sumax_1_to);
          sumax_1_to->registerSyncSet(*session, dev_tgt, stats_update_cb_sumax1_reg_to, NULL);

          SuMax_2_table->operationsAllocate(bfrt::TableOperationsType::REGISTER_SYNC, &sumax_2_to);
          sumax_2_to->registerSyncSet(*session, dev_tgt, stats_update_cb_sumax2_reg_to, NULL);

          SuMax_3_table->operationsAllocate(bfrt::TableOperationsType::REGISTER_SYNC, &sumax_3_to);
          sumax_3_to->registerSyncSet(*session, dev_tgt, stats_update_cb_sumax3_reg_to, NULL);

          flow_reg_table->operationsAllocate(bfrt::TableOperationsType::REGISTER_SYNC, &flow_reg_to);
          flow_reg_to->registerSyncSet(*session, dev_tgt, stats_update_cb_flow_reg_to, NULL);


      }

      void sync_for_reg(const bfrt::BfRtTable* table, std::unique_ptr<bfrt::BfRtTableOperations>& table_operation)
      {
        table->tableOperationsExecute(*table_operation.get());
        session->sessionCompleteOperations();
      }


    }
  }
}


int sendSumMax(std::vector<sumMax> & data, int sockfd) {
    //sumMax: 4 bytes sum + 4 bytes max
    int n = data.size();
    if(n == 0) {
        return -1;
    }
    printf("n: %d\n",n);

    int p_vector = 0, p_buffer = 0;      //vector pointer
    //512 bytes, 64 pairs   
    int packet_num = (n-1)/64 + 1;
    char buf[512] = {};

    //send packet num
    memset(buf, 0, 512);
    memcpy(buf,&n, sizeof(n));
    if(send(sockfd, buf, sizeof(n), 0) <= 0) {
        printf("send num error\n");
        return -1;
    }

    //send packets
    for(int i = 0; i < packet_num; i += 1) {
        int len = 0;
        p_buffer = 0;
        memset(buf, 0, 512);
        if(i == packet_num - 1) {
            len = (n-1)%64 + 1;
        }
        else {
            len = 64;
        }
        
        printf("send turn %d, len %d\n", i,len);

        for(int j = 0; j < len; ++j) {
            memcpy(buf+p_buffer, &(data[p_vector].first), sizeof(int));
            memcpy(buf+p_buffer+sizeof(int), &(data[p_vector].second), sizeof(int));
            p_vector++;
            p_buffer += 2*sizeof(int);
        }

        if(send(sockfd, buf, len*8, 0) <= 0) {
            printf("send error\n");
            return -1;
        }
    }
    
    return 0;
}

int receiveHashRange(int sockfd, std::vector<idRange> & data) {
    int n = 0;
    char buf[384] = {};
    memset(buf,0, sizeof(buf));
    if(recv(sockfd, buf, sizeof(n),0) <= 0) {
        printf("receive error\n");
        return -1;
    }

    //receive n
    memcpy(&n, buf, sizeof(uint16_t));
    int packet_num = (n-1)/32 + 1;
    printf("n: %d\n",n);

    //receive data
    for(int i = 0; i < packet_num; i += 1) {
        memset(buf, 0, 384);
        int len = 0;
        if(i == packet_num - 1) {
            len = (n-1)%32 + 1;
        }
        else {
            len = 32;
        }
        printf("receive turn %d, len %d\n", i,len);

        if(recv(sockfd, buf, len*12, 0) <= 0) {
            printf("receive error\n");
            return -1;
        }

        int p_buffer = 0;
        uint32_t tmp_src_ip, tmp_dst_ip;
        uint16_t tmp_id_1, tmp_id_2;
        for(int j = 0; j < len; ++j){
            memcpy(&tmp_src_ip,buf+p_buffer,sizeof(uint32_t));
            memcpy(&tmp_dst_ip,buf+p_buffer+sizeof(uint32_t),sizeof(uint32_t));
            memcpy(&tmp_id_1, buf+p_buffer+2*sizeof(uint32_t), sizeof(uint16_t));
            memcpy(&tmp_id_2, buf+p_buffer+2*sizeof(uint32_t)+sizeof(uint16_t), sizeof(uint16_t));
            p_buffer += 2*sizeof(uint16_t)+2*sizeof(uint32_t);
            if(tmp_dst_ip == 0 || tmp_id_1 == 0 || tmp_id_2 == 0){
                std::cout<<tmp_dst_ip<<" "<<tmp_id_1<<' '<<tmp_id_2<<std::endl;
            }
            data.push_back(idRange{tmp_src_ip,tmp_dst_ip,tmp_id_1,tmp_id_2});
        }
    }
    return 0;
}


void hash_match(std::vector<idRange> range_id){
        // addr_match_table->tableClear();
    cons_hash_table->tableClear(*session, dev_tgt);

    for(auto item = range_id.begin(); item != range_id.end(); item++){
        cons_hash_table->keyAllocate(&cons_hash_key);
        cons_hash_table->dataAllocate(&cons_hash_data);
        cons_hash_table->keyReset(cons_hash_key.get());
        cons_hash_table->dataReset(cons_hash_action_id, cons_hash_data.get());
        cons_hash_key->setValue(cons_hash_key_addr, (uint64_t)item->dst_ip);
        cons_hash_key->setValueRange(cons_hash_key_index, (uint64_t)item->range_begin, (uint64_t)item->range_end);
        uint64_t match = 1;
        cons_hash_data->setValue(cons_hash_data_id, match);
        cons_hash_table->tableEntryAdd(*session, dev_tgt, *cons_hash_key, *cons_hash_data);
    }
    session->endBatch(true);
    session->sessionCompleteOperations();
}

void time_now(){
    typedef std::chrono::time_point<std::chrono::system_clock, std::chrono::microseconds> microClock_type;
    microClock_type tp = std::chrono::time_point_cast<std::chrono::microseconds>(std::chrono::system_clock::now());
    std::cout<<tp.time_since_epoch().count()<<" ns"<<std::endl;
}
void receiveNewRange(int sockfd, std::vector<idRange> & data) {
    data.clear();
    receiveHashRange(sockfd,data);
    time_now();
    hash_match(data);
    time_now();
    std::cout<<"range size: "<<data.size()<<std::endl;
    // for(auto range_item = data.begin(); range_item != data.end(); range_item++){
    //   std::cout<<range_item->dst_ip<<" "<<range_item->range_begin<<" "<<range_item->range_end<<std::endl;
    // }
}



static void parse_options(bf_switchd_context_t *switchd_ctx,
                          int argc,
                          char **argv){
    int option_index = 0;
    char* port_input;
    enum opts{
      OPT_INSTALLDIR = 1,
      OPT_CONFFILE,
      OPT_BATCH,
    };
    static struct option options[]={
      {"help", no_argument,0,'h'},
      {"port", required_argument, 0, 'p'},
      {"install_dir", required_argument, 0, OPT_INSTALLDIR},
      {"conf_file", required_argument, 0, OPT_CONFFILE}
    };

    while(1){
      int c = getopt_long(argc, argv,"h", options, &option_index);

      if (c == -1){
        break;
      }

      switch (c)
      {
      case OPT_INSTALLDIR:
        /* code */
        switchd_ctx->install_dir = strdup(optarg);
        printf("Install Dir: %s\n", switchd_ctx->install_dir);
        break;
      case OPT_CONFFILE:
        switchd_ctx->conf_file = strdup(optarg);
        printf("Conf-file: %s\n", switchd_ctx->conf_file);
        break;
      case 'p':
        port_input = strdup(optarg);
        sscanf(port_input, "%d", &server_port);
        // std::cout<<"server port: "<<server_port<<std::endl;
        printf("server port: %d", server_port);
        break;
      case 'h':
      case '?':
        printf("bfrt_pref \n");
        printf("Usage : bfrt_perf --install-dir <path to where the SDE is "
            "installed> --conf-file <full path to the conf file "
            "(bfrt_perf.conf)\n");
        exit(c == 'h' ? 0 : 1);
        break;
      default:
        printf("Invalid option \n");
        break;
      }
    }

    if(switchd_ctx->install_dir == NULL){
      printf("ERROR: --install-dir must be specified \n");
      exit(0);
    }

    if(switchd_ctx->conf_file == NULL){
      printf("ERROR: --conf-file must be specfied\n");
      exit(0);
    }
}

int main(int argc, char**argv){
    bf_switchd_context_t *switchd_ctx;

    if((switchd_ctx = (bf_switchd_context_t *) calloc(
            1, sizeof(bf_switchd_context_t))) == NULL){
        printf("cannot Allocate switchd context\n");
        exit(1);
    }
    std::cout<<"start parse_option"<<std::endl;

    parse_options(switchd_ctx, argc, argv);

    switchd_ctx->running_in_background = true;

    bf_status_t status = bf_switchd_lib_init(switchd_ctx);

    init_ports();

    bfrt::examples::cons_hash::init();
    std::cout<<"init complete"<<std::endl;

    bfrt::examples::cons_hash::register_init(flow_reg_table, flow_reg_key, flow_reg_data, "Ingress.flow_reg.f1", flow_table_size, flow_reg_data_id);
    
    bfrt::examples::cons_hash::register_init_counter(SuMax_1_table, sumax_1_key, sumax_1_data, "Ingress.sumax_1.sum_value", "Ingress.sumax_1.max_value", counter_num, sumax_1_data_id, sumax_1_data_id2);
    bfrt::examples::cons_hash::register_init_counter(SuMax_2_table, sumax_2_key, sumax_2_data, "Ingress.sumax_2.sum_value", "Ingress.sumax_2.max_value", counter_num, sumax_2_data_id, sumax_2_data_id2);
    bfrt::examples::cons_hash::register_init_counter(SuMax_3_table, sumax_3_key, sumax_3_data, "Ingress.sumax_3.sum_value", "Ingress.sumax_3.max_value", counter_num, sumax_3_data_id, sumax_3_data_id2);

    bfrt::examples::cons_hash::register_init_counter(HH_reg_table, HH_reg_key, HH_reg_data, "Ingress.HH_reg.id", "Ingress.HH_reg.counter", HH_num, HH_reg_data_id, HH_reg_data_id2);
    
    std::cout<<"*******************"<<std::endl;
    std::cout<<"flow table key id: "<<flow_table_key_id<<" data id: "<<flow_table_data_id<<" action add id: "<<flow_table_add_id<<std::endl;

    bfrt::examples::cons_hash::cb_init();

    /************** R E C E V I E    R A N G E******************/
    int sockfd = socket(AF_INET, SOCK_STREAM,0);

    struct sockaddr_in serveraddr;
    memset(&serveraddr, 0, sizeof(serveraddr));
    serveraddr.sin_family = AF_INET;
    serveraddr.sin_addr.s_addr = htonl(server_ip);
    serveraddr.sin_port = htons(server_port);
    if(connect(sockfd, (struct sockaddr *)&serveraddr, sizeof(serveraddr)) != 0) {
        printf("connect failed\n");
        close(sockfd);
        return -1;
    }

    std::vector<idRange> data;
    receiveNewRange(sockfd, data);

    close(sockfd);
    printf("connect success\n");

//    sleep(1000);

  return status;


}
