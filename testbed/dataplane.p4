#include <core.p4>
#include <tna.p4>

#define freq_thres 100

enum bit<16> ether_type_t {
    TPID       = 0x8100,
    IPV4       = 0x0800,
    ARP        = 0x0806
}


enum bit<8>  ip_proto_t {
    ICMP  = 1,
    IGMP  = 2,
    TCP   = 6,
    UDP   = 17
}

type bit<48> mac_addr_t;


/*************************************************************************
 ***********************  H E A D E R S  *********************************
 *************************************************************************/
/*  Define all the headers the program will recognize             */
/*  The actual sets of headers processed by each gress can differ */

/* Standard ethernet header */
header ethernet_h {
    mac_addr_t    dst_addr;
    mac_addr_t    src_addr;
    ether_type_t  ether_type;
}

header vlan_tag_h {
    bit<3>        pcp;
    bit<1>        cfi;
    bit<12>       vid;
    ether_type_t  ether_type;
}

header arp_h {
    bit<16>       htype;
    bit<16>       ptype;
    bit<8>        hlen;
    bit<8>        plen;
    bit<16>       opcode;
    mac_addr_t    hw_src_addr;
    bit<32>       proto_src_addr;
    mac_addr_t    hw_dst_addr;
    bit<32>       proto_dst_addr;
}

header ipv4_h {
    bit<4>       version;
    bit<4>       ihl;
    bit<6>       dscp;
    bit<2>       ecn;
    bit<16>      total_len;
    bit<16>      identification;
    bit<1>       res;
    bit<2>       flags;
    bit<13>      frag_offset;
    bit<8>       ttl;
    bit<8>       protocol;
    bit<16>      hdr_checksum;
    bit<32>      src_addr;
    bit<32>      dst_addr;
}

header icmp_h {
    bit<16>  type_code;
    bit<16>  checksum;
}

header igmp_h {
    bit<16>  type_code;
    bit<16>  checksum;
}

header tcp_h {
    bit<16>  src_port;
    bit<16>  dst_port;
    bit<32>  seq_no;
    bit<32>  ack_no;
    bit<4>   data_offset;
    bit<4>   res;
    bit<8>   flags;
    bit<16>  window;
    bit<16>  checksum;
    bit<16>  urgent_ptr;
}

header udp_h {
    bit<16>  src_port;
    bit<16>  dst_port;
    bit<16>  len;
    bit<16>  checksum;
}

// header mirror_h {
//     bit<48> dst_addr;
//     bit<48> src_addr;
//     bit<16> ether_type;
//     bit<16> index;
// }

// header clear_h {
//     bit<16> index;
// }

/*************************************************************************
 **************  I N G R E S S   P R O C E S S I N G   *******************
 *************************************************************************/
  
    /***********************  H E A D E R S  ************************/

struct my_ingress_headers_t {
    ethernet_h         ethernet;
    arp_h              arp;
    vlan_tag_h[2]      vlan_tag;
    ipv4_h             ipv4;
    icmp_h             icmp;
    igmp_h             igmp;
    tcp_h              tcp;
    udp_h              udp;
}

    /******  G L O B A L   I N G R E S S   M E T A D A T A  *********/
struct my_ingress_metadata_t{
    bit<32>         s_d_port;
    bit<16>         index_1;
    bit<16>         index_2;
    bit<16>         index_3;
    bit<16>          freq_1;
    bit<16>         freq_2;
    bit<16>         freq_3;
    bit<16>         min1;
    bit<16>         min2;
    bit<16>         w;
    bit<16>          into_sketch;
    bit<1>          cal_switch;
    bit<16>         flag;
    bit<1>          ecmp_select;
}

struct id_pair{
    bit<32>         counter;
    bit<32>         id;
}

struct sumax{
    bit<16>         sum_value;
    bit<16>         max_value;
}

 /***********************  P A R S E R  **************************/

parser IngressParser(packet_in        pkt,
    /* User */
    out my_ingress_headers_t          hdr,
    out my_ingress_metadata_t         meta,
    /* Intrinsic */
    out ingress_intrinsic_metadata_t  ig_intr_md)
{
    state start {
        pkt.extract(ig_intr_md);
        pkt.advance(PORT_METADATA_SIZE);
        transition meta_init;
    }

    state meta_init {
        meta.index_1 = 0;
        meta.index_2 = 0;
        meta.index_3= 0;
        meta.s_d_port = 0;
        meta.freq_1 = 0;
        meta.freq_2 = 0;
        meta.freq_3 = 0;
        meta.min1 = 0;
        meta.min2 = 0;
        meta.w = 65535;
        meta.into_sketch = 0;
        meta.cal_switch = 0;
        meta.flag = 0;
        meta.ecmp_select = 0;
        transition parse_ethernet; 
    }

    state parse_ethernet {
        pkt.extract(hdr.ethernet);
        meta.flag =(bit<16>)hdr.ethernet.ether_type;
        transition select((bit<16>)hdr.ethernet.ether_type) {
            (bit<16>)ether_type_t.TPID &&& 0xEFFF :  parse_vlan_tag;
            (bit<16>)ether_type_t.IPV4            :  parse_ipv4;
            (bit<16>)ether_type_t.ARP             :  parse_arp;
            // 0x0805                                :  parse_arp;
            // (bit<16>)ether_type_t.CLEAR           :  parse_clear;
            default :  accept;
        }
    }

    // state parse_clear{
    //     pkt.extract(hdr.clear);
    //     transition accept;
    // }

    state parse_arp {
        // meta.flag = 3;
        pkt.extract(hdr.arp);
        transition  accept;
    }

    state parse_vlan_tag {
        pkt.extract(hdr.vlan_tag.next);
        transition select(hdr.vlan_tag.last.ether_type) {
            ether_type_t.TPID :  parse_vlan_tag;
            ether_type_t.IPV4 :  parse_ipv4;
            default: accept;
        }    
    }

    state parse_ipv4 {
        // meta.flag = 4;
        pkt.extract(hdr.ipv4);
        transition select(hdr.ipv4.protocol) {
            1 : parse_icmp;
            2 : parse_igmp;
            6 : parse_tcp;
           17 : parse_udp;
            default : accept;
        }    
    }

    state parse_icmp {
        meta.s_d_port = pkt.lookahead<bit<32>>();
        pkt.extract(hdr.icmp);
        transition accept;
    }

    state parse_igmp {
        meta.s_d_port = pkt.lookahead<bit<32>>();
        pkt.extract(hdr.igmp);
        transition accept;  
    }

    state parse_tcp {
        // meta.flag = 5;
        meta.s_d_port = pkt.lookahead<bit<32>>();
        pkt.extract(hdr.tcp);
        transition accept;
    }

    state parse_udp {
        meta.s_d_port = pkt.lookahead<bit<32>>();
        pkt.extract(hdr.udp);
        transition accept;
    }

}

control Ingress(/* User */
    inout my_ingress_headers_t                       hdr,
    inout my_ingress_metadata_t                      meta,
    /* Intrinsic */
    in    ingress_intrinsic_metadata_t               ig_intr_md,
    in    ingress_intrinsic_metadata_from_parser_t   ig_prsr_md,
    inout ingress_intrinsic_metadata_for_deparser_t  ig_dprsr_md,
    inout ingress_intrinsic_metadata_for_tm_t        ig_tm_md)
{

    /******* B I G   F L O W   T A B L E ********/
    Register<bit<16>, bit<16>>(0x400)flow_reg;
    RegisterAction<bit<16>,bit<16>,bit<16>>(flow_reg) flow_reg_add = {
        void apply(inout bit<16> register_data, out bit<16> result){
            register_data = register_data + 1;
            result = register_data;
        }
    };

    action flow_count(bit<16> index){
        meta.into_sketch = flow_reg_add.execute(index);
    }
    table flow_table{
        key = {
            hdr.ipv4.dst_addr:  exact;
        }
        actions = {
            @defaultonly NoAction;
            flow_count;
        }
        size = 1024;
        const default_action = NoAction();
    }


    /******** T O W E R   S K E T C H ********/
    CRCPolynomial<bit<32>>(0x04C11DB7,false,false,false,32w0xFFFFFFFF,32w0xFFFFFFFF) crc32a;
    CRCPolynomial<bit<32>>(0x741B8CD7,false,false,false,32w0xFFFFFFFF,32w0xFFFFFFFF) crc32b;
    CRCPolynomial<bit<32>>(0xDB710641,false,false,false,32w0xFFFFFFFF,32w0xFFFFFFFF) crc32c;
    // CRCPolynomial<bit<32>>(0x82608EDB,false,false,false,32w0xFFFFFFFF,32w0xFFFFFFFF) crc32fp;

    Hash<bit<16>>(HashAlgorithm_t.CUSTOM, crc32a) hash_1;
    Hash<bit<16>>(HashAlgorithm_t.CUSTOM, crc32b) hash_2;
    Hash<bit<16>>(HashAlgorithm_t.CUSTOM, crc32c) hash_3;
    Hash<bit<1>>(HashAlgorithm_t.CUSTOM, crc32a) hash_ecmp;

    Register<sumax, bit<16>>(0x10000)sumax_1;
    RegisterAction<sumax,bit<16>,bit<16>>(sumax_1) sum_layer1 = {
        void apply(inout sumax register_data, out bit<16> result){
            if(register_data.sum_value < meta.w){
                register_data.sum_value = register_data.sum_value + 1;
            }
            if(register_data.max_value < hdr.ipv4.total_len){
                register_data.max_value = hdr.ipv4.total_len;
            }
            
            result = register_data.sum_value;
        }
    };

    Register<sumax,bit<16>>(0x10000)sumax_2;
    RegisterAction<sumax,bit<16>,bit<16>>(sumax_2) sum_layer2 = {
        void apply(inout sumax register_data, out bit<16> result){
            if(register_data.sum_value < meta.w){
                register_data.sum_value = register_data.sum_value + 1;
            }
            if(register_data.max_value < hdr.ipv4.total_len){
                register_data.max_value = hdr.ipv4.total_len;
            }

            result = register_data.sum_value;
        }
    };

    Register<sumax,bit<16>>(0x10000)sumax_3;
    RegisterAction<sumax,bit<16>,bit<16>>(sumax_3) sum_layer3 = {
        void apply(inout sumax register_data, out bit<16> result){
            if(register_data.sum_value < meta.w ){
                register_data.sum_value = register_data.sum_value + 1;
            }
            if(register_data.max_value < hdr.ipv4.total_len){
                register_data.max_value = hdr.ipv4.total_len;
            }
            
            result = register_data.sum_value;
        }
    };

    action cal_hash_index(){
        meta.index_1 = hash_1.get({hdr.ipv4.src_addr, hdr.ipv4.dst_addr, hdr.ipv4.protocol, meta.s_d_port});
        // meta.index_16 = hash_16.get({hdr.ipv4.src_addr, hdr.ipv4.dst_addr, hdr.ipv4.protocol, meta.s_d_port});
        // meta.index_32 = hash_32.get({hdr.ipv4.src_addr, hdr.ipv4.dst_addr, hdr.ipv4.protocol, meta.s_d_port});
    }

    table cal_hash_index_t{
        actions = {
            cal_hash_index;
        }
        default_action = cal_hash_index;
    }

    action layer1_insert(){
        meta.w = sum_layer1.execute(meta.index_1);
    }

    table layer1_insert_t{
        actions = {
            layer1_insert;
        }
        default_action = layer1_insert;
    }

    action layer2_insert(){
        meta.freq_2 = sum_layer2.execute(meta.index_2);
    }
    table layer2_insert_t{
        actions = {
            layer2_insert;
        }
        default_action = layer2_insert;
    }

    action layer3_insert(){
        meta.freq_3 = sum_layer3.execute(meta.index_3);
    }
    table layer3_insert_t{
        actions = {
            layer3_insert;
        }
        default_action = layer3_insert;
    }

    action get_min1(){
        meta.w = min(meta.w, meta.freq_2);
    }
    table get_min1_t{
        actions = {
            get_min1;
        }
        default_action = get_min1;
    }

    action get_min2(){
        meta.w = min(meta.w, meta.freq_3);
    }
    table get_min2_t{
        actions = {
            get_min2;
        }
        default_action = get_min2;
    }

    /***********H E A V Y   H I T T E R  H A S H  T A B L E  *************/

    Register<bit<16>,bit<16>>(0x1)cmp_thres;
    RegisterAction<bit<16>,bit<16>,bit<16>>(cmp_thres)HH_identify=
    {
        void apply(inout bit<16> register_data, out bit<16> result){
            if(register_data > 65534)
                register_data = 1;
            else{
                register_data = register_data + 1;
            }
            result = register_data;
        }
    };
    bit<16> HH_index;
    action cmp_HH(){
        HH_index = HH_identify.execute(0);
    }
    // table cmp_HH_t{
    //     actions = {
    //         cmp_HH;
    //     }
    //     default_action = cmp_HH;
    // }

    table HH_match_t{
        actions = {
            cmp_HH;
            @defaultonly NoAction;
        }
        key = {
            meta.w:   exact;
        }
        size = 100;
        default_action = NoAction();
        const entries = {
            1100: cmp_HH();
            1200: cmp_HH();
            1300: cmp_HH();
        }
    }

    Register<id_pair,bit<16>>(0x10000)HH_reg;
    RegisterAction<id_pair,bit<16>,bit<16>>(HH_reg) HH_insert=
    {
        void apply(inout id_pair register_data, out bit<16> result){
            register_data.id = hdr.ipv4.dst_addr;
            register_data.counter =(bit<32>)meta.w;
            // register_data.id = 1000;
            // register_data.counter = 2000;
            result = 1;
        }
        
    };


    action HH_insert_a(){
        HH_insert.execute(HH_index);
    }
    table HH_insert_t{
        actions = {
            HH_insert_a;
        }
        default_action = HH_insert_a;
    }

    /***debug area start****/
    action HH_test_a(){
        HH_insert.execute(0);
    }

    table HH_test_t{
        actions = {
            HH_test_a;
        }
        default_action = HH_test_a;
    }
    /****debug area end****/
    // action flag_id(bit<16> flag){
    //     meta.flag = flag;
    // }

    // table addr_match_t{
    //     key = {
    //         hdr.ipv4.dst_addr: exact;
    //     }
    //     actions = {
    //         flag_id;
    //         NoAction;
    //     }
    //     size = 1024;
    //     default_action = NoAction();
    // }

    action con_hash_a(bit<1> match){
        meta.cal_switch = match;
    }

    table con_hash_t{
        key = {
            hdr.ipv4.dst_addr: exact;
            meta.index_1:  range;
        } 
        actions = {
            con_hash_a;
            NoAction;
        }
        size = 10000;
        default_action = NoAction();
    }

    action unicast_send(PortId_t port) {
        ig_tm_md.ucast_egress_port = port;
        ig_tm_md.bypass_egress=1;
    }

    action drop(){
        ig_dprsr_md.drop_ctl = 1;
    }

    table arp_host{
        actions = {
            unicast_send;
            drop;
        }
        key = {
            hdr.arp.proto_dst_addr:   exact;
        }
        default_action = drop;
        size = 100;
    }

    action ecmp_set(){
        meta.ecmp_select = hash_ecmp.get({hdr.ipv4.src_addr, hdr.ipv4.dst_addr, hdr.ipv4.protocol, meta.s_d_port});
    }

    table ecmp_select_t{
        actions = {
            ecmp_set;
        }
        default_action = ecmp_set;
        size = 1024;
    }

    action forward(PortId_t port){
        ig_tm_md.ucast_egress_port = port;
        hdr.ipv4.ttl = hdr.ipv4.ttl - 1;
    }

    table ipv4_host{
        key = {
            hdr.ipv4.dst_addr:      exact;
            meta.ecmp_select:      exact;
        }

        actions = {
            drop;
            forward;
        }

        default_action = drop();
        size = 1024;

    }
    Register<bit<32>,bit<16>>(0x1)debug_reg;
    RegisterAction<bit<32>,bit<16>,bit<32>>(debug_reg)debug_test=
    {
        void apply(inout bit<32> register_data, out bit<32> result){
            // register_data =(bit<16>)ig_tm_md.ucast_egress_port;
            // register_data = register_data + 1;
            register_data = (bit<32>)meta.flag;
        }
    };

    action debug_action(){
        debug_test.execute(0);
    }

    table debug_t{
        actions = {
            debug_action;
        }
        default_action = debug_action();
    }
bit<1> prob_delay_flag = 0;
    action prob_delay_a(){
        prob_delay_flag = 1;

    }

    table prob_delay_t{
        key = {
            meta.index_1:   range;
        } 
        actions = {
            prob_delay_a;
            NoAction;
        }
        default_action = NoAction();

    }

    
    apply{ 
        if(hdr.arp.isValid()){
            arp_host.apply();
        }
        
        else if(hdr.ipv4.isValid()){
            ecmp_select_t.apply();
            ipv4_host.apply();
        
            debug_t.apply();
            prob_delay_t.apply();
            cal_hash_index_t.apply();

            con_hash_t.apply();
            // if(prob_delay_flag == 1){
            
                if((prob_delay_flag == 1 && meta.cal_switch == 1) || hdr.ipv4.res == 1){

                    flow_table.apply();
                    if(meta.into_sketch != 0){
                        
                        meta.index_2 = hash_2.get({hdr.ipv4.src_addr, hdr.ipv4.dst_addr, hdr.ipv4.protocol, meta.s_d_port});
                        meta.index_3 = hash_3.get({hdr.ipv4.src_addr, hdr.ipv4.dst_addr, hdr.ipv4.protocol, meta.s_d_port});
                        layer1_insert_t.apply();
                        layer2_insert_t.apply();

                        get_min1_t.apply();
                        // layer3_insert_t.apply();

                        // get_min2_t.apply();
                        HH_index = 0;
                        HH_match_t.apply();
                        if(HH_index > 0)
                            HH_insert_t.apply();
                    }
                    hdr.ipv4.res = 0;
                }
            // }
                else if(prob_delay_flag == 0){
                    hdr.ipv4.res = 1;
                }

        }
    }

}

control IngressDeparser(packet_out pkt,
    /* User */
    inout my_ingress_headers_t                       hdr,
    in    my_ingress_metadata_t                      meta,
    /* Intrinsic */
    in    ingress_intrinsic_metadata_for_deparser_t  ig_dprsr_md)
{
    apply{
        pkt.emit(hdr);
    }
}

/*************************************************************************
 ****************  E G R E S S   P R O C E S S I N G   *******************
 *************************************************************************/

    /***********************  H E A D E R S  ************************/

struct my_egress_headers_t {
}

    /********  G L O B A L   E G R E S S   M E T A D A T A  *********/

struct my_egress_metadata_t {
}

    /***********************  P A R S E R  **************************/

parser EgressParser(packet_in        pkt,
    /* User */
    out my_egress_headers_t          hdr,
    out my_egress_metadata_t         meta,
    /* Intrinsic */
    out egress_intrinsic_metadata_t  eg_intr_md)
{
    /* This is a mandatory state, required by Tofino Architecture */
    state start {
        pkt.extract(eg_intr_md);
        transition accept;
    }
}

    /***************** M A T C H - A C T I O N  *********************/

control Egress(
    /* User */
    inout my_egress_headers_t                          hdr,
    inout my_egress_metadata_t                         meta,
    /* Intrinsic */    
    in    egress_intrinsic_metadata_t                  eg_intr_md,
    in    egress_intrinsic_metadata_from_parser_t      eg_prsr_md,
    inout egress_intrinsic_metadata_for_deparser_t     eg_dprsr_md,
    inout egress_intrinsic_metadata_for_output_port_t  eg_oport_md)
{
    apply {
    }
}

    /*********************  D E P A R S E R  ************************/

control EgressDeparser(packet_out pkt,
    /* User */
    inout my_egress_headers_t                       hdr,
    in    my_egress_metadata_t                      meta,
    /* Intrinsic */
    in    egress_intrinsic_metadata_for_deparser_t  eg_dprsr_md)
{
    apply {
        pkt.emit(hdr);
    }
}


/************ F I N A L   P A C K A G E ******************************/
Pipeline(
    IngressParser(),
    Ingress(),
    IngressDeparser(),
    EgressParser(),
    Egress(),
    EgressDeparser()
) pipe;

Switch(pipe) main;