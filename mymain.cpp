#include <ctime>
#include <limits>
#include "data_loader.h"
#include "solution.h"
#include <set>
#include <string>
#include <iostream>
#include <fstream>
#include <numeric>
#include <chrono>

using namespace std::chrono;
using namespace std;


std::set<std::vector<int>> dedup(std::vector<join_result> answer, int durability) {
    set<vector<int>> distinct;
    size_t orig_answer_size = answer.size();
    
    clock_t ts = clock();
    for (auto tup : answer) {
        if (tup.t_end - tup.t_start >= durability) {
            std::vector<int> row;
            for (auto a : tup.attrs) {
                row.push_back(a);
            }
            row.push_back(tup.t_start);
            row.push_back(tup.t_end);
            distinct.insert(row);
        }
    }
    size_t deduped_size = distinct.size();
    clock_t te = clock();

    std::cerr << "original answer size " << orig_answer_size << std::endl;
    std::cerr << "deduped answer size " << deduped_size << std::endl;
    std::cerr << "deduplication time " << (((double) te - ts) / CLOCKS_PER_SEC) << std::endl;
    return distinct;
}

void dump_csv(std::set<std::vector<int>> distinct) {
    std::string csv_fname = "out.csv";
    std::ofstream csv_out(csv_fname);
    if (!csv_out.is_open()) {
        std::cerr << "failed to open " << csv_fname << '\n';
    }
    for (auto tup : distinct) {
        for (int i=0; i<tup.size()-1; i++) {
            csv_out << tup[i] << ", ";
        }
        csv_out << tup[tup.size() - 1] << std::endl;
    }
    csv_out.close();
}

void bowtie(std::string path, int durability, bool csv) {
    clock_t ts, te;
    TableLoader tl;
    tl.load_test_table(0, path, 2);
    tl.load_test_table(1);
    tl.load_test_table(2);
    tl.load_test_table(3);
    tl.load_test_table(4);
    tl.load_test_table(5);

    vector<int> join_order = vector<int>{0,1,4,2,3,5};
    map<int, vector<int>> join_attrs;
    map<int, vector<join_result>> join_tables;
    int total_num_attrs = 5;
    
    // global join attrs id:
    // A:0, B:1, C:2, D:3, E:4
    join_attrs[0] = vector<int>{0,1}; // AB
    join_attrs[1] = vector<int>{0,2}; // AC
    join_attrs[2] = vector<int>{0,3}; // AD
    join_attrs[3] = vector<int>{0,4}; // AE
    join_attrs[4] = vector<int>{1,2}; // BC
    join_attrs[5] = vector<int>{3,4}; // DE

    ts = clock();
    join_tables[0] = tl.prepare(0, total_num_attrs, vector<int>{0,1}, join_attrs[0], durability);
    join_tables[1] = tl.prepare(1, total_num_attrs, vector<int>{0,1}, join_attrs[1], durability);
    join_tables[2] = tl.prepare(2, total_num_attrs, vector<int>{0,1}, join_attrs[2], durability);
    join_tables[3] = tl.prepare(3, total_num_attrs, vector<int>{0,1}, join_attrs[3], durability);
    join_tables[4] = tl.prepare(4, total_num_attrs, vector<int>{0,1}, join_attrs[4], durability);
    join_tables[5] = tl.prepare(5, total_num_attrs, vector<int>{0,1}, join_attrs[5], durability);


    te = clock();
    clock_t filter_time = te - ts;

    ts = clock();
    Solution durable_join(total_num_attrs);
    durable_join.setup_table_index(join_order, join_tables);
    durable_join.setup_hash_index(join_order, join_attrs, join_tables);
    te = clock();
    double index_time = ((double) te - ts) / CLOCKS_PER_SEC;

    ts = clock();
    vector<int> join_order_part_1 = {0,1,4};
    map<int, vector<join_result>> join_tables_part_1;
    map<int, vector<int>> join_attrs_part_1;
    for (int id : join_order_part_1) {
        join_tables_part_1[id] = join_tables[id];
        join_attrs_part_1[id] = join_attrs[id];
    }
    vector<int> join_order_part_2 = {2,3,5};
    map<int, vector<join_result>> join_tables_part_2;
    map<int, vector<int>> join_attrs_part_2;
    for (int id : join_order_part_2) {
        join_tables_part_2[id] = join_tables[id];
        join_attrs_part_2[id] = join_attrs[id];
    }
    std::vector<join_result> final_answer;
    std::vector<join_result> part_1 = durable_join.durable_generic_join(join_tables_part_1, join_order_part_1, join_attrs_part_1, 0, durability);
    std::cout << part_1.size() << std::endl;
    // part_1[0].print();
    std::vector<join_result> part_2 = durable_join.durable_generic_join(join_tables_part_2, join_order_part_2, join_attrs_part_2, 0, durability);
    std::cout << part_2.size() << std::endl;
    // part_2[0].print();
    if (!part_1.empty() && !part_2.empty()) {
        std::vector<int> common_attrs = get_intersection(part_1[0].attr_id, part_2[0].attr_id);
        cerr << "common attrs: ";
        for (auto attr : common_attrs) { cerr << " " << attr; }
        cerr << endl;

        std::vector<int> union_attrs = get_union(part_1[0].attr_id, part_2[0].attr_id);
        cerr << "union attrs: ";
        for (auto attr : union_attrs) { cerr << " " << attr; }
        cerr << endl;

        final_answer = durable_join.pairwise_forward_scan_temporal_join(part_1, part_2, common_attrs, union_attrs, durability);
    }

    set<vector<int>> distinct;
    for (auto tup : final_answer) {
        if (tup.t_end - tup.t_start >= durability) {
            std::vector<int> row;
            for (auto a : tup.attrs) {
                row.push_back(a);
            }
            row.push_back(tup.t_start);
            row.push_back(tup.t_end);
            distinct.insert(row);
        }
    }
    te = clock();
    double join_time = ((double) te - ts) / CLOCKS_PER_SEC;

    cerr << "Found " << distinct.size() << " matching tuples" << endl;
    cerr << "Index time " << index_time << endl;
    cerr << "Join time " << join_time << endl;

    if (csv) {
        std::string csv_fname = "out.csv";
        std::ofstream csv_out(csv_fname);
        if (!csv_out.is_open()) {
            std::cerr << "failed to open " << csv_fname << '\n';
        }
        for (auto tup : distinct) {
            for (int i=0; i<tup.size()-1; i++) {
                csv_out << tup[i] << ", ";
            }
            csv_out << tup[tup.size() - 1] << std::endl;
        }
        csv_out.close();
    }

    // ts = clock();
    // vector<join_result> baseline = 
    //     durable_join.multiway_durable_join_baseline(join_tables, join_order, join_attrs, durability, -1);
    // te = clock();
    // cout << "ground truth size: " << baseline.size() << ' '
    //         << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << ' '
    //         << "filter time: " << (double) filter_time / CLOCKS_PER_SEC << endl;
}

void triangles(std::string path, int durability, bool temporal, bool csv) {
    clock_t ts, te;

    // Load table
    TableLoader tl;
    // AB
    tl.load_test_table(0, path, 2);
    // BC
    tl.load_test_table(1, path, 2);
    // AC
    tl.load_test_table(2, path, 2);

    // Define the join order
    vector<int> join_order = vector<int>{0,1,2};

    // Define the join attributes and the tables
    map<int, vector<int>> join_attrs;
    map<int, vector<join_result>> join_tables;

    int total_num_attrs = 3;
    // Join attribute ids
    join_attrs[0] = vector<int>{0,1}; //AB
    join_attrs[1] = vector<int>{1,2}; //BC
    join_attrs[2] = vector<int>{0,2}; //AC
    
    ts = clock();
    join_tables[0] = tl.prepare(0, total_num_attrs, vector<int>{0,1}, join_attrs[0], durability);
    join_tables[1] = tl.prepare(1, total_num_attrs, vector<int>{0,1}, join_attrs[1], durability);
    join_tables[2] = tl.prepare(2, total_num_attrs, vector<int>{0,1}, join_attrs[2], durability);

    // Instantiate the algorithm
    Solution durable_join(total_num_attrs);
    
    // Setup the indices
    durable_join.setup_table_index(join_order, join_tables);
    durable_join.setup_hash_index(join_order, join_attrs, join_tables);
    te = clock();
    double time_index = ((double) te - ts) / CLOCKS_PER_SEC;

    // run the algorithm
    ts = clock();
    vector<join_result> answer = 
        durable_join.durable_generic_join(
            join_tables, join_order, join_attrs, 0, durability);

    // In order to have the correct count of output tuples, 
    // we must remove duplicates from the output
    set<vector<int>> distinct = dedup(answer, durability);
    te = clock();
    double time_join = ((double) te - ts) / CLOCKS_PER_SEC;

    cerr << "Found " << distinct.size() << " matching tuples" << endl;
    cerr << "Index time " << time_index << endl;
    cerr << "Join time " << time_join << endl;

    if (csv) {
        dump_csv(distinct);
    }
}

void cycle4(std::string path, int durability, bool temporal, bool csv) {
    clock_t ts, te;

    // Load table
    TableLoader tl;
    // AB
    tl.load_test_table(0, path, 2);
    // BC
    tl.load_test_table(1);
    // CD
    tl.load_test_table(2);
    // AD
    tl.load_test_table(3);

    // Define the join order
    vector<int> join_order = vector<int>{0,1,2,3};

    // Define the join attributes and the tables
    map<int, vector<int>> join_attrs;
    map<int, vector<join_result>> join_tables;

    int total_num_attrs = 4;
    // Join attribute ids
    join_attrs[0] = vector<int>{0,1}; //AB
    join_attrs[1] = vector<int>{1,2}; //BC
    join_attrs[2] = vector<int>{2,3}; //CD
    join_attrs[3] = vector<int>{0,3}; //AD
    
    ts = clock();
    join_tables[0] = tl.prepare(0, total_num_attrs, vector<int>{0,1}, join_attrs[0], durability);
    join_tables[1] = tl.prepare(1, total_num_attrs, vector<int>{0,1}, join_attrs[1], durability);
    join_tables[2] = tl.prepare(2, total_num_attrs, vector<int>{0,1}, join_attrs[2], durability);
    join_tables[3] = tl.prepare(3, total_num_attrs, vector<int>{0,1}, join_attrs[3], durability);

    // Instantiate the algorithm
    Solution durable_join(total_num_attrs);
    
    // Setup the indices
    durable_join.setup_table_index(join_order, join_tables);
    durable_join.setup_hash_index(join_order, join_attrs, join_tables);
    te = clock();
    double time_index = ((double) te - ts) / CLOCKS_PER_SEC;

    // run the algorithm
    ts = clock();
    vector<join_result> answer = 
        durable_join.durable_generic_join(
            join_tables, join_order, join_attrs, 0, durability);

    // In order to have the correct count of output tuples, 
    // we must remove duplicates from the output
    set<vector<int>> distinct = dedup(answer, durability);
    te = clock();
    double time_join = ((double) te - ts) / CLOCKS_PER_SEC;

    cerr << "Found " << distinct.size() << " matching tuples" << endl;
    cerr << "Index time " << time_index << endl;
    cerr << "Join time " << time_join << endl;

    if (csv) {
        dump_csv(distinct);
    }
}

void cycle5(std::string path, int durability, bool temporal, bool csv) {
    clock_t ts, te;

    // Load table
    TableLoader tl;
    // AB
    tl.load_test_table(0, path, 2);
    // BC
    tl.load_test_table(1);
    // CD
    tl.load_test_table(2);
    // AD
    tl.load_test_table(3);
    tl.load_test_table(4);

    // Define the join order
    vector<int> join_order = vector<int>{0,1,2,3,4};

    // Define the join attributes and the tables
    map<int, vector<int>> join_attrs;
    map<int, vector<join_result>> join_tables;

    int total_num_attrs = 5;
    // Join attribute ids
    join_attrs[0] = vector<int>{0,1}; //AB
    join_attrs[1] = vector<int>{1,2}; //BC
    join_attrs[2] = vector<int>{2,3}; //CD
    join_attrs[3] = vector<int>{3,4}; //AD
    join_attrs[4] = vector<int>{0,4}; //AD
    
    ts = clock();
    join_tables[0] = tl.prepare(0, total_num_attrs, vector<int>{0,1}, join_attrs[0], durability);
    join_tables[1] = tl.prepare(1, total_num_attrs, vector<int>{0,1}, join_attrs[1], durability);
    join_tables[2] = tl.prepare(2, total_num_attrs, vector<int>{0,1}, join_attrs[2], durability);
    join_tables[3] = tl.prepare(3, total_num_attrs, vector<int>{0,1}, join_attrs[3], durability);
    join_tables[4] = tl.prepare(4, total_num_attrs, vector<int>{0,1}, join_attrs[4], durability);

    // Instantiate the algorithm
    Solution durable_join(total_num_attrs);
    
    // Setup the indices
    durable_join.setup_table_index(join_order, join_tables);
    durable_join.setup_hash_index(join_order, join_attrs, join_tables);
    te = clock();
    double time_index = ((double) te - ts) / CLOCKS_PER_SEC;

    // run the algorithm
    ts = clock();
    vector<join_result> answer = 
        durable_join.durable_generic_join(
            join_tables, join_order, join_attrs, 0, durability);

    // In order to have the correct count of output tuples, 
    // we must remove duplicates from the output
    set<vector<int>> distinct = dedup(answer, durability);
    te = clock();
    double time_join = ((double) te - ts) / CLOCKS_PER_SEC;

    cerr << "Found " << distinct.size() << " matching tuples" << endl;
    cerr << "Index time " << time_index << endl;
    cerr << "Join time " << time_join << endl;

    if (csv) {
        dump_csv(distinct);
    }
}


int main(int argc, char* argv[]) {
    // we need to use durability at least 1 to ensure that empty
    // intervals are not included in the output
    // triangles("data/flightgraph-dedup.csv", 1, true, true);
    // cycle4("data/flightgraph-dedup.csv", 1, true, true);
    cycle5("data/flightgraph-dedup.csv", 1, true, true);
    // bowtie("data/flightgraph-dedup.csv", 1, true);
    return 0;
}

