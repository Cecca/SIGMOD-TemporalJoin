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
#include <nlohmann/json.hpp>

using namespace std::chrono;
using namespace std;
using json = nlohmann::json;

#define INDEX_TIME 0
#define JOIN_TIME 1

struct Result {
    clock_t starts[2];
    double times[2];
    uint64_t output_count;

    Result(): starts{0,0}, times{0,0}, output_count(0) {}
    
    void start_timer(int timer) {
        starts[timer] = clock();
    }
    void stop_timer(int timer) {
        clock_t te = clock();
        times[timer] = ((double) (te - starts[timer])) / CLOCKS_PER_SEC;
    }

    void print_json() {
        json jobj = {
            {"output_count", output_count},
            {"time_index_s", times[INDEX_TIME]},
            {"time_join_s", times[JOIN_TIME]},
        };
        std::cout << jobj << std::endl;
    }
};

std::set<std::vector<int>> dedup(std::vector<join_result> answer, int durability) {
    set<vector<int>> distinct;
    size_t orig_answer_size = answer.size();
    
    clock_t ts = clock();
    for (auto tup : answer) {
        assert(tup.attrs.size() > 0);
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
    Result res;
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
    join_attrs[0] = vector<int>{0,1}; // AB   p1
    join_attrs[1] = vector<int>{0,2}; // AC   p1
    join_attrs[2] = vector<int>{0,3}; // AD   p2
    join_attrs[3] = vector<int>{0,4}; // AE   p2
    join_attrs[4] = vector<int>{1,2}; // BC   p1
    join_attrs[5] = vector<int>{3,4}; // DE   p2

    res.start_timer(INDEX_TIME);
    join_tables[0] = tl.prepare(0, total_num_attrs, vector<int>{0,1}, join_attrs[0], durability);
    join_tables[1] = tl.prepare(1, total_num_attrs, vector<int>{0,1}, join_attrs[1], durability);
    join_tables[2] = tl.prepare(2, total_num_attrs, vector<int>{0,1}, join_attrs[2], durability);
    join_tables[3] = tl.prepare(3, total_num_attrs, vector<int>{0,1}, join_attrs[3], durability);
    join_tables[4] = tl.prepare(4, total_num_attrs, vector<int>{0,1}, join_attrs[4], durability);
    join_tables[5] = tl.prepare(5, total_num_attrs, vector<int>{0,1}, join_attrs[5], durability);

    Solution durable_join(total_num_attrs);
    durable_join.setup_table_index(join_order, join_tables);
    durable_join.setup_hash_index(join_order, join_attrs, join_tables);
    res.stop_timer(INDEX_TIME);

    res.start_timer(JOIN_TIME);
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
    std::cerr << "part 1 size " << part_1.size() << std::endl;
    // part_1[0].print();
    std::vector<join_result> part_2 = durable_join.durable_generic_join(join_tables_part_2, join_order_part_2, join_attrs_part_2, 0, durability);
    std::cerr << "part 2 size " << part_2.size() << std::endl;
    // part_2[0].print();
    if (!part_1.empty() && !part_2.empty()) {
        std::vector<int> common_attrs = get_intersection(part_1[0].attr_id, part_2[0].attr_id);
        std::vector<int> union_attrs = get_union(part_1[0].attr_id, part_2[0].attr_id);

        durable_join.sort_by_join_attrs(part_1, common_attrs);
        durable_join.sort_by_join_attrs(part_2, common_attrs);
        final_answer = durable_join.pairwise_forward_scan_temporal_join(part_1, part_2, common_attrs, union_attrs, durability);
    }

    set<vector<int>> distinct = dedup(final_answer, durability);
    res.stop_timer(JOIN_TIME);
    res.output_count = distinct.size();

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
    res.print_json();
}

void triangles(std::string path, int durability, bool csv) {
    Result res;

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
    
    res.start_timer(INDEX_TIME);
    join_tables[0] = tl.prepare(0, total_num_attrs, vector<int>{0,1}, join_attrs[0], durability);
    join_tables[1] = tl.prepare(1, total_num_attrs, vector<int>{0,1}, join_attrs[1], durability);
    join_tables[2] = tl.prepare(2, total_num_attrs, vector<int>{0,1}, join_attrs[2], durability);

    // Instantiate the algorithm
    Solution durable_join(total_num_attrs);
    
    // Setup the indices
    durable_join.setup_table_index(join_order, join_tables);
    durable_join.setup_hash_index(join_order, join_attrs, join_tables);
    res.stop_timer(INDEX_TIME);

    // run the algorithm
    res.start_timer(JOIN_TIME);
    vector<join_result> answer = 
        durable_join.durable_generic_join(
            join_tables, join_order, join_attrs, 0, durability);

    // In order to have the correct count of output tuples, 
    // we must remove duplicates from the output
    set<vector<int>> distinct = dedup(answer, durability);
    res.stop_timer(JOIN_TIME);
    res.output_count = distinct.size();
    res.print_json();

    if (csv) {
        dump_csv(distinct);
    }
}

void cycle4(std::string path, int durability, bool csv) {
    Result res;

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
    
    res.start_timer(INDEX_TIME);
    join_tables[0] = tl.prepare(0, total_num_attrs, vector<int>{0,1}, join_attrs[0], durability);
    join_tables[1] = tl.prepare(1, total_num_attrs, vector<int>{0,1}, join_attrs[1], durability);
    join_tables[2] = tl.prepare(2, total_num_attrs, vector<int>{0,1}, join_attrs[2], durability);
    join_tables[3] = tl.prepare(3, total_num_attrs, vector<int>{0,1}, join_attrs[3], durability);

    // Instantiate and run the algorithm
    Solution durable_join(total_num_attrs);
    res.start_timer(JOIN_TIME);
    vector<int> partial_join_order_1 = vector<int>{0,1};
    map<int, vector<int>> partial_join_attrs_1;
    map<int, vector<join_result>> partial_join_tables_1;
    for (int id : partial_join_order_1) {
        partial_join_attrs_1[id] = join_attrs[id];
        partial_join_tables_1[id] = join_tables[id];
    }
    vector<int> partial_join_order_2 = vector<int>{2,3};
    map<int, vector<int>> partial_join_attrs_2;
    map<int, vector<join_result>> partial_join_tables_2;
    for (int id : partial_join_order_2) {
        partial_join_attrs_2[id] = join_attrs[id];
        partial_join_tables_2[id] = join_tables[id];
    }
    vector<join_result> partial_1 = durable_join.multiway_durable_join_baseline(partial_join_tables_1, partial_join_order_1, partial_join_attrs_1, -1, -1);
    for (int i=0; i<partial_1.size(); ++i) {
        partial_1[i].table_id = 0;
        partial_1[i].idx = i;
    }
    assert(partial_1[0].attrs.size() > 0);
    vector<join_result> partial_2 = durable_join.multiway_durable_join_baseline(partial_join_tables_2, partial_join_order_2, partial_join_attrs_2, -1, -1);
    for (int i=0; i<partial_2.size(); ++i) {
        // for (auto a : partial_2[i].attrs) {
        //     cerr << a << " ";
        // }
        // cerr << endl;
        partial_2[i].table_id = 1;
        partial_2[i].idx = i;
    }
    assert(partial_2[0].attrs.size() > 0);
    map<int, vector<join_result>> hybrid_tables;
    map<int, vector<int>> hybrid_join_attrs;
    vector<int> hybrid_join_order = {0,1};
    hybrid_tables[0] = partial_1;
    hybrid_tables[1] = partial_2;
    hybrid_join_attrs[0] = partial_1[0].attr_id;
    hybrid_join_attrs[1] = partial_2[0].attr_id;
    cerr << "hybrid_join_attrs[0] = ";
    for (auto a : hybrid_join_attrs[0]) {
        cerr << a << ",";
    }
    cerr << endl;
    cerr << "hybrid_join_attrs[1] = ";
    for (auto a : hybrid_join_attrs[1]) {
        cerr << a << ",";
    }
    cerr << endl;
    vector<join_result> hybrid_answer = durable_join.n_star_durable_join_v2(hybrid_tables, hybrid_join_order, hybrid_join_attrs);
    
    // In order to have the correct count of output tuples, 
    // we must remove duplicates from the output
    set<vector<int>> distinct = dedup(hybrid_answer, durability);
    res.stop_timer(JOIN_TIME);
    res.output_count = distinct.size();
    res.print_json();

    if (csv) {
        dump_csv(distinct);
    }
}

void cycle5(std::string path, int durability, bool csv) {
    Result res;

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
    
    res.start_timer(INDEX_TIME);
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
    res.stop_timer(INDEX_TIME);

    // run the algorithm
    res.start_timer(JOIN_TIME);
    vector<join_result> answer = 
        durable_join.durable_generic_join(
            join_tables, join_order, join_attrs, 0, durability);

    // In order to have the correct count of output tuples, 
    // we must remove duplicates from the output
    set<vector<int>> distinct = dedup(answer, durability);
    res.stop_timer(JOIN_TIME);
    res.output_count = distinct.size();
    res.print_json();

    if (csv) {
        dump_csv(distinct);
    }
}

void clique4(std::string path, int durability, bool csv) {
    Result res;

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
    // AC
    tl.load_test_table(4);
    // BD
    tl.load_test_table(5);

    // Define the join order
    vector<int> join_order = vector<int>{0,1,2,3,4,5};

    // Define the join attributes and the tables
    map<int, vector<int>> join_attrs;
    map<int, vector<join_result>> join_tables;

    int total_num_attrs = 4;
    // Join attribute ids
    join_attrs[0] = vector<int>{0,1}; //AB
    join_attrs[1] = vector<int>{0,2}; //AC
    join_attrs[2] = vector<int>{0,3}; //AD
    join_attrs[3] = vector<int>{1,2}; //BC
    join_attrs[4] = vector<int>{1,3}; //BD
    join_attrs[5] = vector<int>{2,3}; //CD
    
    res.start_timer(INDEX_TIME);
    join_tables[0] = tl.prepare(0, total_num_attrs, vector<int>{0,1}, join_attrs[0], durability);
    join_tables[1] = tl.prepare(1, total_num_attrs, vector<int>{0,1}, join_attrs[1], durability);
    join_tables[2] = tl.prepare(2, total_num_attrs, vector<int>{0,1}, join_attrs[2], durability);
    join_tables[3] = tl.prepare(3, total_num_attrs, vector<int>{0,1}, join_attrs[3], durability);
    join_tables[4] = tl.prepare(4, total_num_attrs, vector<int>{0,1}, join_attrs[4], durability);
    join_tables[5] = tl.prepare(5, total_num_attrs, vector<int>{0,1}, join_attrs[5], durability);

    // Instantiate the algorithm
    Solution durable_join(total_num_attrs);
    
    // Setup the indices
    durable_join.setup_table_index(join_order, join_tables);
    durable_join.setup_hash_index(join_order, join_attrs, join_tables);
    res.stop_timer(INDEX_TIME);

    // run the algorithm
    res.start_timer(JOIN_TIME);
   vector<join_result> answer = 
        durable_join.durable_generic_join(
            join_tables, join_order, join_attrs, 0, durability);

    // In order to have the correct count of output tuples, 
    // we must remove duplicates from the output
    set<vector<int>> distinct = dedup(answer, durability);
    res.stop_timer(JOIN_TIME);
    res.output_count = distinct.size();
    res.print_json();

    if (csv) {
        dump_csv(distinct);
    }
}

void line3(std::string graph, int durability, bool csv) {
    Result res;
    TableLoader tl;

    tl.load_test_table(0, graph, 2);
    tl.load_test_table(1);
    tl.load_test_table(2);

    vector<int> join_order = vector<int>{0,1,2};
    map<int, vector<int>> join_attrs;
    map<int, vector<join_result>> join_tables;
    int total_num_attrs = 4;
    
    // global join attrs id:
    // A:0, B:1, C:2, D:3, E:4
    join_attrs[0] = vector<int>{0,1}; // B
    join_attrs[1] = vector<int>{1,2}; // BC
    join_attrs[2] = vector<int>{2,3}; // C

    res.start_timer(INDEX_TIME);
    join_tables[0] = tl.prepare(0, total_num_attrs, vector<int>{0,1}, join_attrs[0], durability);
    join_tables[1] = tl.prepare(1, total_num_attrs, vector<int>{0,1}, join_attrs[1], durability);
    join_tables[2] = tl.prepare(2, total_num_attrs, vector<int>{0,1}, join_attrs[2], durability);
    res.stop_timer(INDEX_TIME);

    Solution durable_join(total_num_attrs);

    res.start_timer(JOIN_TIME);
    vector<join_result> answer = durable_join.line_k_join(join_tables, join_attrs, durability);
    set<vector<int>> distinct = dedup(answer, durability);
    res.stop_timer(JOIN_TIME);
    res.output_count = distinct.size();

    if (csv) {
        dump_csv(distinct);
    }
    res.print_json();
}
void line4(std::string graph, int durability) {
    Result res;
    TableLoader tl;

    tl.load_test_table(0, graph, 2);
    tl.load_test_table(1);
    tl.load_test_table(2);
    tl.load_test_table(3);

    vector<int> join_order = vector<int>{0,1,2,3};
    map<int, vector<int>> join_attrs;
    map<int, vector<join_result>> join_tables;
    int total_num_attrs = 5;
    
    // global join attrs id:
    // A:0, B:1, C:2, D:3, E:4
    join_attrs[0] = vector<int>{0,1}; // B
    join_attrs[1] = vector<int>{1,2}; // BC
    join_attrs[2] = vector<int>{2,3}; // CD
    join_attrs[3] = vector<int>{3,4}; // DE

    res.start_timer(INDEX_TIME);
    join_tables[0] = tl.prepare(0, total_num_attrs, vector<int>{0,1}, join_attrs[0], durability, true);
    join_tables[1] = tl.prepare(1, total_num_attrs, vector<int>{0,1}, join_attrs[1], durability, true);
    join_tables[2] = tl.prepare(2, total_num_attrs, vector<int>{0,1}, join_attrs[2], durability, true);
    join_tables[3] = tl.prepare(3, total_num_attrs, vector<int>{0,1}, join_attrs[3], durability, true);
    res.stop_timer(INDEX_TIME);

    Solution durable_join(total_num_attrs);

    res.start_timer(JOIN_TIME);
    vector<join_result> answer = durable_join.line_k_join(join_tables, join_attrs, durability);
    set<vector<int>> distinct = dedup(answer, durability);
    res.stop_timer(JOIN_TIME);
    res.output_count = distinct.size();

    res.print_json();
}
void line5(std::string graph, int durability) {
    Result res;
    TableLoader tl;

    tl.load_test_table(0, graph, 2);
    tl.load_test_table(1);
    tl.load_test_table(2);
    tl.load_test_table(3);
    tl.load_test_table(4);

    vector<int> join_order = vector<int>{0,1,2,3,4};
    map<int, vector<int>> join_attrs;
    map<int, vector<join_result>> join_tables;
    int total_num_attrs = 6;
    
    // global join attrs id:
    // A:0, B:1, C:2, D:3, E:4
    join_attrs[0] = vector<int>{0,1}; // B
    join_attrs[1] = vector<int>{1,2}; // BC
    join_attrs[2] = vector<int>{2,3}; // CD
    join_attrs[3] = vector<int>{3,4}; // DE
    join_attrs[4] = vector<int>{4,5}; // DE

    res.start_timer(INDEX_TIME);
    join_tables[0] = tl.prepare(0, total_num_attrs, vector<int>{0,1}, join_attrs[0], durability, true);
    join_tables[1] = tl.prepare(1, total_num_attrs, vector<int>{0,1}, join_attrs[1], durability, true);
    join_tables[2] = tl.prepare(2, total_num_attrs, vector<int>{0,1}, join_attrs[2], durability, true);
    join_tables[3] = tl.prepare(3, total_num_attrs, vector<int>{0,1}, join_attrs[3], durability, true);
    join_tables[4] = tl.prepare(4, total_num_attrs, vector<int>{0,1}, join_attrs[4], durability, true);
    res.stop_timer(INDEX_TIME);

    Solution durable_join(total_num_attrs);

    res.start_timer(JOIN_TIME);
    vector<join_result> answer = durable_join.line_k_join(join_tables, join_attrs, durability);
    set<vector<int>> distinct = dedup(answer, durability);
    res.stop_timer(JOIN_TIME);
    res.output_count = distinct.size();

    res.print_json();
}

void employee_cycle(std::string path_dept, std::string path_title, int durability, bool csv) {
    Result res;

    // Load table
    TableLoader tl;
    // emp_a, dept 0,1
    tl.load_test_table(0, path_dept, 2);
    // emp_b, dept 2,1
    tl.load_test_table(1, path_dept, 2);
    // emp_a, title 0,3
    tl.load_test_table(2, path_title, 2);
    // emp_b, title 2,3
    tl.load_test_table(3, path_title, 2);

    // Define the join order
    vector<int> join_order = vector<int>{0,1,2,3};

    // Define the join attributes and the tables
    map<int, vector<int>> join_attrs;
    map<int, vector<join_result>> join_tables;

    int total_num_attrs = 4;
    // Join attribute ids
    join_attrs[0] = vector<int>{0,1}; // emp_a, dept
    join_attrs[1] = vector<int>{1,2}; // dept, emp_b
    join_attrs[2] = vector<int>{0,3}; // emp_a, title
    join_attrs[3] = vector<int>{2,3}; // emp_b, title
    
    res.start_timer(INDEX_TIME);
    join_tables[0] = tl.prepare(0, total_num_attrs, vector<int>{0,1}, join_attrs[0], durability);
    join_tables[1] = tl.prepare(1, total_num_attrs, vector<int>{1,0}, join_attrs[1], durability);
    join_tables[2] = tl.prepare(2, total_num_attrs, vector<int>{0,1}, join_attrs[2], durability);
    join_tables[3] = tl.prepare(3, total_num_attrs, vector<int>{0,1}, join_attrs[3], durability);

    // Instantiate the algorithm
    Solution durable_join(total_num_attrs);
    
    // Setup the indices
    durable_join.setup_table_index(join_order, join_tables);
    durable_join.setup_hash_index(join_order, join_attrs, join_tables);
    res.stop_timer(INDEX_TIME);

    // run the algorithm
    res.start_timer(JOIN_TIME);
    vector<join_result> answer = 
        durable_join.durable_generic_join(
            join_tables, join_order, join_attrs, 0, durability);

    // In order to have the correct count of output tuples, 
    // we must remove duplicates from the output
    set<vector<int>> distinct = dedup(answer, durability);
    res.stop_timer(JOIN_TIME);
    res.output_count = distinct.size();
    res.print_json();

    if (csv) {
        dump_csv(distinct);
    }
}

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "USAGE: MyMain config.json";
        return 1;
    }
    std::string config_path = argv[1]; 
    std::ifstream f(config_path);
    json config = json::parse(f);

    std::string query = config["query_name"];
    if (query == "triangle") {
        triangles(config["dataset"], config["durability"], false);
    } else if (query == "cycle4") {
        cycle4(config["dataset"], config["durability"], true);
    } else if (query == "cycle5") {
        cycle5(config["dataset"], config["durability"], false);
    } else if (query == "bowtie") {
        bowtie(config["dataset"], config["durability"], false);
    } else if (query == "clique4") {
        clique4(config["dataset"], config["durability"], false);
    } else if (query == "line3") {
        line3(config["dataset"], config["durability"], false);
    } else if (query == "line4") {
        line4(config["dataset"], config["durability"]);
    } else if (query == "line5") {
        line5(config["dataset"], config["durability"]);
    } else if (query == "employee-cycle") {
        employee_cycle(config["dept"], config["titles"], config["durability"], false);
    }

    // we need to use durability at least 1 to ensure that empty
    // intervals are not included in the output
    // triangles("data/flightgraph-dedup.csv", 1, true, true);
    // cycle4("data/flightgraph-dedup.csv", 1, true, true);
    // cycle5("data/flightgraph-dedup.csv", 1, true, false);
    // bowtie("data/flightgraph-dedup.csv", 1, true);
    return 0;
}

