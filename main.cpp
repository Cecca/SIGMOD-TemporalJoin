#include "data_loader.h"
#include "solution.h"
#include <string>
#include <iostream>
#include <fstream>
#include <numeric>
#include <chrono>

using namespace std::chrono;
using namespace std;

void dateTimeConverterTest() {
    string date_time = "2019-11-05 00:11:24";
    time_t tm = getEpochTime(date_time);
    cerr << tm << endl;
}

void dataLoaderTest() {
    TableLoader tl;
    tl.load_taxi_table(0, "/home/home1/jygao/workspace/DurableJoin/data/nyc-taxi/green_tripdata_2019-11.csv", 
            {{0,"id"},{1,"t_start"},{2,"t_end"},{5,"pickup_id"},{6,"dropoff_id"}}, true);
    
    tl.sort_by_attr(0, {{5,"pickup_id"}});
    tl.data_viewer(0, 20);

    tl.sort_by_attr(0, {{6,"dropoff_id"}});
    tl.data_viewer(0, 20);

    tl.sort_by_attr(0, {{5,"pickup_id"},{6,"dropoff_id"}});
    tl.data_viewer(0, 20);

}

void synLoaderTest() {
    TableLoader tl;
    tl.load_test_table(0, "/home/home1/jygao/workspace/DurableJoin/data/test-db/R1_AB.txt", 2);
    tl.sort_by_attr(0, std::vector<int>{0,1});
    tl.data_viewer_test(0, 20);

    tl.load_test_table(1, "/home/home1/jygao/workspace/DurableJoin/data/test-db/R2_ABD.txt", 3);
    tl.sort_by_attr(1, std::vector<int>{0,1,2});
    tl.data_viewer_test(1, 20);
}

void taxiStarJoinFullTest() {
    TableLoader tl;
    tl.load_taxi_table_v2(0, "/home/home1/jygao/workspace/DurableJoin/data/nyc-taxi/fhv_tripdata_2019-11.csv", 
            {{1,"t_start"},{2,"t_end"},{3,"pickup_id"},{4,"dropoff_id"}}, true);
    tl.load_taxi_table_v2(1, "/home/home1/jygao/workspace/DurableJoin/data/nyc-taxi/green_tripdata_2019-11.csv", 
            {{1,"t_start"},{2,"t_end"},{5,"pickup_id"},{6,"dropoff_id"}}, true);
    // tl.load_taxi_table_v2(2, "/home/home1/jygao/workspace/DurableJoin/data/nyc-taxi/yellow_tripdata_2019-11.csv", 
    //         {{1,"t_start"},{2,"t_end"},{7,"pickup_id"},{8,"dropoff_id"}}, true);
    tl.load_taxi_table_v2(2, "/home/home1/jygao/workspace/DurableJoin/data/nyc-taxi/xab", 
             {{1,"t_start"},{2,"t_end"},{7,"pickup_id"},{8,"dropoff_id"}}, true);
    
    vector<int> join_order = vector<int>{0,1,2};
    map<int, vector<int>> join_attrs;
    map<int, vector<join_result>> join_tables;
    int total_num_attrs = 4;

    // global join attrs id:
    // pickup_id: 0
    join_attrs[0] = vector<int>{0,1};
    join_attrs[1] = vector<int>{0,2};
    join_attrs[2] = vector<int>{0,3};

    clock_t ts, te;

    Solution durable_join(total_num_attrs);

    for (int tau = 50000; tau >= 5000; tau -= 5000) {
        join_tables[0] = tl.prepare(0, total_num_attrs, vector<int>{0,1}, join_attrs[0], tau);
        join_tables[1] = tl.prepare(1, total_num_attrs, vector<int>{0,1}, join_attrs[1], tau);
        join_tables[2] = tl.prepare(2, total_num_attrs, vector<int>{0,1}, join_attrs[2], tau);

        cerr << "================" << tau << "================" << endl;
        ts = clock();
        vector<join_result> answer = durable_join.hierarchy_durable_join("/home/home1/jygao/workspace/DurableJoin/join_tree_star_join.dat", 
                        join_tables, join_order, join_attrs, 30, tau);
        te = clock();
        cerr << "answer size: " << answer.size() << endl;
        for (int i=0 ; i < answer.size(); ++i) {
            if (i > 10)
                break;
            print_vector(answer[i].id);
        }
        
        ts = clock();
        vector<join_result> baseline = 
            durable_join.multiway_durable_join_baseline(join_tables, join_order, join_attrs, tau);
        te = clock();
        cerr << "ground truth size: " << baseline.size() << ' '
                << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << endl;
        for (int i=0; i < baseline.size(); ++i) {
            if (i > 10)
                break;
            print_vector(baseline[i].id);
        }
        
        // durable_join.difference(answer, baseline);
    }
}

void taxiStarJoinTest() {
    TableLoader tl;
    tl.load_taxi_table_v2(0, "/home/home1/jygao/workspace/DurableJoin/data/nyc-taxi/fhv_tripdata_2019-11.csv", 
            {{1,"t_start"},{2,"t_end"},{3,"pickup_id"},{4,"dropoff_id"}}, true);
    tl.load_taxi_table_v2(1, "/home/home1/jygao/workspace/DurableJoin/data/nyc-taxi/green_tripdata_2019-11.csv", 
            {{1,"t_start"},{2,"t_end"},{5,"pickup_id"},{6,"dropoff_id"}}, true);
    tl.load_taxi_table_v2(2, "/home/home1/jygao/workspace/DurableJoin/data/nyc-taxi/yellow_tripdata_2019-11.csv", 
            {{1,"t_start"},{2,"t_end"},{7,"pickup_id"},{8,"dropoff_id"}}, true);
    // tl.load_taxi_table_v2(4, "/home/home1/jygao/workspace/DurableJoin/data/nyc-taxi/fhvhv_tripdata_2019-11.csv", 
    //         {{2,"t_start"},{3,"t_end"},{4,"pickup_id"},{5,"dropoff_id"}}, true);
    
    vector<int> join_order = vector<int>{0,1,2};
    map<int, vector<int>> join_attrs;
    map<int, vector<join_result>> join_tables;
    int total_num_attrs = 2;
    
    // global join attrs id:
    // pickup_id: 0
    join_attrs[0] = vector<int>{0,1};
    join_attrs[1] = vector<int>{0,1};
    join_attrs[2] = vector<int>{0,1};
    
    clock_t ts, te;

    Solution durable_join(total_num_attrs);

    for (int tau = 50000; tau >= 5000; tau -= 5000) {
        join_tables[0] = tl.prepare(0, total_num_attrs, vector<int>{0,1}, join_attrs[0], 0);
        join_tables[1] = tl.prepare(1, total_num_attrs, vector<int>{0,1}, join_attrs[1], 0);
        join_tables[2] = tl.prepare(2, total_num_attrs, vector<int>{0,1}, join_attrs[2], 0);
        cerr << "================" << tau << "================" << endl;
        ts = clock();
        vector<join_result> answer = durable_join.hierarchy_durable_join("/home/home1/jygao/workspace/DurableJoin/join_tree_star_join_v2.dat", 
                        join_tables, join_order, join_attrs, 30, tau);
        te = clock();
        cerr << "answer size: " << answer.size() << endl;
        for (int i=0 ; i < answer.size(); ++i) {
            if (i > 10)
                break;
            print_vector(answer[i].id);
        }
        
        ts = clock();
        vector<join_result> baseline = 
            durable_join.multiway_durable_join_baseline(join_tables, join_order, join_attrs, tau);
        te = clock();
        cerr << "ground truth size: " << baseline.size() << ' '
                << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << endl;
        for (int i=0; i < baseline.size(); ++i) {
            if (i > 10)
                break;
            print_vector(baseline[i].id);
        }
    }
    
    // durable_join.difference(answer, baseline);

}

void IntervalTreeTest(int durability) {
    intervalVector test_intervals;
    test_intervals.push_back(interval(10 + durability/2, 30 - durability/2, 1));
    test_intervals.push_back(interval(10 + durability/2, 90 - durability/2, 2));
    test_intervals.push_back(interval(60 + durability/2, 70 - durability/2, 3));
    test_intervals.push_back(interval(30 + durability/2, 80 - durability/2, 4));
    test_intervals.push_back(interval(20 + durability/2, 30 - durability/2, 5));
    test_intervals.push_back(interval(15 + durability/2, 35 - durability/2, 6));
    test_intervals.push_back(interval(5 + durability/2, 30 - durability/2, 7));
    test_intervals.push_back(interval(1 + durability/2, 13 - durability/2, 8));

    intervalTree iTree(std::move(test_intervals), 16, 1);

    intervalVector result1 = iTree.findOverlapping(50 + durability/2, 70 - durability/2);
    // intervalVector result2 = iTree.findOverlapping_with_durability(20, 70, 15);
    int count = iTree.countOverlapping(50 + durability/2, 70 - durability/2);

    std::cerr << "regular overlapping" << std::endl;
    for (auto item : result1)
        item.print();
    std::cerr << count << std::endl;
    // std::cerr << "durability overlapping" << std::endl;
    // for (auto item : result2)
    //     item.print();
}

void JoinBaselineTest_v2(int durability) {
    TableLoader tl;
    tl.load_test_table(0, "/home/home1/jygao/workspace/DurableJoin/data/test-db/R1_AB.txt", 2);
    tl.sort_by_attr(0, std::vector<int>{0});

    tl.load_test_table(1, "/home/home1/jygao/workspace/DurableJoin/data/test-db/R2_ABD.txt", 3);
    tl.sort_by_attr(1, std::vector<int>{0});

    tl.load_test_table(4, "/home/home1/jygao/workspace/DurableJoin/data/test-db/R5_ACG.txt", 3);
    tl.sort_by_attr(4, std::vector<int>{0});

    vector<int> join_order = vector<int>{0,1,4};
    map<int, vector<int>> join_attrs;
    map<int, vector<join_result>> join_tables;
    // int durability = 200;
    int total_num_attrs = 5;

    join_attrs[0] = vector<int>{0}; //A
    join_attrs[1] = vector<int>{0}; //A
    join_attrs[4] = vector<int>{0}; //A

    join_tables[0] = tl.prepare(0, total_num_attrs, vector<int>{0}, join_attrs[0], durability);
    join_tables[1] = tl.prepare(1, total_num_attrs, vector<int>{0}, join_attrs[1], durability);
    join_tables[4] = tl.prepare(4, total_num_attrs, vector<int>{0}, join_attrs[4], durability);

    Solution durable_join(total_num_attrs);

    clock_t ts, te;
    ts = clock();
    vector<join_result> answer = 
        durable_join.multiway_durable_join_baseline(join_tables, join_order, join_attrs, durability);
    te = clock();

    cerr << "answer size: " << answer.size() << ' '
            << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << endl;
    // for (auto item : answer)
    //     item.print();
    
    ts = clock();
    vector<join_result> ground_truth = 
        durable_join.multiway_durable_join_baseline(join_tables, join_order, join_attrs, durability, 1);
    te = clock();

    cerr << "ground truth size: " << ground_truth.size() << ' '
            << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << endl;

    ts = clock();
    vector<join_result> fs_temporal_join = 
        durable_join.multiway_durable_join_baseline(join_tables, join_order, join_attrs, durability, -1);
    te = clock();

    cerr << "ground truth size: " << fs_temporal_join.size() << ' '
            << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << endl;

}

void JoinBaselineTest(int durability) {
    TableLoader tl;
    tl.load_test_table(0, "/home/home1/jygao/workspace/DurableJoin/data/test-db/R1_AB.txt", 2);
    tl.sort_by_attr(0, std::vector<int>{0,1});
    // tl.data_viewer_test(0, 50);

    tl.load_test_table(1, "/home/home1/jygao/workspace/DurableJoin/data/test-db/R2_ABD.txt", 3);
    tl.sort_by_attr(1, std::vector<int>{0,1});

    tl.load_test_table(2, "/home/home1/jygao/workspace/DurableJoin/data/test-db/R3_ABE.txt", 3);
    tl.sort_by_attr(2, std::vector<int>{0,1});

    tl.load_test_table(3, "/home/home1/jygao/workspace/DurableJoin/data/test-db/R4_ACF.txt", 3);
    tl.sort_by_attr(3, std::vector<int>{0,1});

    tl.load_test_table(4, "/home/home1/jygao/workspace/DurableJoin/data/test-db/R5_ACG.txt", 3);
    tl.sort_by_attr(4, std::vector<int>{0,1});

    vector<int> join_order = vector<int>{0,1,2,3,4};
    map<int, vector<int>> join_attrs;
    map<int, vector<join_result>> join_tables;
    
    int total_num_attrs = 7;

    // global join attrs id:
    // A:0, B:1, C:2, D:3, E:4, F:5, G:6
    join_attrs[0] = vector<int>{0,1}; //AB
    join_attrs[1] = vector<int>{0,1}; //AB
    join_attrs[2] = vector<int>{0,1}; //AB
    join_attrs[3] = vector<int>{0,2}; //AC
    join_attrs[4] = vector<int>{0,2}; //AC

    join_tables[0] = tl.prepare(0, total_num_attrs, vector<int>{0,1}, join_attrs[0], durability);
    join_tables[1] = tl.prepare(1, total_num_attrs, vector<int>{0,1}, join_attrs[1], durability);
    join_tables[2] = tl.prepare(2, total_num_attrs, vector<int>{0,1}, join_attrs[2], durability);
    join_tables[3] = tl.prepare(3, total_num_attrs, vector<int>{0,1}, join_attrs[3], durability);
    join_tables[4] = tl.prepare(4, total_num_attrs, vector<int>{0,1}, join_attrs[4], durability);

    Solution durable_join(total_num_attrs);

    clock_t ts, te;
    ts = clock();
    vector<join_result> answer = 
        durable_join.multiway_durable_join_baseline(join_tables, join_order, join_attrs, durability);
    te = clock();

    cerr << "answer size: " << answer.size() << ' '
            << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << endl;

    ts = clock();
    vector<join_result> temporal_join_answer = 
        durable_join.multiway_durable_join_baseline(join_tables, join_order, join_attrs, durability, 1);
    te = clock();

    cerr << "answer size: " << temporal_join_answer.size() << ' '
            << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << endl;


    ts = clock();
    vector<join_result> fs_temporal_join_answer = 
        durable_join.multiway_durable_join_baseline(join_tables, join_order, join_attrs, durability, -1);
    te = clock();

    cerr << "answer size: " << fs_temporal_join_answer.size() << ' '
            << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << endl;

    // for (auto item : answer)
    //     item.print();
    
    // ts = clock();
    // vector<join_result> ground_truth = 
    //     durable_join.multiway_durable_join_baseline(join_tables, join_order, join_attrs, durability, 1);
    // te = clock();

    // cerr << "ground truth size: " << ground_truth.size() << ' '
    //         << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << endl;
    // for (auto item : ground_truth)
    //     item.print();
}

void LoadJoinTreeTest() {
    Solution durable_join;
    string filename = "/home/home1/jygao/workspace/DurableJoin/join_tree_1.dat";
    map<int, JoinTreeNode*> tree = durable_join.create_join_tree(filename);
    durable_join.tree_viewer(tree, 30);
}

void cyclicJoinTest(int durability) {
    clock_t ts, te;
    TableLoader tl;

    tl.load_test_table(0, "/home/home1/jygao/workspace/DurableJoin/data/Cyclic/CTableStart.csv", 2);
    tl.load_test_table(1, "/home/home1/jygao/workspace/DurableJoin/data/Cyclic/CTable1.csv", 2);
    tl.load_test_table(2, "/home/home1/jygao/workspace/DurableJoin/data/Cyclic/CTable2.csv", 2);
    tl.load_test_table(3, "/home/home1/jygao/workspace/DurableJoin/data/Cyclic/CTable3.csv", 2);
    tl.load_test_table(4, "/home/home1/jygao/workspace/DurableJoin/data/Cyclic/CTableEnd.csv", 2);

    vector<int> join_order = vector<int>{0,1,2,3,4};
    map<int, vector<int>> join_attrs;
    map<int, vector<join_result>> join_tables;
    int total_num_attrs = 4;
    
    // global join attrs id:
    // A:0, B:1, C:2, D:3, E:4
    join_attrs[0] = vector<int>{1}; // B
    join_attrs[1] = vector<int>{1,2}; // BC
    join_attrs[2] = vector<int>{2,3}; // CD
    join_attrs[3] = vector<int>{1,3}; // BD
    join_attrs[4] = vector<int>{3}; // D

    ts = clock();
    join_tables[0] = tl.prepare(0, total_num_attrs, vector<int>{1}, join_attrs[0], durability, true);
    join_tables[1] = tl.prepare(1, total_num_attrs, vector<int>{0,1}, join_attrs[1], durability, true);
    join_tables[2] = tl.prepare(2, total_num_attrs, vector<int>{0,1}, join_attrs[2], durability, true);
    join_tables[3] = tl.prepare(3, total_num_attrs, vector<int>{0,1}, join_attrs[3], durability, true);
    join_tables[4] = tl.prepare(4, total_num_attrs, vector<int>{0}, join_attrs[4], durability, true);
    te = clock();
    clock_t filter_time = te - ts;

    Solution durable_join(total_num_attrs);

    // ts = clock();
    // vector<join_result> baseline = 
    //     durable_join.multiway_durable_join_baseline(join_tables, join_order, join_attrs, -1, -1);
    // te = clock();
    // cerr << "ground truth size: " << baseline.size() << ' '
    //         << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << ' '
    //         << "filter time: " << (double) filter_time / CLOCKS_PER_SEC << endl;

    durable_join.setup_table_index(join_order, join_tables);
    durable_join.setup_hash_index(join_order, join_attrs, join_tables);

    vector<int> join_order_part_1 = {1,2,3};
    map<int, vector<join_result>> join_tables_part_1;
    map<int, vector<int>> join_attrs_part_1;
    for (int id : join_order_part_1) {
        join_tables_part_1[id] = join_tables[id];
        join_attrs_part_1[id] = join_attrs[id];
    }

    clock_t total = 0;
    ts = clock();
    std::vector<join_result> final_answer;
    std::vector<join_result> part_1 = durable_join.durable_generic_join(join_tables_part_1, join_order_part_1, join_attrs_part_1, 0, -1);
    for (int i=0; i<part_1.size(); ++i) {
        part_1[i].table_id = 1;
        part_1[i].idx = i;
    }
    te = clock();
    total += te - ts;
    map<int, vector<join_result>> hybrid_tables;
    map<int, vector<int>> hybrid_join_attrs;
    vector<int> hybrid_join_order = {0,1,4};
    hybrid_tables[0] = join_tables[0];
    hybrid_tables[1] = part_1;
    hybrid_tables[4] = join_tables[4];
    hybrid_join_attrs[0] = join_attrs[0];
    hybrid_join_attrs[1] = part_1[0].attr_id;
    hybrid_join_attrs[4] = join_attrs[4];
    ts = clock();
    // vector<join_result> hybrid_answer = durable_join.n_star_durable_join_v2(hybrid_tables, hybrid_join_order, hybrid_join_attrs);
    // vector<join_result> hybrid_answer = durable_join.multiway_durable_join_baseline(hybrid_tables, hybrid_join_order, hybrid_join_attrs, -1, -1);
    int root_id = 0;
    vector<join_result> hybrid_answer = 
        durable_join.non_hierarchy_durable_join_v2("/home/home1/jygao/workspace/DurableJoin/yannakakis_line_3_join_tree_v2.dat", 
                        hybrid_tables, hybrid_join_order, hybrid_join_attrs, root_id, -1);
    te = clock();
    total += te - ts;
    cerr << "hybrid answer size: " << hybrid_answer.size() << ' '
            << "time usage: " << (double) total / CLOCKS_PER_SEC << endl;

}

void starJoinTest(int durability) {
    clock_t ts, te;
    TableLoader tl;

    tl.load_test_table(0, "/home/home1/jygao/workspace/DurableJoin/data/Star/STableStart.csv", 2);
    tl.load_test_table(1, "/home/home1/jygao/workspace/DurableJoin/data/Star/STable1.csv", 2);
    tl.load_test_table(2, "/home/home1/jygao/workspace/DurableJoin/data/Star/STable2.csv", 2);
    tl.load_test_table(3, "/home/home1/jygao/workspace/DurableJoin/data/Star/STableEnd.csv", 2);

    vector<int> join_order = vector<int>{0,1,2,3};
    map<int, vector<int>> join_attrs;
    map<int, vector<join_result>> join_tables;
    int total_num_attrs = 5;
    
    // global join attrs id:
    // A:0, B:1, C:2, D:3, E:4
    join_attrs[0] = vector<int>{0,1}; // AB
    join_attrs[1] = vector<int>{0,2}; // AC
    join_attrs[2] = vector<int>{0,3}; // AD
    join_attrs[3] = vector<int>{0,4}; // AE

    ts = clock();
    join_tables[0] = tl.prepare(0, total_num_attrs, vector<int>{0,1}, join_attrs[0], durability, true);
    join_tables[1] = tl.prepare(1, total_num_attrs, vector<int>{0,1}, join_attrs[1], durability, true);
    join_tables[2] = tl.prepare(2, total_num_attrs, vector<int>{0,1}, join_attrs[2], durability, true);
    join_tables[3] = tl.prepare(3, total_num_attrs, vector<int>{0,1}, join_attrs[3], durability, true);
    te = clock();
    clock_t filter_time = te - ts;

    Solution durable_join(total_num_attrs);
    
    // ts = clock();
    // vector<join_result> baseline = 
    //     durable_join.multiway_durable_join_baseline(join_tables, join_order, join_attrs, -1, -1);
    // te = clock();
    // cerr << "ground truth size: " << baseline.size() << ' '
    //         << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << ' '
    //         << "filter time: " << (double) filter_time / CLOCKS_PER_SEC << endl;

    ts = clock();
    vector<join_result> better_answer = durable_join.n_star_durable_join(join_tables, join_order, join_attrs);
    te = clock();
    cerr << "answer size: " << better_answer.size() << ' '
            << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << ' '
            << "filter time: " << (double) filter_time / CLOCKS_PER_SEC << endl;

    
}

void HierachicalJoinTest() {
    clock_t ts, te;
    TableLoader tl;
    tl.load_test_table(0, "/home/home1/jygao/workspace/DurableJoin/data/test-db-large/R1_AB.txt", 2);
    tl.load_test_table(1, "/home/home1/jygao/workspace/DurableJoin/data/test-db-large/R2_ABD.txt", 3);
    tl.load_test_table(2, "/home/home1/jygao/workspace/DurableJoin/data/test-db-large/R3_ABE.txt", 3);
    tl.load_test_table(3, "/home/home1/jygao/workspace/DurableJoin/data/test-db-large/R4_ACF.txt", 3);
    tl.load_test_table(4, "/home/home1/jygao/workspace/DurableJoin/data/test-db-large/R5_ACG.txt", 3);

    vector<int> join_order = vector<int>{0,1,2,3,4};
    map<int, vector<int>> join_attrs;
    map<int, vector<join_result>> join_tables;
    int total_num_attrs = 7;
    
    // global join attrs id:
    // A:0, B:1, C:2, D:3, E:4, F:5, G:6
    join_attrs[0] = vector<int>{0,1}; //AB
    join_attrs[1] = vector<int>{0,1}; //AB
    join_attrs[2] = vector<int>{0,1}; //AB
    join_attrs[3] = vector<int>{0,2}; //AC
    join_attrs[4] = vector<int>{0,2}; //AC

    Solution durable_join(total_num_attrs);
    
    for (int tau = 900; tau >= 400; tau -= 100) {
        join_tables[0] = tl.prepare(0, total_num_attrs, vector<int>{0,1}, join_attrs[0], 0);
        join_tables[1] = tl.prepare(1, total_num_attrs, vector<int>{0,1}, join_attrs[1], 0);
        join_tables[2] = tl.prepare(2, total_num_attrs, vector<int>{0,1}, join_attrs[2], 0);
        join_tables[3] = tl.prepare(3, total_num_attrs, vector<int>{0,1}, join_attrs[3], 0);
        join_tables[4] = tl.prepare(4, total_num_attrs, vector<int>{0,1}, join_attrs[4], 0);
        cerr << "============" << tau << "============" << endl;
        ts = clock();
        vector<join_result> answer = durable_join.hierarchy_durable_join("/home/home1/jygao/workspace/DurableJoin/join_tree_1.dat", 
                        join_tables, join_order, join_attrs, 30, tau);
        te = clock();
        cerr << "answer size: " << answer.size() << endl;
        
        ts = clock();
        vector<join_result> baseline = 
            durable_join.multiway_durable_join_baseline(join_tables, join_order, join_attrs, tau);
        te = clock();
        cerr << "ground truth size: " << baseline.size() << ' '
                << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << endl;
    }
    // durable_join.difference(answer, baseline);
}

void Cyclic3Join(int durability) {
    clock_t ts, te;
    TableLoader tl;

    tl.load_test_table(0, "/usr/project/xtmp/jygao/flight-db/flights_slim.csv", 2);
    tl.load_test_table(1);
    tl.load_test_table(2);

    vector<int> join_order = vector<int>{0,1,2};
    map<int, vector<int>> join_attrs;
    map<int, vector<join_result>> join_tables;

    int total_num_attrs = 3;

    // global join attrs id:
    // A:0, B:1, C:2
    join_attrs[0] = vector<int>{0,1}; //AB
    join_attrs[1] = vector<int>{1,2}; //BC
    join_attrs[2] = vector<int>{0,2}; //AC

    join_tables[0] = tl.prepare(0, total_num_attrs, vector<int>{0,1}, join_attrs[0], durability, true);
    join_tables[1] = tl.prepare(1, total_num_attrs, vector<int>{0,1}, join_attrs[1], durability, true);
    join_tables[2] = tl.prepare(2, total_num_attrs, vector<int>{0,1}, join_attrs[2], durability, true);

    Solution durable_join(total_num_attrs);
    durable_join.setup_table_index(join_order, join_tables);
    durable_join.setup_hash_index(join_order, join_attrs, join_tables);

    ts = clock();
    vector<join_result> generic_join_answer = durable_join.durable_generic_join(join_tables, join_order, join_attrs, 0, -1);
    te = clock();
    cerr << "answer size: " << generic_join_answer.size() << ' '
            << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << endl;

    // ts = clock();
    // vector<join_result> fs_temporal_join_answer = 
    //     durable_join.multiway_durable_join_baseline(join_tables, join_order, join_attrs, -1, -1);
    // te = clock();
    // cerr << "answer size: " << fs_temporal_join_answer.size() << ' '
    //         << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << endl;
}

void Cyclic4Join(int durability) {
    clock_t ts, te;
    TableLoader tl;

    // tl.load_test_table(0, "/usr/project/xtmp/jygao/flight-db/flights_slim.csv", 2);
    tl.load_test_table(0, "data/test-db/R1_AB.txt", 2);
    tl.load_test_table(1);
    tl.load_test_table(2);
    tl.load_test_table(3);


    vector<int> join_order = vector<int>{0,1,2,3};
    map<int, vector<int>> join_attrs;
    map<int, vector<join_result>> join_tables;

    int total_num_attrs = 4;

    // global join attrs id:
    // A:0, B:1, C:2, D:3
    join_attrs[0] = vector<int>{0,1}; //AB
    join_attrs[1] = vector<int>{1,2}; //BC
    join_attrs[2] = vector<int>{2,3}; //CD
    join_attrs[3] = vector<int>{0,3}; //AD

    join_tables[0] = tl.prepare(0, total_num_attrs, vector<int>{0,1}, join_attrs[0], durability, true);
    join_tables[1] = tl.prepare(1, total_num_attrs, vector<int>{0,1}, join_attrs[1], durability, true);
    join_tables[2] = tl.prepare(2, total_num_attrs, vector<int>{0,1}, join_attrs[2], durability, true);
    join_tables[3] = tl.prepare(3, total_num_attrs, vector<int>{0,1}, join_attrs[3], durability, true);

    Solution durable_join(total_num_attrs);
    // durable_join.setup_table_index(join_order, join_tables);
    // durable_join.setup_hash_index(join_order, join_attrs, join_tables);

    // ts = clock();
    // vector<join_result> generic_join_answer = durable_join.durable_generic_join(join_tables, join_order, join_attrs, 0, -1);
    // te = clock();
    // cerr << "answer size: " << generic_join_answer.size() << ' '
    //         << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << endl;

    // ts = clock();
    // vector<join_result> fs_temporal_join_answer = 
    //     durable_join.multiway_durable_join_baseline(join_tables, join_order, join_attrs, -1, -1);
    // te = clock();
    // cerr << "answer size: " << fs_temporal_join_answer.size() << ' '
    //         << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << endl;
    
    clock_t total = 0;
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
    ts = clock();
    vector<join_result> partial_1 = durable_join.multiway_durable_join_baseline(partial_join_tables_1, partial_join_order_1, partial_join_attrs_1, -1, -1);
    te = clock();
    cerr << (double) (te - ts) / CLOCKS_PER_SEC << endl;
    for (int i=0; i<partial_1.size(); ++i) {
        partial_1[i].table_id = 0;
        partial_1[i].idx = i;
    }
    total += te - ts;
    ts = clock();
    vector<join_result> partial_2 = durable_join.multiway_durable_join_baseline(partial_join_tables_2, partial_join_order_2, partial_join_attrs_2, -1, -1);
    te = clock();
    cerr << (double) (te - ts) / CLOCKS_PER_SEC << endl;
    for (int i=0; i<partial_2.size(); ++i) {
        partial_2[i].table_id = 1;
        partial_2[i].idx = i;
    }
    total += te - ts;
    map<int, vector<join_result>> hybrid_tables;
    map<int, vector<int>> hybrid_join_attrs;
    vector<int> hybrid_join_order = {0,1};
    hybrid_tables[0] = partial_1;
    hybrid_tables[1] = partial_2;
    hybrid_join_attrs[0] = partial_1[0].attr_id;
    hybrid_join_attrs[1] = partial_2[0].attr_id;
    ts = clock();
    vector<join_result> hybrid_answer = durable_join.n_star_durable_join_v2(hybrid_tables, hybrid_join_order, hybrid_join_attrs);
    assert(hybrid_answer[0].attrs.size() > 0);
    te = clock();
    total += te - ts;
    cerr << "hybrid answer size: " << hybrid_answer.size() << ' '
            << "time usage: " << (double) total / CLOCKS_PER_SEC << endl;

}

void Cyclic5Join(int durability) {
    clock_t ts, te;
    TableLoader tl;

    tl.load_test_table(0, "/usr/project/xtmp/jygao/flight-db/flights_slim.csv", 2);
    tl.load_test_table(1);
    tl.load_test_table(2);
    tl.load_test_table(3);
    tl.load_test_table(4);


    vector<int> join_order = vector<int>{0,1,2,3,4};
    map<int, vector<int>> join_attrs;
    map<int, vector<join_result>> join_tables;

    int total_num_attrs = 5;

    // global join attrs id:
    // A:0, B:1, C:2
    join_attrs[0] = vector<int>{0,1}; //AB
    join_attrs[1] = vector<int>{1,2}; //BC
    join_attrs[2] = vector<int>{2,3}; //CD
    join_attrs[3] = vector<int>{3,4}; //DE
    join_attrs[4] = vector<int>{0,4}; //AE

    join_tables[0] = tl.prepare(0, total_num_attrs, vector<int>{0,1}, join_attrs[0], durability, true);
    join_tables[1] = tl.prepare(1, total_num_attrs, vector<int>{0,1}, join_attrs[1], durability, true);
    join_tables[2] = tl.prepare(2, total_num_attrs, vector<int>{0,1}, join_attrs[2], durability, true);
    join_tables[3] = tl.prepare(3, total_num_attrs, vector<int>{0,1}, join_attrs[3], durability, true);
    join_tables[4] = tl.prepare(4, total_num_attrs, vector<int>{0,1}, join_attrs[4], durability, true);

    Solution durable_join(total_num_attrs);
    // durable_join.setup_table_index(join_order, join_tables);
    // durable_join.setup_hash_index(join_order, join_attrs, join_tables);

    // ts = clock();
    // vector<join_result> generic_join_answer = durable_join.durable_generic_join(join_tables, join_order, join_attrs, 0, -1);
    // te = clock();
    // cerr << "answer size: " << generic_join_answer.size() << ' '
    //         << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << endl;

    // ts = clock();
    // vector<join_result> fs_temporal_join_answer = 
    //     durable_join.multiway_durable_join_baseline(join_tables, join_order, join_attrs, -1, -1);
    // te = clock();
    // cerr << "answer size: " << fs_temporal_join_answer.size() << ' '
    //         << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << endl;
    
    clock_t total = 0;
    vector<int> partial_join_order_1 = vector<int>{0,1,2};
    map<int, vector<int>> partial_join_attrs_1;
    map<int, vector<join_result>> partial_join_tables_1;
    for (int id : partial_join_order_1) {
        partial_join_attrs_1[id] = join_attrs[id];
        partial_join_tables_1[id] = join_tables[id];
    }
    vector<int> partial_join_order_2 = vector<int>{3,4};
    map<int, vector<int>> partial_join_attrs_2;
    map<int, vector<join_result>> partial_join_tables_2;
    for (int id : partial_join_order_2) {
        partial_join_attrs_2[id] = join_attrs[id];
        partial_join_tables_2[id] = join_tables[id];
    }
    ts = clock();
    vector<join_result> partial_1 = durable_join.multiway_durable_join_baseline(partial_join_tables_1, partial_join_order_1, partial_join_attrs_1, -1, -1);
    te = clock();
    cerr << (double) (te - ts) / CLOCKS_PER_SEC << endl;
    for (int i=0; i<partial_1.size(); ++i) {
        partial_1[i].table_id = 0;
        partial_1[i].idx = i;
    }
    total += te - ts;
    ts = clock();
    vector<join_result> partial_2 = durable_join.multiway_durable_join_baseline(partial_join_tables_2, partial_join_order_2, partial_join_attrs_2, -1, -1);
    te = clock();
    cerr << (double) (te - ts) / CLOCKS_PER_SEC << endl;
    for (int i=0; i<partial_2.size(); ++i) {
        partial_2[i].table_id = 1;
        partial_2[i].idx = i;
    }
    total += te - ts;
    map<int, vector<join_result>> hybrid_tables;
    map<int, vector<int>> hybrid_join_attrs;
    vector<int> hybrid_join_order = {0,1};
    hybrid_tables[0] = partial_1;
    hybrid_tables[1] = partial_2;
    hybrid_join_attrs[0] = partial_1[0].attr_id;
    hybrid_join_attrs[1] = partial_2[0].attr_id;
    ts = clock();
    vector<join_result> hybrid_answer = durable_join.n_star_durable_join_v2(hybrid_tables, hybrid_join_order, hybrid_join_attrs);
    te = clock();
    total += te - ts;
    cerr << "hybrid answer size: " << hybrid_answer.size() << ' '
            << "time usage: " << (double) total / CLOCKS_PER_SEC << endl;
}

void YannakakisTest(int durability) {
    clock_t ts, te;
    TableLoader tl;
    tl.load_test_table(0, "/home/home1/jygao/workspace/DurableJoin/data/test-db-yannakakis/R1_AB.txt", 2);
    tl.load_test_table(1, "/home/home1/jygao/workspace/DurableJoin/data/test-db-yannakakis/R2_ABC.txt", 3);
    tl.load_test_table(2, "/home/home1/jygao/workspace/DurableJoin/data/test-db-yannakakis/R3_B.txt", 1);
    tl.load_test_table(3, "/home/home1/jygao/workspace/DurableJoin/data/test-db-yannakakis/R4_ABD.txt", 3);
    
    vector<int> join_order = vector<int>{0,1,3,2};
    map<int, vector<int>> join_attrs;
    map<int, vector<join_result>> join_tables;
    int total_num_attrs = 4;
    
    // global join attrs id:
    // A:0, B:1, C:2, D:3
    join_attrs[0] = vector<int>{0,1}; //AB
    join_attrs[1] = vector<int>{0,1}; //AB
    join_attrs[2] = vector<int>{1}; //B
    join_attrs[3] = vector<int>{0,1}; //AB

    join_tables[0] = tl.prepare(0, total_num_attrs, vector<int>{0,1}, join_attrs[0], 0);
    join_tables[1] = tl.prepare(1, total_num_attrs, vector<int>{0,1}, join_attrs[1], 0);
    join_tables[2] = tl.prepare(2, total_num_attrs, vector<int>{0}, join_attrs[2], 0);
    join_tables[3] = tl.prepare(3, total_num_attrs, vector<int>{0,1}, join_attrs[3], 0);

    Solution durable_join(total_num_attrs);

    ts = clock();
    int root_id = 0;
    vector<join_result> yannakakis_answer = 
        durable_join.non_hierarchy_durable_join_v2("/home/home1/jygao/workspace/DurableJoin/yannakakis_join_tree.dat", 
                        join_tables, join_order, join_attrs, root_id, durability);
    te = clock();
    cerr << "answer size: " << yannakakis_answer.size() << ' '
            << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << endl;

    for (auto item : yannakakis_answer)
        item.print();

    ts = clock();
    vector<join_result> fs_temporal_join_answer = 
        durable_join.multiway_durable_join_baseline(join_tables, join_order, join_attrs, durability, -1);
    te = clock();
    cerr << "answer size: " << fs_temporal_join_answer.size() << ' '
            << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << endl;
}

void Line3CountingTest(int durability) {
    clock_t ts, te;
    TableLoader tl;
    tl.load_test_table(0, "/home/home1/jygao/workspace/DurableJoin/data/test-line-join/LineNewData/1M/R1_1MNew.csv", 2);
    tl.load_test_table(1, "/home/home1/jygao/workspace/DurableJoin/data/test-line-join/LineNewData/1M/R2_1MNew.csv", 2);
    tl.load_test_table(2, "/home/home1/jygao/workspace/DurableJoin/data/test-line-join/LineNewData/1M/R3_1MNew.csv", 2);

    vector<int> join_order = vector<int>{0,1,2};
    map<int, vector<int>> join_attrs;
    map<int, vector<join_result>> join_tables;
    int total_num_attrs = 3;
    
    // global join attrs id:
    // A:0, B:1, C:2, D:3, E:4, F:5, G:6
    join_attrs[0] = vector<int>{1}; // B
    join_attrs[1] = vector<int>{1,2}; // BC
    join_attrs[2] = vector<int>{2}; // C

    ts = clock();
    join_tables[0] = tl.prepare(0, total_num_attrs, vector<int>{1}, join_attrs[0], durability);
    join_tables[1] = tl.prepare(1, total_num_attrs, vector<int>{0,1}, join_attrs[1], durability);
    join_tables[2] = tl.prepare(2, total_num_attrs, vector<int>{0}, join_attrs[2], durability);
    te = clock();
    clock_t filter_time = te - ts;

    Solution durable_join(total_num_attrs);

    ts = clock();
    int answer = durable_join.line_3_join_counting(join_tables[0], join_tables[1], join_tables[2], join_attrs, durability);
    te = clock();
    cerr << "answer size: " << answer << ' '
            << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << ' '
            << "filter time: " << (double) filter_time / CLOCKS_PER_SEC << endl;
}

void Line3JoinSweepPlaneTest(int durability) {
    clock_t ts, te;
    TableLoader tl;
    // tl.load_test_table(0, "/home/home1/jygao/workspace/DurableJoin/data/test-line-join/LineData/100K/R1_100K.csv", 2);
    // tl.load_test_table(1, "/home/home1/jygao/workspace/DurableJoin/data/test-line-join/LineData/100K/R2_100K.csv", 2);
    // tl.load_test_table(2, "/home/home1/jygao/workspace/DurableJoin/data/test-line-join/LineData/100K/R3_100K.csv", 2);

    tl.load_test_table(0, "/home/home1/jygao/workspace/DurableJoin/data/test-line-join/LineData/NoIntRes/R1_0.csv", 2);
    tl.load_test_table(1, "/home/home1/jygao/workspace/DurableJoin/data/test-line-join/LineData/NoIntRes/R2_0.csv", 2);
    tl.load_test_table(2, "/home/home1/jygao/workspace/DurableJoin/data/test-line-join/LineData/NoIntRes/R3_0.csv", 2);
    
    tl.sort_by_attr(0, std::vector<int>{0,1});
    tl.sort_by_attr(1, std::vector<int>{0,1});
    tl.sort_by_attr(2, std::vector<int>{0,1});

    vector<int> join_order = vector<int>{0,1,2};
    map<int, vector<int>> join_attrs;
    map<int, vector<join_result>> join_tables;
    int total_num_attrs = 3;
    
    // global join attrs id:
    // A:0, B:1, C:2, D:3, E:4, F:5, G:6
    join_attrs[0] = vector<int>{1}; // B
    join_attrs[1] = vector<int>{1,2}; // BC
    join_attrs[2] = vector<int>{2}; // C

    ts = clock();
    join_tables[0] = tl.prepare(0, total_num_attrs, vector<int>{1}, join_attrs[0], durability, true);
    join_tables[1] = tl.prepare(1, total_num_attrs, vector<int>{0,1}, join_attrs[1], durability, true);
    join_tables[2] = tl.prepare(2, total_num_attrs, vector<int>{0}, join_attrs[2], durability, true);
    te = clock();
    clock_t filter_time = te - ts;

    Solution durable_join(total_num_attrs);

    // ts = clock();
    // vector<join_result> answer = durable_join.line_3_join(join_tables[0], join_tables[1], join_tables[2], join_attrs, durability);
    // te = clock();
    // cerr << "answer size: " << answer.size() << ' '
    //         << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << ' '
    //         << "filter time: " << (double) filter_time / CLOCKS_PER_SEC << endl;
    
    ts = clock();
    int root_id = 0;
    vector<join_result> yannakakis_answer = 
        durable_join.non_hierarchy_durable_join_v2("/home/home1/jygao/workspace/DurableJoin/yannakakis_line_3_join_tree.dat", 
                        join_tables, join_order, join_attrs, root_id, -1);
    te = clock();
    cerr << "answer size: " << yannakakis_answer.size() << ' '
            << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << endl;
    // for (auto item : yannakakis_answer)
    //     item.print();

    ts = clock();
    vector<join_result> baseline = 
        durable_join.multiway_durable_join_baseline(join_tables, join_order, join_attrs, -1, -1);
    te = clock();
    cerr << "ground truth size: " << baseline.size() << ' '
            << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << ' '
            << "filter time: " << (double) filter_time / CLOCKS_PER_SEC << endl;
    // for (auto item : baseline)
    //     item.print();
}

void SynLineJoinTest(int durability) {
    clock_t ts, te;
    TableLoader tl;

    tl.load_test_table(0, "/home/home1/jygao/workspace/DurableJoin/data/Acyclic/TableStart.csv", 2);
    tl.load_test_table(1, "/home/home1/jygao/workspace/DurableJoin/data/Acyclic/Table1.csv", 2);
    tl.load_test_table(2, "/home/home1/jygao/workspace/DurableJoin/data/Acyclic/Table2.csv", 2);
    tl.load_test_table(3, "/home/home1/jygao/workspace/DurableJoin/data/Acyclic/TableEnd.csv", 2);

    vector<int> join_order = vector<int>{0,1,2,3};
    map<int, vector<int>> join_attrs;
    map<int, vector<join_result>> join_tables;
    int total_num_attrs = 4;
    
    // global join attrs id:
    // A:0, B:1, C:2, D:3, E:4, F:5, G:6
    join_attrs[0] = vector<int>{1}; // B
    join_attrs[1] = vector<int>{1,2}; // BC
    join_attrs[2] = vector<int>{2,3}; // CD
    join_attrs[3] = vector<int>{3}; // D

    ts = clock();
    join_tables[0] = tl.prepare(0, total_num_attrs, vector<int>{1}, join_attrs[0], durability, true);
    join_tables[1] = tl.prepare(1, total_num_attrs, vector<int>{0,1}, join_attrs[1], durability, true);
    join_tables[2] = tl.prepare(2, total_num_attrs, vector<int>{0,1}, join_attrs[2], durability, true);
    join_tables[3] = tl.prepare(3, total_num_attrs, vector<int>{0}, join_attrs[3], durability, true);

    te = clock();
    clock_t filter_time = te - ts;

    Solution durable_join(total_num_attrs);

    ts = clock();
    // std::vector<int> common_attrs = get_intersection(join_attrs[1], join_attrs[2]);
    // std::vector<int> union_attrs = get_union(join_attrs[1], join_attrs[2]);
    // std::vector<join_result> middle_table = durable_join.pairwise_forward_scan_temporal_join(join_tables[1], join_tables[2], common_attrs, union_attrs, -1);
    // // for (int i=0; i<middle_table.size(); ++i) {
    // //     middle_table[i].table_id = 1;
    // //     middle_table[i].idx = i;
    // // }
 
    // map<int, vector<int>> hybrid_join_attrs;
    // hybrid_join_attrs[0] = join_attrs[0];
    // hybrid_join_attrs[1] = middle_table[0].attr_id;
    // hybrid_join_attrs[2] = join_attrs[3];
    
    vector<join_result> answer = durable_join.line_k_join(join_tables, join_attrs, -1);
    te = clock();
    cerr << "answer size: " << answer.size() << ' '
            << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << ' '
            << "filter time: " << (double) filter_time / CLOCKS_PER_SEC << endl;
    
    // ts = clock();
    // vector<join_result> baseline = 
    //     durable_join.multiway_durable_join_baseline(join_tables, join_order, join_attrs, -1, -1);
    // te = clock();
    // cerr << "ground truth size: " << baseline.size() << ' '
    //         << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << ' '
    //         << "filter time: " << (double) filter_time / CLOCKS_PER_SEC << endl;

    // ts = clock();
    // int root_id = 0;
    // vector<join_result> yannakakis_answer = 
    //     durable_join.non_hierarchy_durable_join_v2("/home/home1/jygao/workspace/DurableJoin/yannakakis_line_4_join_tree.dat", 
    //                     join_tables, join_order, join_attrs, root_id, -1);
    // te = clock();
    // cerr << "answer size: " << yannakakis_answer.size() << ' '
    //         << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << endl;

    // clock_t total = 0;
    // vector<int> partial_join_order = vector<int>{0,1,2};
    // map<int, vector<int>> partial_join_attrs;
    // map<int, vector<join_result>> partial_join_tables;
    // for (int id : partial_join_order) {
    //     partial_join_attrs[id] = join_attrs[id];
    //     partial_join_tables[id] = join_tables[id];
    // }
    // clock_t tts = clock();
    // vector<join_result> partial = durable_join.multiway_durable_join_baseline(partial_join_tables, partial_join_order, partial_join_attrs, -1, -1);
    // clock_t tte = clock();
    // cerr << (double) (tte - tts) / CLOCKS_PER_SEC << endl;
    // for (int i=0; i<partial.size(); ++i) {
    //     partial[i].table_id = 0;
    //     partial[i].idx = i;
    // }
    // total += tte - tts;
    // map<int, vector<join_result>> hybrid_tables;
    // map<int, vector<int>> hybrid_join_attrs;
    // vector<int> hybrid_join_order = {0,3};
    // hybrid_tables[0] = partial;
    // hybrid_tables[3] = join_tables[3];
    // hybrid_join_attrs[0] = partial[0].attr_id;
    // hybrid_join_attrs[3] = join_attrs[3];
    // tts = clock();
    // vector<join_result> hybrid_answer = durable_join.n_star_durable_join(hybrid_tables, hybrid_join_order, hybrid_join_attrs);
    // tte = clock();
    // total += tte - tts;
    // cerr << "hybrid answer size: " << hybrid_answer.size() << ' '
    //         << "time usage: " << (double) total / CLOCKS_PER_SEC << endl;

}

void lineKJoinTest(int durability) {
    clock_t ts, te;
    TableLoader tl;
    tl.load_test_table(0, "/home/home1/jygao/workspace/DurableJoin/data/test-db-partial/R1_AB.txt", 2);
    tl.load_test_table(1, "/home/home1/jygao/workspace/DurableJoin/data/test-db-partial/R1_AB.txt", 2);
    tl.load_test_table(2, "/home/home1/jygao/workspace/DurableJoin/data/test-db-partial/R1_AB.txt", 2);
    tl.load_test_table(3, "/home/home1/jygao/workspace/DurableJoin/data/test-db-partial/R1_AB.txt", 2);
    tl.load_test_table(4, "/home/home1/jygao/workspace/DurableJoin/data/test-db-partial/R1_AB.txt", 2);
    tl.load_test_table(5, "/home/home1/jygao/workspace/DurableJoin/data/test-db-partial/R1_AB.txt", 2);

    vector<int> join_order = vector<int>{0,1,2,3,4,5};
    map<int, vector<int>> join_attrs;
    map<int, vector<join_result>> join_tables;
    int total_num_attrs = 6;
    
    // global join attrs id:
    // A:0, B:1, C:2, D:3, E:4, F:5, G:6
    join_attrs[0] = vector<int>{1}; 
    join_attrs[1] = vector<int>{1,2}; 
    join_attrs[2] = vector<int>{2,3}; 
    join_attrs[3] = vector<int>{3,4}; 
    join_attrs[4] = vector<int>{4,5}; 
    join_attrs[5] = vector<int>{5}; 

    ts = clock();
    join_tables[0] = tl.prepare(0, total_num_attrs, vector<int>{1}, join_attrs[0], durability);
    join_tables[1] = tl.prepare(1, total_num_attrs, vector<int>{0,1}, join_attrs[1], durability);
    join_tables[2] = tl.prepare(2, total_num_attrs, vector<int>{0,1}, join_attrs[2], durability);
    join_tables[3] = tl.prepare(3, total_num_attrs, vector<int>{0,1}, join_attrs[3], durability);
    join_tables[4] = tl.prepare(4, total_num_attrs, vector<int>{0,1}, join_attrs[4], durability);
    join_tables[5] = tl.prepare(5, total_num_attrs, vector<int>{0}, join_attrs[5], durability);
    te = clock();
    clock_t filter_time = te - ts;

    Solution durable_join(total_num_attrs);

    ts = clock();
    vector<join_result> answer = durable_join.line_k_join(join_tables, join_attrs, durability);
    te = clock();
    cerr << "answer size: " << answer.size() << ' '
            << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << ' '
            << "filter time: " << (double) filter_time / CLOCKS_PER_SEC << endl;
    
    // for (int i=0; i<answer.size(); i += 5000)
    //     answer[i].print();
    
    ts = clock();
    vector<join_result> baseline = 
        durable_join.multiway_durable_join_baseline(join_tables, join_order, join_attrs, durability, 1);
    te = clock();
    cerr << "ground truth size: " << baseline.size() << ' '
            << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << ' '
            << "filter time: " << (double) filter_time / CLOCKS_PER_SEC << endl;

    durable_join.difference(answer, baseline);
}

void dblpLineKJoinTest(int durability) {
    clock_t ts, te;
    TableLoader tl;
    // tl.load_test_table(0, "/usr/project/xtmp/jygao/DBLP/articles_aggregate_year.txt", 2);
    // tl.load_test_table(1, "/usr/project/xtmp/jygao/DBLP/articles_aggregate_year.txt", 2);
    // tl.load_test_table(2, "/usr/project/xtmp/jygao/DBLP/articles_aggregate_year.txt", 2);
    // tl.load_test_table(3, "/usr/project/xtmp/jygao/DBLP/articles_aggregate_year.txt", 2);
    // // tl.load_test_table(4, "/usr/project/xtmp/jygao/DBLP/articles_aggregate_year_DAG.txt", 2);
    // // tl.load_test_table(5, "/usr/project/xtmp/jygao/DBLP/articles_aggregate_year_DAG.txt", 2);

    tl.load_test_table(0, "/usr/project/xtmp/jygao/DBLP/inproceedings_aggregate_year.txt", 2);
    // tl.load_test_table(1, "/usr/project/xtmp/jygao/DBLP/inproceedings_aggregate_year.txt", 2);
    // tl.load_test_table(2, "/usr/project/xtmp/jygao/DBLP/inproceedings_aggregate_year.txt", 2);
    // tl.load_test_table(3, "/usr/project/xtmp/jygao/DBLP/inproceedings_aggregate_year.txt", 2);
    // tl.load_test_table(4, "/usr/project/xtmp/jygao/DBLP/inproceedings_aggregate_year.txt", 2);
    // tl.load_test_table(5, "/usr/project/xtmp/jygao/DBLP/inproceedings_aggregate_year.txt", 2);
    // tl.load_test_table(6, "/usr/project/xtmp/jygao/DBLP/inproceedings_aggregate_year.txt", 2);
    // tl.load_test_table(7, "/usr/project/xtmp/jygao/DBLP/inproceedings_aggregate_year.txt", 2);
    // tl.load_test_table(8, "/usr/project/xtmp/jygao/DBLP/inproceedings_aggregate_year.txt", 2);
    tl.load_test_table(1);
    tl.load_test_table(2);
    tl.load_test_table(3);
    tl.load_test_table(4);
    tl.load_test_table(5);
    tl.load_test_table(6);
    tl.load_test_table(7);
    tl.load_test_table(8);

    vector<int> join_order = vector<int>{0,1,2,3,4,5,6,7,8};
    // vector<int> join_order = vector<int>{0,1,2,3,4,5};
    map<int, vector<int>> join_attrs;
    map<int, vector<join_result>> join_tables;
    int total_num_attrs = 9;
    
    // global join attrs id:
    // A:0, B:1, C:2, D:3, E:4, F:5, G:6
    join_attrs[0] = vector<int>{1};
    join_attrs[1] = vector<int>{1,2};
    join_attrs[2] = vector<int>{2,3}; 
    join_attrs[3] = vector<int>{3,4}; 
    join_attrs[4] = vector<int>{4,5}; 
    join_attrs[5] = vector<int>{5,6}; 
    join_attrs[6] = vector<int>{6,7}; 
    join_attrs[7] = vector<int>{7,8};
    join_attrs[8] = vector<int>{8};

    ts = clock();
    join_tables[0] = tl.prepare(0, total_num_attrs, vector<int>{1}, join_attrs[0], durability, true);
    join_tables[1] = tl.prepare(1, total_num_attrs, vector<int>{0,1}, join_attrs[1], durability, true);
    join_tables[2] = tl.prepare(2, total_num_attrs, vector<int>{0,1}, join_attrs[2], durability, true);
    join_tables[3] = tl.prepare(3, total_num_attrs, vector<int>{0,1}, join_attrs[3], durability, true);
    join_tables[4] = tl.prepare(4, total_num_attrs, vector<int>{0,1}, join_attrs[4], durability, true);
    join_tables[5] = tl.prepare(5, total_num_attrs, vector<int>{0,1}, join_attrs[5], durability, true);
    join_tables[6] = tl.prepare(6, total_num_attrs, vector<int>{0,1}, join_attrs[6], durability, true);
    join_tables[7] = tl.prepare(7, total_num_attrs, vector<int>{0,1}, join_attrs[7], durability, true);
    join_tables[8] = tl.prepare(8, total_num_attrs, vector<int>{0}, join_attrs[8], durability, true);

    te = clock();
    clock_t filter_time = te - ts;

    Solution durable_join(total_num_attrs);

    ts = clock();
    int root_id = 0;
    vector<join_result> yannakakis_answer = 
        durable_join.non_hierarchy_durable_join_v2("/home/home1/jygao/workspace/DurableJoin/yannakakis_line_9_join_tree.dat", 
                        join_tables, join_order, join_attrs, root_id, -1);
    te = clock();
    // for (auto record : yannakakis_answer)
    //     record.print();
    yannakakis_answer = durable_join.remove_invalid(yannakakis_answer);
    cerr << "answer size: " << yannakakis_answer.size() << ' '
            << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << endl;


    // ts = clock();
    // vector<join_result> answer = durable_join.line_k_join(join_tables, join_attrs, durability);
    // te = clock();
    // answer = durable_join.remove_invalid(answer);
    // cerr << "answer size: " << answer.size() << ' '
    //         << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << ' '
    //         << "filter time: " << (double) filter_time / CLOCKS_PER_SEC << endl;


    // ts = clock();
    // int answer_size = durable_join.line_k_join_counting(join_tables, join_attrs, durability);
    // te = clock();
    // cerr << "answer size: " << answer_size << ' '
    //         << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << ' '
    //         << "filter time: " << (double) filter_time / CLOCKS_PER_SEC << endl;

    ts = clock();
    vector<join_result> baseline = 
        durable_join.multiway_durable_join_baseline(join_tables, join_order, join_attrs, -1, -1);
    te = clock();
    // for (auto record : baseline)
    //     record.print();
    baseline = durable_join.remove_invalid(baseline);
    cerr << "ground truth size: " << baseline.size() << ' '
            << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << ' '
            << "filter time: " << (double) filter_time / CLOCKS_PER_SEC << endl;

    // durable_join.difference(answer, baseline);
}

void RealLine3JoinTest(int durability) {
    clock_t ts, te;
    TableLoader tl;

    // tl.load_test_table(0, "/usr/project/xtmp/jygao/flight-db/flights_slim.csv", 2);
    tl.load_test_table(0, "/usr/project/xtmp/jygao/DBLP/inproceedings_aggregate_year.txt", 2);
    tl.load_test_table(1);
    tl.load_test_table(2);

    vector<int> join_order = vector<int>{0,1,2};
    map<int, vector<int>> join_attrs;
    map<int, vector<join_result>> join_tables;
    int total_num_attrs = 3;
    
    // global join attrs id:
    // A:0, B:1, C:2
    join_attrs[0] = vector<int>{1}; // B
    join_attrs[1] = vector<int>{1,2}; // BC
    join_attrs[2] = vector<int>{2}; // C

    ts = clock();
    join_tables[0] = tl.prepare(0, total_num_attrs, vector<int>{1}, join_attrs[0], durability, true);
    join_tables[1] = tl.prepare(1, total_num_attrs, vector<int>{0,1}, join_attrs[1], durability, true);
    join_tables[2] = tl.prepare(2, total_num_attrs, vector<int>{0}, join_attrs[2], durability, true);
    cerr << "filtered table size : " << join_tables[0].size() << endl;
    te = clock();
    clock_t filter_time = te - ts;

    Solution durable_join(total_num_attrs);

    // ts = clock();
    // vector<join_result> answer = durable_join.line_3_join(join_tables[0], join_tables[1], join_tables[2], join_attrs, -1);
    // te = clock();
    // answer = durable_join.remove_invalid(answer);
    // cerr << "answer size: " << answer.size() << ' '
    //         << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << ' '
    //         << "filter time: " << (double) filter_time / CLOCKS_PER_SEC << endl;

    ts = clock();
    int root_id = 0;
    vector<join_result> yannakakis_answer = 
        durable_join.non_hierarchy_durable_join_v2("/home/home1/jygao/workspace/DurableJoin/yannakakis_line_3_join_tree.dat", 
                        join_tables, join_order, join_attrs, root_id, -1);
    te = clock();
    yannakakis_answer = durable_join.remove_invalid(yannakakis_answer);
    cerr << "answer size: " << yannakakis_answer.size() << ' '
            << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << endl;
    
    
    // ts = clock();
    // vector<join_result> baseline = 
    //     durable_join.multiway_durable_join_baseline(join_tables, join_order, join_attrs, -1, -1);
    // te = clock();
    // // for (auto item : baseline)
    // //     item.print();
    // baseline = durable_join.remove_invalid(baseline);
    // cerr << "ground truth size: " << baseline.size() << ' '
    //         << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << ' '
    //         << "filter time: " << (double) filter_time / CLOCKS_PER_SEC << endl;

    // clock_t total = 0;
    // std::vector<int> common_attrs = get_intersection(join_attrs[0], join_attrs[1]);
    // std::vector<int> union_attrs = get_union(join_attrs[0], join_attrs[1]);
    // ts = clock();
    // durable_join.sort_by_join_attrs(join_tables[0], common_attrs);
    // durable_join.sort_by_join_attrs(join_tables[1], common_attrs);
    // vector<join_result> partial = durable_join.pairwise_forward_scan_temporal_join(join_tables[0], join_tables[1], common_attrs, union_attrs, -1);
    // te = clock();
    // for (int i=0; i<partial.size(); ++i) {
    //     partial[i].table_id = 0;
    //     partial[i].idx = i;
    // }
    // cerr << (double) (te - ts) / CLOCKS_PER_SEC << endl;
    // total += te - ts;
    // map<int, vector<join_result>> hybrid_tables;
    // map<int, vector<int>> hybrid_join_attrs;
    // vector<int> hybrid_join_order = {0,2};
    // hybrid_tables[0] = partial;
    // hybrid_tables[2] = join_tables[2];
    // hybrid_join_attrs[0] = partial[0].attr_id;
    // hybrid_join_attrs[2] = join_attrs[2];
    // ts = clock();
    // vector<join_result> hybrid_answer = durable_join.n_star_durable_join(hybrid_tables, hybrid_join_order, hybrid_join_attrs);
    // te = clock();
    // total += te - ts;
    // cerr << "hybrid answer size: " << hybrid_answer.size() << ' '
    //         << "time usage: " << (double) total / CLOCKS_PER_SEC << endl;
}

void RealLine4JoinTest(int durability) {
    clock_t ts, te;
    TableLoader tl;

    tl.load_test_table(0, "/usr/project/xtmp/jygao/flight-db/flights_slim.csv", 2);
    // tl.load_test_table(0, "/usr/project/xtmp/jygao/DBLP/inproceedings_aggregate_year.txt", 2);
    tl.load_test_table(1);
    tl.load_test_table(2);
    tl.load_test_table(3);

    vector<int> join_order = vector<int>{0,1,2,3};
    map<int, vector<int>> join_attrs;
    map<int, vector<join_result>> join_tables;
    int total_num_attrs = 4;
    
    // global join attrs id:
    // A:0, B:1, C:2, D:3
    join_attrs[0] = vector<int>{1}; // B
    join_attrs[1] = vector<int>{1,2}; // BC
    join_attrs[2] = vector<int>{2,3}; // CD
    join_attrs[3] = vector<int>{3}; // D

    ts = clock();
    join_tables[0] = tl.prepare(0, total_num_attrs, vector<int>{1}, join_attrs[0], durability, true);
    join_tables[1] = tl.prepare(1, total_num_attrs, vector<int>{0,1}, join_attrs[1], durability, true);
    join_tables[2] = tl.prepare(2, total_num_attrs, vector<int>{0,1}, join_attrs[2], durability, true);
    join_tables[3] = tl.prepare(3, total_num_attrs, vector<int>{0}, join_attrs[3], durability, true);
    cerr << "filtered table size : " << join_tables[0].size() << endl;
    te = clock();
    clock_t filter_time = te - ts;

    Solution durable_join(total_num_attrs);

    ts = clock();
    vector<join_result> answer = durable_join.line_k_join(join_tables, join_attrs, -1);
    te = clock();
    cerr << "ground truth size: " << answer.size() << ' '
            << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << ' '
            << "filter time: " << (double) filter_time / CLOCKS_PER_SEC << endl;

    // ts = clock();
    // vector<join_result> baseline = 
    //     durable_join.multiway_durable_join_baseline(join_tables, join_order, join_attrs, -1, -1);
    // te = clock();
    // baseline = durable_join.remove_invalid(baseline);
    // cerr << "ground truth size: " << baseline.size() << ' '
    //         << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << ' '
    //         << "filter time: " << (double) filter_time / CLOCKS_PER_SEC << endl;

    // ts = clock();
    // int root_id = 0;
    // vector<join_result> yannakakis_answer = 
    //     durable_join.non_hierarchy_durable_join_v2("/home/home1/jygao/workspace/DurableJoin/yannakakis_line_4_join_tree.dat", 
    //                     join_tables, join_order, join_attrs, root_id, -1);
    // te = clock();
    // yannakakis_answer = durable_join.remove_invalid(yannakakis_answer);
    // cerr << "answer size: " << yannakakis_answer.size() << ' '
    //         << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << endl;

    // clock_t total = 0;
    // vector<int> partial_join_order = vector<int>{0,1,2};
    // map<int, vector<int>> partial_join_attrs;
    // map<int, vector<join_result>> partial_join_tables;
    // for (int id : partial_join_order) {
    //     partial_join_attrs[id] = join_attrs[id];
    //     partial_join_tables[id] = join_tables[id];
    // }
    // ts = clock();
    // vector<join_result> partial = durable_join.multiway_durable_join_baseline(partial_join_tables, partial_join_order, partial_join_attrs, -1, -1);
    // te = clock();
    // cerr << (double) (te - ts) / CLOCKS_PER_SEC << endl;
    // for (int i=0; i<partial.size(); ++i) {
    //     partial[i].table_id = 0;
    //     partial[i].idx = i;
    // }
    // total += te - ts;
    // map<int, vector<join_result>> hybrid_tables;
    // map<int, vector<int>> hybrid_join_attrs;
    // vector<int> hybrid_join_order = {0,3};
    // hybrid_tables[0] = partial;
    // hybrid_tables[3] = join_tables[3];
    // hybrid_join_attrs[0] = partial[0].attr_id;
    // hybrid_join_attrs[3] = join_attrs[3];
    // ts = clock();
    // vector<join_result> hybrid_answer = durable_join.n_star_durable_join(hybrid_tables, hybrid_join_order, hybrid_join_attrs);
    // te = clock();
    // total += te - ts;
    // cerr << "hybrid answer size: " << hybrid_answer.size() << ' '
    //         << "time usage: " << (double) total / CLOCKS_PER_SEC << endl;

}

void RealLine5JoinTest(int durability) {
    clock_t ts, te;
    TableLoader tl;

    tl.load_test_table(0, "/usr/project/xtmp/jygao/flight-db/flights_slim.csv", 2);
    // tl.load_test_table(0, "/usr/project/xtmp/jygao/DBLP/inproceedings_aggregate_year.txt", 2);
    tl.load_test_table(1);
    tl.load_test_table(2);
    tl.load_test_table(3);
    tl.load_test_table(4);

    vector<int> join_order = vector<int>{0,1,2,3,4};
    map<int, vector<int>> join_attrs;
    map<int, vector<join_result>> join_tables;
    int total_num_attrs = 5;
    
    // global join attrs id:
    // A:0, B:1, C:2, D:3, E:4
    join_attrs[0] = vector<int>{1}; // B
    join_attrs[1] = vector<int>{1,2}; // BC
    join_attrs[2] = vector<int>{2,3}; // CD
    join_attrs[3] = vector<int>{3,4}; // DE
    join_attrs[4] = vector<int>{4}; // DE

    ts = clock();
    join_tables[0] = tl.prepare(0, total_num_attrs, vector<int>{1}, join_attrs[0], durability, true);
    join_tables[1] = tl.prepare(1, total_num_attrs, vector<int>{0,1}, join_attrs[1], durability, true);
    join_tables[2] = tl.prepare(2, total_num_attrs, vector<int>{0,1}, join_attrs[2], durability, true);
    join_tables[3] = tl.prepare(3, total_num_attrs, vector<int>{0,1}, join_attrs[3], durability, true);
    join_tables[4] = tl.prepare(4, total_num_attrs, vector<int>{0}, join_attrs[4], durability, true);
    cerr << "filtered table size : " << join_tables[0].size() << endl;
    te = clock();
    clock_t filter_time = te - ts;

    Solution durable_join(total_num_attrs);

    ts = clock();
    vector<join_result> answer = durable_join.line_k_join(join_tables, join_attrs, -1);
    te = clock();
    cerr << "ground truth size: " << answer.size() << ' '
            << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << ' '
            << "filter time: " << (double) filter_time / CLOCKS_PER_SEC << endl;

    // ts = clock();
    // vector<join_result> baseline = 
    //     durable_join.multiway_durable_join_baseline(join_tables, join_order, join_attrs, -1, -1);
    // te = clock();
    // baseline = durable_join.remove_invalid(baseline);
    // cerr << "ground truth size: " << baseline.size() << ' '
    //         << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << ' '
    //         << "filter time: " << (double) filter_time / CLOCKS_PER_SEC << endl;
    
    // ts = clock();
    // int root_id = 0;
    // vector<join_result> yannakakis_answer = 
    //     durable_join.non_hierarchy_durable_join_v2("/home/home1/jygao/workspace/DurableJoin/yannakakis_line_5_join_tree.dat", 
    //                     join_tables, join_order, join_attrs, root_id, -1);
    // te = clock();
    // yannakakis_answer = durable_join.remove_invalid(yannakakis_answer);
    // cerr << "answer size: " << yannakakis_answer.size() << ' '
    //         << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << endl;

    // clock_t total = 0;
    // vector<int> partial_join_order = vector<int>{0,1,2,3};
    // map<int, vector<int>> partial_join_attrs;
    // map<int, vector<join_result>> partial_join_tables;
    // for (int id : partial_join_order) {
    //     partial_join_attrs[id] = join_attrs[id];
    //     partial_join_tables[id] = join_tables[id];
    // }
    // clock_t tts = clock();
    // vector<join_result> partial = durable_join.multiway_durable_join_baseline(partial_join_tables, partial_join_order, partial_join_attrs, -1, -1);
    // clock_t tte = clock();
    // cerr << (double) (tte - tts) / CLOCKS_PER_SEC << endl;
    // for (int i=0; i<partial.size(); ++i) {
    //     partial[i].table_id = 0;
    //     partial[i].idx = i;
    // }
    // total += tte - tts;
    // map<int, vector<join_result>> hybrid_tables;
    // map<int, vector<int>> hybrid_join_attrs;
    // vector<int> hybrid_join_order = {0,4};
    // hybrid_tables[0] = partial;
    // hybrid_tables[4] = join_tables[4];
    // hybrid_join_attrs[0] = partial[0].attr_id;
    // hybrid_join_attrs[4] = join_attrs[4];
    // tts = clock();
    // vector<join_result> hybrid_answer = durable_join.n_star_durable_join(hybrid_tables, hybrid_join_order, hybrid_join_attrs);
    // tte = clock();
    // total += tte - tts;
    // cerr << "hybrid answer size: " << hybrid_answer.size() << ' '
    //         << "time usage: " << (double) total / CLOCKS_PER_SEC << endl;

}


void tpcQ3Join(int durability) {
    clock_t ts, te;
    TableLoader tl;

    tl.load_test_table(2, "/usr/project/xtmp/jygao/TPC-BiH/time_customer_slim.csv", 2);
    tl.load_test_table(1, "/usr/project/xtmp/jygao/TPC-BiH/time_orders_slim.csv", 2);
    tl.load_test_table(0, "/usr/project/xtmp/jygao/TPC-BiH/time_lineitem_slim.csv", 2);

    vector<int> join_order = vector<int>{0,1,2};
    map<int, vector<int>> join_attrs;
    map<int, vector<join_result>> join_tables;
    int total_num_attrs = 3;
    
    // global join attrs id:
    // A:0, B:1, C:2
    join_attrs[0] = vector<int>{1}; // B
    join_attrs[1] = vector<int>{1,2}; // BC
    join_attrs[2] = vector<int>{2}; // C

    ts = clock();
    join_tables[0] = tl.prepare(0, total_num_attrs, vector<int>{1}, join_attrs[0], durability, true);
    join_tables[1] = tl.prepare(1, total_num_attrs, vector<int>{0,1}, join_attrs[1], durability, true);
    join_tables[2] = tl.prepare(2, total_num_attrs, vector<int>{0}, join_attrs[2], durability, true);
    cerr << "filtered table size : " << endl;
    for (int id : join_order)
        cerr << join_tables[id].size() << std::endl;
    te = clock();
    clock_t filter_time = te - ts;

    Solution durable_join(total_num_attrs);

    ts = clock();
    vector<join_result> baseline = 
        durable_join.multiway_durable_join_baseline(join_tables, join_order, join_attrs, -1, -1);
    te = clock();
    baseline = durable_join.remove_invalid(baseline);
    cerr << "ground truth size: " << baseline.size() << ' '
            << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << ' '
            << "filter time: " << (double) filter_time / CLOCKS_PER_SEC << endl;

    // clock_t total = 0;
    // std::vector<int> common_attrs = get_intersection(join_attrs[0], join_attrs[1]);
    // std::vector<int> union_attrs = get_union(join_attrs[0], join_attrs[1]);
    // ts = clock();
    // durable_join.sort_by_join_attrs(join_tables[0], common_attrs);
    // durable_join.sort_by_join_attrs(join_tables[1], common_attrs);
    // vector<join_result> partial = durable_join.pairwise_forward_scan_temporal_join(join_tables[0], join_tables[1], common_attrs, union_attrs, -1);
    // te = clock();
    // for (int i=0; i<partial.size(); ++i) {
    //     partial[i].table_id = 0;
    //     partial[i].idx = i;
    // }
    // cerr << (double) (te - ts) / CLOCKS_PER_SEC << endl;
    // total += te - ts;
    // map<int, vector<join_result>> hybrid_tables;
    // map<int, vector<int>> hybrid_join_attrs;
    // vector<int> hybrid_join_order = {0,2};
    // hybrid_tables[0] = partial;
    // hybrid_tables[2] = join_tables[2];
    // hybrid_join_attrs[0] = partial[0].attr_id;
    // hybrid_join_attrs[2] = join_attrs[2];
    // ts = clock();
    // vector<join_result> hybrid_answer = durable_join.n_star_durable_join(hybrid_tables, hybrid_join_order, hybrid_join_attrs);
    // te = clock();
    // total += te - ts;
    // cerr << "hybrid answer size: " << hybrid_answer.size() << ' '
    //         << "time usage: " << (double) total / CLOCKS_PER_SEC << endl;
    
    // ts = clock();
    // vector<join_result> answer = durable_join.line_3_join(join_tables[0], join_tables[1], join_tables[2], join_attrs, -1);
    // te = clock();
    // answer = durable_join.remove_invalid(answer);
    // cerr << "answer size: " << answer.size() << ' '
    //         << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << ' '
    //         << "filter time: " << (double) filter_time / CLOCKS_PER_SEC << endl;


    // ts = clock();
    // int root_id = 0;
    // vector<join_result> yannakakis_answer = 
    //     durable_join.non_hierarchy_durable_join_v2("/home/home1/jygao/workspace/DurableJoin/yannakakis_line_3_join_tree.dat", 
    //                     join_tables, join_order, join_attrs, root_id, -1);
    // te = clock();
    // yannakakis_answer = durable_join.remove_invalid(yannakakis_answer);
    // cerr << "answer size: " << yannakakis_answer.size() << ' '
    //         << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << endl;
}
void tpcQ5Join(int durability) {
    clock_t ts, te;
    TableLoader tl;

    tl.load_test_table(0, "/usr/project/xtmp/jygao/TPC-BiH/time_supplier_slim.csv", 2);
    tl.load_test_table(1, "/usr/project/xtmp/jygao/TPC-BiH/time_lineitem_slim.csv", 2);
    tl.load_test_table(2, "/usr/project/xtmp/jygao/TPC-BiH/time_orders_slim.csv", 2);
    tl.load_test_table(3, "/usr/project/xtmp/jygao/TPC-BiH/time_customer_slim.csv", 2);

    vector<int> join_order = vector<int>{0,1,2,3};
    map<int, vector<int>> join_attrs;
    map<int, vector<join_result>> join_tables;
    int total_num_attrs = 4;
    
    // global join attrs id:
    // A:0, B:1, C:2
    join_attrs[0] = vector<int>{1}; // B
    join_attrs[1] = vector<int>{1,2}; // BC
    join_attrs[2] = vector<int>{2,3}; // C
    join_attrs[3] = vector<int>{3}; 

    ts = clock();
    join_tables[0] = tl.prepare(0, total_num_attrs, vector<int>{1}, join_attrs[0], durability, true);
    join_tables[1] = tl.prepare(1, total_num_attrs, vector<int>{0,1}, join_attrs[1], durability, true);
    join_tables[2] = tl.prepare(2, total_num_attrs, vector<int>{0,1}, join_attrs[2], durability, true);
    join_tables[3] = tl.prepare(3, total_num_attrs, vector<int>{0}, join_attrs[3], durability, true);
    cerr << "filtered table size : " << endl;
    for (int id : join_order)
        cerr << join_tables[id].size() << std::endl;
    te = clock();
    clock_t filter_time = te - ts;

    Solution durable_join(total_num_attrs);
    
    ts = clock();
    std::vector<join_result> answer = durable_join.line_k_join(join_tables, join_attrs, -1);
    te = clock();
    cerr << "answer size: " << answer.size() << ' '
            << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << ' '
            << "filter time: " << (double) filter_time / CLOCKS_PER_SEC << endl;

    // clock_t total = 0;
    // vector<int> partial_join_order = vector<int>{0,1,2};
    // map<int, vector<int>> partial_join_attrs;
    // map<int, vector<join_result>> partial_join_tables;
    // for (int id : partial_join_order) {
    //     partial_join_attrs[id] = join_attrs[id];
    //     partial_join_tables[id] = join_tables[id];
    // }
    // ts = clock();
    // vector<join_result> partial = durable_join.multiway_durable_join_baseline(partial_join_tables, partial_join_order, partial_join_attrs, -1);
    // te = clock();
    // for (int i=0; i<partial.size(); ++i) {
    //     partial[i].table_id = 0;
    //     partial[i].idx = i;
    // }
    // cerr << (double) (te - ts) / CLOCKS_PER_SEC << endl;
    // total += te - ts;
    // map<int, vector<join_result>> hybrid_tables;
    // map<int, vector<int>> hybrid_join_attrs;
    // vector<int> hybrid_join_order = {0,3};
    // hybrid_tables[0] = partial;
    // hybrid_tables[3] = join_tables[3];
    // hybrid_join_attrs[0] = partial[0].attr_id;
    // hybrid_join_attrs[3] = join_attrs[3];
    // ts = clock();
    // vector<join_result> hybrid_answer = durable_join.n_star_durable_join(hybrid_tables, hybrid_join_order, hybrid_join_attrs);
    // te = clock();
    // total += te - ts;
    // cerr << "hybrid answer size: " << hybrid_answer.size() << ' '
    //         << "time usage: " << (double) total / CLOCKS_PER_SEC << endl;


    // ts = clock();
    // int root_id = 0;
    // vector<join_result> yannakakis_answer = 
    //     durable_join.non_hierarchy_durable_join_v2("/home/home1/jygao/workspace/DurableJoin/yannakakis_line_4_join_tree.dat", 
    //                     join_tables, join_order, join_attrs, root_id, -1);
    // te = clock();
    // yannakakis_answer = durable_join.remove_invalid(yannakakis_answer);
    // cerr << "answer size: " << yannakakis_answer.size() << ' '
    //         << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << endl;
    
    
    // ts = clock();
    // vector<join_result> baseline = 
    //     durable_join.multiway_durable_join_baseline(join_tables, join_order, join_attrs, -1, -1);
    // te = clock();
    // baseline = durable_join.remove_invalid(baseline);
    // cerr << "ground truth size: " << baseline.size() << ' '
    //         << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << ' '
    //         << "filter time: " << (double) filter_time / CLOCKS_PER_SEC << endl;
}

void tpcQ9Join(int durability) {
    clock_t ts, te;
    TableLoader tl;

    tl.load_test_table(0, "/usr/project/xtmp/jygao/TPC-BiH/time_partsupp_slim.csv", 2);
    tl.load_test_table(1, "/usr/project/xtmp/jygao/TPC-BiH/time_lineitem_slim.csv", 2);
    tl.load_test_table(2, "/usr/project/xtmp/jygao/TPC-BiH/time_orders_slim.csv", 2);

    vector<int> join_order = vector<int>{0,1,2};
    map<int, vector<int>> join_attrs;
    map<int, vector<join_result>> join_tables;
    int total_num_attrs = 3;
    
    // global join attrs id:
    // A:0, B:1, C:2
    join_attrs[0] = vector<int>{1}; // B
    join_attrs[1] = vector<int>{1,2}; // BC
    join_attrs[2] = vector<int>{2}; // C

    ts = clock();
    join_tables[0] = tl.prepare(0, total_num_attrs, vector<int>{1}, join_attrs[0], durability, true);
    join_tables[1] = tl.prepare(1, total_num_attrs, vector<int>{0,1}, join_attrs[1], durability, true);
    join_tables[2] = tl.prepare(2, total_num_attrs, vector<int>{0}, join_attrs[2], durability, true);
    cerr << "filtered table size : " << endl;
    for (int id : join_order)
        cerr << join_tables[id].size() << std::endl;
    te = clock();
    clock_t filter_time = te - ts;

    Solution durable_join(total_num_attrs);

    // ts = clock();
    // vector<join_result> answer = durable_join.line_3_join(join_tables[0], join_tables[1], join_tables[2], join_attrs, -1);
    // te = clock();
    // answer = durable_join.remove_invalid(answer);
    // cerr << "answer size: " << answer.size() << ' '
    //         << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << ' '
    //         << "filter time: " << (double) filter_time / CLOCKS_PER_SEC << endl;


    // ts = clock();
    // int root_id = 0;
    // vector<join_result> yannakakis_answer = 
    //     durable_join.non_hierarchy_durable_join_v2("/home/home1/jygao/workspace/DurableJoin/yannakakis_line_3_join_tree.dat", 
    //                     join_tables, join_order, join_attrs, root_id, -1);
    // te = clock();
    // yannakakis_answer = durable_join.remove_invalid(yannakakis_answer);
    // cerr << "answer size: " << yannakakis_answer.size() << ' '
    //         << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << endl;
    
    clock_t total = 0;
    std::vector<int> common_attrs = get_intersection(join_attrs[1], join_attrs[2]);
    std::vector<int> union_attrs = get_union(join_attrs[1], join_attrs[2]);
    ts = clock();
    durable_join.sort_by_join_attrs(join_tables[1], common_attrs);
    durable_join.sort_by_join_attrs(join_tables[2], common_attrs);
    vector<join_result> partial = durable_join.pairwise_forward_scan_temporal_join(join_tables[1], join_tables[2], common_attrs, union_attrs, -1);
    te = clock();
    for (int i=0; i<partial.size(); ++i) {
        partial[i].table_id = 1;
        partial[i].idx = i;
    }
    cerr << (double) (te - ts) / CLOCKS_PER_SEC << endl;
    total += te - ts;
    map<int, vector<join_result>> hybrid_tables;
    map<int, vector<int>> hybrid_join_attrs;
    vector<int> hybrid_join_order = {0,1};
    hybrid_tables[0] = join_tables[0];
    hybrid_tables[1] = partial;
    hybrid_join_attrs[0] = join_attrs[0];
    hybrid_join_attrs[1] = partial[0].attr_id;
    ts = clock();
    vector<join_result> hybrid_answer = durable_join.n_star_durable_join(hybrid_tables, hybrid_join_order, hybrid_join_attrs);
    te = clock();
    total += te - ts;
    cerr << "hybrid answer size: " << hybrid_answer.size() << ' '
            << "time usage: " << (double) total / CLOCKS_PER_SEC << endl;
    
    // ts = clock();
    // vector<join_result> baseline = 
    //     durable_join.multiway_durable_join_baseline(join_tables, join_order, join_attrs, -1, -1);
    // te = clock();
    // baseline = durable_join.remove_invalid(baseline);
    // cerr << "ground truth size: " << baseline.size() << ' '
    //         << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << ' '
    //         << "filter time: " << (double) filter_time / CLOCKS_PER_SEC << endl;
}

void tpcQ9_4_Join(int durability) {
    clock_t ts, te;
    TableLoader tl;

    tl.load_test_table(0, "/usr/project/xtmp/jygao/TPC-BiH/time_partsupp_slim.csv", 2);
    tl.load_test_table(1, "/usr/project/xtmp/jygao/TPC-BiH/time_lineitem_slim.csv", 2);
    tl.load_test_table(2, "/usr/project/xtmp/jygao/TPC-BiH/time_orders_slim.csv", 2);
    tl.load_test_table(3, "/usr/project/xtmp/jygao/TPC-BiH/time_customer_slim.csv", 1);

    vector<int> join_order = vector<int>{0,1,2,3};
    map<int, vector<int>> join_attrs;
    map<int, vector<join_result>> join_tables;
    int total_num_attrs = 4;
    
    // global join attrs id:
    // A:0, B:1, C:2
    join_attrs[0] = vector<int>{1}; // B
    join_attrs[1] = vector<int>{1,2}; // BC
    join_attrs[2] = vector<int>{2,3}; // C
    join_attrs[3] = vector<int>{3}; // C

    ts = clock();
    join_tables[0] = tl.prepare(0, total_num_attrs, vector<int>{1}, join_attrs[0], durability, true);
    join_tables[1] = tl.prepare(1, total_num_attrs, vector<int>{0,1}, join_attrs[1], durability, true);
    join_tables[2] = tl.prepare(2, total_num_attrs, vector<int>{0,1}, join_attrs[2], durability, true);
    join_tables[3] = tl.prepare(3, total_num_attrs, vector<int>{0}, join_attrs[3], durability, true);
    cerr << "filtered table size : " << endl;
    for (int id : join_order)
        cerr << join_tables[id].size() << std::endl;
    te = clock();
    clock_t filter_time = te - ts;

    Solution durable_join(total_num_attrs);

    // ts = clock();
    // std::vector<join_result> answer = durable_join.line_k_join(join_tables, join_attrs, -1);
    // te = clock();
    // cerr << "answer size: " << answer.size() << ' '
    //         << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << ' '
    //         << "filter time: " << (double) filter_time / CLOCKS_PER_SEC << endl;

    clock_t total = 0;
    vector<int> partial_join_order = vector<int>{0,1,2};
    map<int, vector<int>> partial_join_attrs;
    map<int, vector<join_result>> partial_join_tables;
    for (int id : partial_join_order) {
        partial_join_attrs[id] = join_attrs[id];
        partial_join_tables[id] = join_tables[id];
    }
    ts = clock();
    vector<join_result> partial = durable_join.multiway_durable_join_baseline(partial_join_tables, partial_join_order, partial_join_attrs, -1);
    te = clock();
    for (int i=0; i<partial.size(); ++i) {
        partial[i].table_id = 0;
        partial[i].idx = i;
    }
    cerr << (double) (te - ts) / CLOCKS_PER_SEC << endl;
    total += te - ts;
    map<int, vector<join_result>> hybrid_tables;
    map<int, vector<int>> hybrid_join_attrs;
    vector<int> hybrid_join_order = {0,3};
    hybrid_tables[0] = partial;
    hybrid_tables[3] = join_tables[3];
    hybrid_join_attrs[0] = partial[0].attr_id;
    hybrid_join_attrs[3] = join_attrs[3];
    ts = clock();
    vector<join_result> hybrid_answer = durable_join.n_star_durable_join(hybrid_tables, hybrid_join_order, hybrid_join_attrs);
    te = clock();
    total += te - ts;
    cerr << "hybrid answer size: " << hybrid_answer.size() << ' '
            << "time usage: " << (double) total / CLOCKS_PER_SEC << endl;


    // ts = clock();
    // int root_id = 0;
    // vector<join_result> yannakakis_answer = 
    //     durable_join.non_hierarchy_durable_join_v2("/home/home1/jygao/workspace/DurableJoin/yannakakis_line_4_join_tree.dat", 
    //                     join_tables, join_order, join_attrs, root_id, -1);
    // te = clock();
    // yannakakis_answer = durable_join.remove_invalid(yannakakis_answer);
    // cerr << "answer size: " << yannakakis_answer.size() << ' '
    //         << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << endl;
    
    
    // ts = clock();
    // vector<join_result> baseline = 
    //     durable_join.multiway_durable_join_baseline(join_tables, join_order, join_attrs, -1, -1);
    // te = clock();
    // baseline = durable_join.remove_invalid(baseline);
    // cerr << "ground truth size: " << baseline.size() << ' '
    //         << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << ' '
    //         << "filter time: " << (double) filter_time / CLOCKS_PER_SEC << endl;
}

void tpcStarJoinTest(int durability) {
    clock_t ts, te;
    TableLoader tl;

    tl.load_test_table(0, "/home/home1/jygao/workspace/DurableJoin/data/TPC/TPC-5.txt", 2);
    tl.load_test_table(1, "/home/home1/jygao/workspace/DurableJoin/data/TPC/TPC-5.txt", 2);
    tl.load_test_table(2, "/home/home1/jygao/workspace/DurableJoin/data/TPC/TPC-5.txt", 2);

    // tl.load_test_table(0, "/home/home1/jygao/workspace/DurableJoin/data/test-db-sample/R1_AB.txt", 2);
    // tl.load_test_table(1, "/home/home1/jygao/workspace/DurableJoin/data/test-db-sample/R1_AB.txt", 2);
    // tl.load_test_table(2, "/home/home1/jygao/workspace/DurableJoin/data/test-db-sample/R1_AB.txt", 2);

    vector<int> join_order = vector<int>{0,1,2};
    map<int, vector<int>> join_attrs;
    map<int, vector<join_result>> join_tables;
    int total_num_attrs = 4;
    
    // global join attrs id:
    // A:0, B:1, C:2, D:3
    join_attrs[0] = vector<int>{0,1}; // AB
    join_attrs[1] = vector<int>{0,2}; // AC
    join_attrs[2] = vector<int>{0,3}; // AD

    ts = clock();
    join_tables[0] = tl.prepare(0, total_num_attrs, vector<int>{0,1}, join_attrs[0], durability);
    join_tables[1] = tl.prepare(1, total_num_attrs, vector<int>{0,1}, join_attrs[1], durability);
    join_tables[2] = tl.prepare(2, total_num_attrs, vector<int>{0,1}, join_attrs[2], durability);
    te = clock();
    clock_t filter_time = te - ts;

    Solution durable_join(total_num_attrs);

    ts = clock();
    vector<join_result> answer = durable_join.hierarchy_durable_join("/home/home1/jygao/workspace/DurableJoin/join_tree_star_join.dat", 
                    join_tables, join_order, join_attrs, 30, durability);
    te = clock();
    answer = durable_join.remove_invalid(answer);
    cerr << "answer size: " << answer.size() << ' '
            << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << ' '
            << "filter time: " << (double) filter_time / CLOCKS_PER_SEC << endl;
    
    ts = clock();
    vector<join_result> baseline = 
        durable_join.multiway_durable_join_baseline(join_tables, join_order, join_attrs, durability);
    te = clock();
    baseline = durable_join.remove_invalid(baseline);
    cerr << "ground truth size: " << baseline.size() << ' '
            << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << ' '
            << "filter time: " << (double) filter_time / CLOCKS_PER_SEC << endl;

    durable_join.difference(answer, baseline);
}

void RealStar3JoinTest(int durability) {
    clock_t ts, te;
    TableLoader tl;
    tl.load_test_table(0, "/usr/project/xtmp/jygao/flight-db/flights_slim.csv", 2);
    tl.load_test_table(1);
    tl.load_test_table(2);

    vector<int> join_order = vector<int>{0,1,2};
    map<int, vector<int>> join_attrs;
    map<int, vector<join_result>> join_tables;
    int total_num_attrs = 4;
    
    // global join attrs id:
    // A:0, B:1, C:2, D:3
    join_attrs[0] = vector<int>{0,1}; // AB
    join_attrs[1] = vector<int>{0,2}; // AC
    join_attrs[2] = vector<int>{0,3}; // AD

    ts = clock();
    join_tables[0] = tl.prepare(0, total_num_attrs, vector<int>{0,1}, join_attrs[0], durability, true);
    join_tables[1] = tl.prepare(1, total_num_attrs, vector<int>{0,1}, join_attrs[1], durability, true);
    join_tables[2] = tl.prepare(2, total_num_attrs, vector<int>{0,1}, join_attrs[2], durability, true);

    te = clock();
    clock_t filter_time = te - ts;

    Solution durable_join(total_num_attrs);

    // ts = clock();
    // vector<join_result> answer = durable_join.hierarchy_durable_join("/home/home1/jygao/workspace/DurableJoin/join_tree_star_join.dat", 
    //                 join_tables, join_order, join_attrs, 30, -1);
    // // vector<join_result> answer = durable_join.hierarchy_durable_join("/home/home1/jygao/workspace/DurableJoin/join_tree_star_join.dat", 
    // //                 join_tables, join_order, join_attrs, 30, 0);
    // te = clock();
    // answer = durable_join.remove_invalid(answer);
    // cerr << "answer size: " << answer.size() << ' '
    //         << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << ' '
    //         << "filter time: " << (double) filter_time / CLOCKS_PER_SEC << endl;
    // // for (auto item : answer)
    // //     item.print();

    ts = clock();
    vector<join_result> better_answer = durable_join.n_star_durable_join(join_tables, join_order, join_attrs);
    te = clock();
    better_answer = durable_join.remove_invalid(better_answer);
    cerr << "answer size: " << better_answer.size() << ' '
            << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << ' '
            << "filter time: " << (double) filter_time / CLOCKS_PER_SEC << endl;


    // ts = clock();
    // vector<join_result> baseline = 
    //     durable_join.multiway_durable_join_baseline(join_tables, join_order, join_attrs, -1, -1);
    // te = clock();
    // baseline = durable_join.remove_invalid(baseline);
    // cerr << "ground truth size: " << baseline.size() << ' '
    //         << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << ' '
    //         << "filter time: " << (double) filter_time / CLOCKS_PER_SEC << endl;
}

void RealStar4JoinTest(int durability) {
    clock_t ts, te;
    TableLoader tl;
    tl.load_test_table(0, "/usr/project/xtmp/jygao/flight-db/flights_slim.csv", 2);
    tl.load_test_table(1);
    tl.load_test_table(2);
    tl.load_test_table(3);

    vector<int> join_order = vector<int>{0,1,2,3};
    map<int, vector<int>> join_attrs;
    map<int, vector<join_result>> join_tables;
    int total_num_attrs = 5;
    
    // global join attrs id:
    // A:0, B:1, C:2, D:3, E:4
    join_attrs[0] = vector<int>{0,1}; // AB
    join_attrs[1] = vector<int>{0,2}; // AC
    join_attrs[2] = vector<int>{0,3}; // AD
    join_attrs[3] = vector<int>{0,4}; // AE

    ts = clock();
    join_tables[0] = tl.prepare(0, total_num_attrs, vector<int>{0,1}, join_attrs[0], durability, true);
    join_tables[1] = tl.prepare(1, total_num_attrs, vector<int>{0,1}, join_attrs[1], durability, true);
    join_tables[2] = tl.prepare(2, total_num_attrs, vector<int>{0,1}, join_attrs[2], durability, true);
    join_tables[3] = tl.prepare(3, total_num_attrs, vector<int>{0,1}, join_attrs[3], durability, true);

    te = clock();
    clock_t filter_time = te - ts;

    Solution durable_join(total_num_attrs);

    // ts = clock();
    // vector<join_result> answer = durable_join.hierarchy_durable_join("/home/home1/jygao/workspace/DurableJoin/join_tree_star_join.dat", 
    //                 join_tables, join_order, join_attrs, 30, -1);
    // // vector<join_result> answer = durable_join.hierarchy_durable_join("/home/home1/jygao/workspace/DurableJoin/join_tree_star_join.dat", 
    // //                 join_tables, join_order, join_attrs, 30, 0);
    // te = clock();
    // answer = durable_join.remove_invalid(answer);
    // cerr << "answer size: " << answer.size() << ' '
    //         << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << ' '
    //         << "filter time: " << (double) filter_time / CLOCKS_PER_SEC << endl;
    // // for (auto item : answer)
    // //     item.print();

    // ts = clock();
    // vector<join_result> better_answer = durable_join.n_star_durable_join(join_tables, join_order, join_attrs);
    // te = clock();
    // better_answer = durable_join.remove_invalid(better_answer);
    // cerr << "answer size: " << better_answer.size() << ' '
    //         << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << ' '
    //         << "filter time: " << (double) filter_time / CLOCKS_PER_SEC << endl;


    ts = clock();
    vector<join_result> baseline = 
        durable_join.multiway_durable_join_baseline(join_tables, join_order, join_attrs, -1, -1);
    te = clock();
    baseline = durable_join.remove_invalid(baseline);
    cerr << "ground truth size: " << baseline.size() << ' '
            << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << ' '
            << "filter time: " << (double) filter_time / CLOCKS_PER_SEC << endl;
}

void BowtieJoin(int durability) {
    clock_t ts, te;
    TableLoader tl;
    tl.load_test_table(0, "/usr/project/xtmp/jygao/flight-db/flights_slim.csv", 2);
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

    Solution durable_join(total_num_attrs);
    // durable_join.setup_table_index(join_order, join_tables);
    // durable_join.setup_hash_index(join_order, join_attrs, join_tables);

    // vector<int> join_order_part_1 = {0,1,4};
    // map<int, vector<join_result>> join_tables_part_1;
    // map<int, vector<int>> join_attrs_part_1;
    // for (int id : join_order_part_1) {
    //     join_tables_part_1[id] = join_tables[id];
    //     join_attrs_part_1[id] = join_attrs[id];
    // }
    // vector<int> join_order_part_2 = {2,3,5};
    // map<int, vector<join_result>> join_tables_part_2;
    // map<int, vector<int>> join_attrs_part_2;
    // for (int id : join_order_part_2) {
    //     join_tables_part_2[id] = join_tables[id];
    //     join_attrs_part_2[id] = join_attrs[id];
    // }
    // ts = clock();
    // std::vector<join_result> final_answer;
    // std::vector<join_result> part_1 = durable_join.durable_generic_join(join_tables_part_1, join_order_part_1, join_attrs_part_1, 0, durability);
    // // std::cerr << part_1.size() << std::endl;
    // // part_1[0].print();
    // std::vector<join_result> part_2 = durable_join.durable_generic_join(join_tables_part_2, join_order_part_2, join_attrs_part_2, 0, durability);
    // // std::cerr << part_2.size() << std::endl;
    // // part_2[0].print();
    // if (!part_1.empty() && !part_2.empty()) {
    //     std::vector<int> common_attrs = get_intersection(part_1[0].attr_id, part_2[0].attr_id);
    //     std::vector<int> union_attrs = get_union(part_1[0].attr_id, part_2[0].attr_id);
    //     final_answer = durable_join.pairwise_forward_scan_temporal_join(part_1, part_2, common_attrs, union_attrs, durability);
    // }
    // te = clock();
    // cerr << "answer size: " << final_answer.size() << ' '
    //         << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << ' '
    //         << "filter time: " << (double) filter_time / CLOCKS_PER_SEC << endl;

    ts = clock();
    vector<join_result> baseline = 
        durable_join.multiway_durable_join_baseline(join_tables, join_order, join_attrs, durability, -1);
    te = clock();
    cerr << "ground truth size: " << baseline.size() << ' '
            << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << ' '
            << "filter time: " << (double) filter_time / CLOCKS_PER_SEC << endl;
}

void dblpGenericJoinTest(int durability) {
    clock_t ts, te;
    TableLoader tl;
    tl.load_test_table(0, "/usr/project/xtmp/jygao/DBLP/inproceedings_aggregate_year.txt", 2);
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

    Solution durable_join(total_num_attrs);
    durable_join.setup_table_index(join_order, join_tables);
    durable_join.setup_hash_index(join_order, join_attrs, join_tables);

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
    ts = clock();
    std::vector<join_result> final_answer;
    std::vector<join_result> part_1 = durable_join.durable_generic_join(join_tables_part_1, join_order_part_1, join_attrs_part_1, 0, durability);
    // std::cerr << part_1.size() << std::endl;
    // part_1[0].print();
    std::vector<join_result> part_2 = durable_join.durable_generic_join(join_tables_part_2, join_order_part_2, join_attrs_part_2, 0, durability);
    // std::cerr << part_2.size() << std::endl;
    // part_2[0].print();
    if (!part_1.empty() && !part_2.empty()) {
        std::vector<int> common_attrs = get_intersection(part_1[0].attr_id, part_2[0].attr_id);
        std::vector<int> union_attrs = get_union(part_1[0].attr_id, part_2[0].attr_id);
        final_answer = durable_join.pairwise_forward_scan_temporal_join(part_1, part_2, common_attrs, union_attrs, durability);
    }
    te = clock();
    cerr << "answer size: " << final_answer.size() << ' '
            << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << ' '
            << "filter time: " << (double) filter_time / CLOCKS_PER_SEC << endl;

    ts = clock();
    vector<join_result> baseline = 
        durable_join.multiway_durable_join_baseline(join_tables, join_order, join_attrs, durability, -1);
    te = clock();
    cerr << "ground truth size: " << baseline.size() << ' '
            << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << ' '
            << "filter time: " << (double) filter_time / CLOCKS_PER_SEC << endl;

}

void employeeStarJoinTest(int durability) {

    TableLoader tl;
    tl.load_test_table(0, "/usr/xtmp/jygao/employee-db/title.txt", 2);
    tl.load_test_table(1, "/usr/xtmp/jygao/employee-db/salary.txt", 2);
    tl.load_test_table(2, "/usr/xtmp/jygao/employee-db/dept-emp.txt", 2);

    vector<int> join_order = vector<int>{0,1,2};
    map<int, vector<int>> join_attrs;
    map<int, vector<join_result>> join_tables;
    int total_num_attrs = 4;
    
    // global join attrs id:
    // A:0, B:1, C:2, D:3
    join_attrs[0] = vector<int>{0,1}; // AB
    join_attrs[1] = vector<int>{0,2}; // AC
    join_attrs[2] = vector<int>{0,3}; // AD

    join_tables[0] = tl.prepare(0, total_num_attrs, vector<int>{0,1}, join_attrs[0], durability, true);
    join_tables[1] = tl.prepare(1, total_num_attrs, vector<int>{0,1}, join_attrs[1], durability, true);
    join_tables[2] = tl.prepare(2, total_num_attrs, vector<int>{0,1}, join_attrs[2], durability, true);
    // join_tables[0] = tl.prepare(0, total_num_attrs, vector<int>{0,1}, join_attrs[0], durability, true);
    // join_tables[1] = tl.prepare(1, total_num_attrs, vector<int>{0,1}, join_attrs[1], durability, true);
    // join_tables[2] = tl.prepare(2, total_num_attrs, vector<int>{0,1}, join_attrs[2], durability, true);

    Solution durable_join(total_num_attrs);

    auto h_ts = high_resolution_clock::now();
    // vector<join_result> answer = durable_join.hierarchy_durable_join("/home/home1/jygao/workspace/DurableJoin/join_tree_star_join.dat", 
    //                 join_tables, join_order, join_attrs, 30, durability);
    vector<join_result> answer = durable_join.n_star_durable_join(join_tables, join_order, join_attrs);

    auto h_te = high_resolution_clock::now();
    auto h_duration = duration_cast<milliseconds>(h_te - h_ts);
    answer = durable_join.remove_invalid(answer);
    cerr << "answer size: " << answer.size() << ' '
            << "time usage: " << h_duration.count() << endl;
    
    auto b_ts = high_resolution_clock::now();
    vector<join_result> baseline = 
        durable_join.multiway_durable_join_baseline(join_tables, join_order, join_attrs, -1, -1);
    auto b_te = high_resolution_clock::now();
    auto b_duration = duration_cast<milliseconds>(b_te - b_ts);
    baseline = durable_join.remove_invalid(baseline);
    cerr << "ground truth size: " << baseline.size() << ' '
            << "time usage: " << b_duration.count() << endl;

    // durable_join.difference(answer, baseline);
}

void tpcKStarJoinTest(int durability) {
    clock_t ts, te;
    TableLoader tl;

    tl.load_test_table(0, "/home/home1/jygao/workspace/DurableJoin/data/TPC/TPC-2.txt", 2);
    tl.load_test_table(1, "/home/home1/jygao/workspace/DurableJoin/data/TPC/TPC-2.txt", 2);
    tl.load_test_table(2, "/home/home1/jygao/workspace/DurableJoin/data/TPC/TPC-2.txt", 2);
    tl.load_test_table(3, "/home/home1/jygao/workspace/DurableJoin/data/TPC/TPC-2.txt", 2);
    tl.load_test_table(4, "/home/home1/jygao/workspace/DurableJoin/data/TPC/TPC-2.txt", 2);
    tl.load_test_table(5, "/home/home1/jygao/workspace/DurableJoin/data/TPC/TPC-2.txt", 2);

    vector<int> join_order = vector<int>{0,1,2,3,4,5};
    map<int, vector<int>> join_attrs;
    map<int, vector<join_result>> join_tables;
    int total_num_attrs = 7;
    
    // global join attrs id:
    // A:0, B:1, C:2, D:3, E:4, F:5, G:6
    join_attrs[0] = vector<int>{0,1}; // AB
    join_attrs[1] = vector<int>{0,2}; // AC
    join_attrs[2] = vector<int>{0,3}; // AD
    join_attrs[3] = vector<int>{0,4}; // AE
    join_attrs[4] = vector<int>{0,5}; // AF
    join_attrs[5] = vector<int>{0,6}; // AG

    ts = clock();
    join_tables[0] = tl.prepare(0, total_num_attrs, vector<int>{0,1}, join_attrs[0], durability);
    join_tables[1] = tl.prepare(1, total_num_attrs, vector<int>{0,1}, join_attrs[1], durability);
    join_tables[2] = tl.prepare(2, total_num_attrs, vector<int>{0,1}, join_attrs[2], durability);
    join_tables[3] = tl.prepare(3, total_num_attrs, vector<int>{0,1}, join_attrs[3], durability);
    join_tables[4] = tl.prepare(4, total_num_attrs, vector<int>{0,1}, join_attrs[4], durability);
    join_tables[5] = tl.prepare(5, total_num_attrs, vector<int>{0,1}, join_attrs[5], durability);
    te = clock();
    clock_t filter_time = te - ts;

    Solution durable_join(total_num_attrs);

    ts = clock();
    vector<join_result> answer = durable_join.hierarchy_durable_join("/home/home1/jygao/workspace/DurableJoin/join_tree_6_star_join.dat", 
                    join_tables, join_order, join_attrs, 30, durability);
    te = clock();
    answer = durable_join.remove_invalid(answer);
    cerr << "answer size: " << answer.size() << ' '
        << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << ' '
            << "filter time: " << (double) filter_time / CLOCKS_PER_SEC << endl;
    // for (auto item : answer)
    //     item.print();
    
    ts = clock();
    vector<join_result> baseline = 
        durable_join.multiway_durable_join_baseline(join_tables, join_order, join_attrs, durability);
    te = clock();
    baseline = durable_join.remove_invalid(baseline);
    cerr << "ground truth size: " << baseline.size() << ' '
           << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << ' '
            << "filter time: " << (double) filter_time / CLOCKS_PER_SEC << endl;

    durable_join.difference(answer, baseline);
    // ts = clock();
    // vector<join_result> naive_baseline = 
    //     durable_join.multiway_durable_join_baseline(join_tables, join_order, join_attrs, durability);
    // te = clock();
    // cerr << "ground truth size: " << naive_baseline.size() << ' '
    //         << "time usage: " << (double) (te - ts + filter_time) / CLOCKS_PER_SEC << endl;
}


void RealStar5JoinTest(int durability) {
    clock_t ts, te;
    TableLoader tl;

    tl.load_test_table(0, "/usr/project/xtmp/jygao/flight-db/flights_slim.csv", 2);
    tl.load_test_table(1);
    tl.load_test_table(2);
    tl.load_test_table(3);
    tl.load_test_table(4);

    vector<int> join_order = vector<int>{0,1,2,3,4};
    map<int, vector<int>> join_attrs;
    map<int, vector<join_result>> join_tables;
    int total_num_attrs = 6;
    
    // global join attrs id:
    // A:0, B:1, C:2, D:3, E:4, F:5, G:6
    join_attrs[0] = vector<int>{0,1}; // AB
    join_attrs[1] = vector<int>{0,2}; // AC
    join_attrs[2] = vector<int>{0,3}; // AD
    join_attrs[3] = vector<int>{0,4}; // AE
    join_attrs[4] = vector<int>{0,5}; // AF

    ts = clock();
    join_tables[0] = tl.prepare(0, total_num_attrs, vector<int>{0,1}, join_attrs[0], durability, true);
    join_tables[1] = tl.prepare(1, total_num_attrs, vector<int>{0,1}, join_attrs[1], durability, true);
    join_tables[2] = tl.prepare(2, total_num_attrs, vector<int>{0,1}, join_attrs[2], durability, true);
    join_tables[3] = tl.prepare(3, total_num_attrs, vector<int>{0,1}, join_attrs[3], durability, true);
    join_tables[4] = tl.prepare(4, total_num_attrs, vector<int>{0,1}, join_attrs[4], durability, true);

    te = clock();
    clock_t filter_time = te - ts;

    Solution durable_join(total_num_attrs);

    ts = clock();
    vector<join_result> answer = durable_join.n_star_durable_join(join_tables, join_order, join_attrs);
    te = clock();
    answer = durable_join.remove_invalid(answer);
    cerr << "answer size: " << answer.size() << ' '
        << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << ' '
            << "filter time: " << (double) filter_time / CLOCKS_PER_SEC << endl;
    
    // ts = clock();
    // vector<join_result> baseline = 
    //     durable_join.multiway_durable_join_baseline(join_tables, join_order, join_attrs, -1, -1);
    // te = clock();
    // baseline = durable_join.remove_invalid(baseline);
    // cerr << "ground truth size: " << baseline.size() << ' '
    //        << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << ' '
    //         << "filter time: " << (double) filter_time / CLOCKS_PER_SEC << endl;

}

int main(int argc, char* argv[]) {
    int durability = stoi(argv[1]);
    string exp_type(argv[2]);
    cerr << "durability: " << durability << " exp type: " << exp_type << endl;
    // dataLoaderTest();
    // synLoaderTest();
    // JoinBaselineTest(durability);
    // JoinBaselineTest_v2();
    // HierachicalJoinTest();
    // UtilityTest();
    // Line3JoinTest(durability);
    // taxiStarJoinTest();
    // taxiStarJoinFullTest();
    if (exp_type == "line-sweep-plane")
        Line3JoinSweepPlaneTest(durability);
    if (exp_type == "yannakakis")
        YannakakisTest(durability);
    if (exp_type == "cycle-3")
        Cyclic3Join(durability);
    if (exp_type == "cycle-4")
        Cyclic4Join(durability);
    if (exp_type == "cycle-5")
        Cyclic5Join(durability);
    if (exp_type == "bowtie")
        BowtieJoin(durability);
    if (exp_type == "baseline-test")
        JoinBaselineTest(durability);
    if (exp_type == "line-counting")
        Line3CountingTest(durability);
    if (exp_type == "interval-tree")
        IntervalTreeTest(durability);
    if (exp_type == "syn-line")
        SynLineJoinTest(durability);
    if (exp_type == "syn-star")
        starJoinTest(durability);
    if (exp_type == "syn-cyclic")
        cyclicJoinTest(durability);
    if (exp_type == "line-3")
        RealLine3JoinTest(durability);
    if (exp_type == "line-4")
        RealLine4JoinTest(durability);
    if (exp_type == "line-5")
        RealLine5JoinTest(durability);
    if (exp_type == "star-3")
        RealStar3JoinTest(durability);
    if (exp_type == "star-4")
        RealStar4JoinTest(durability);
    if (exp_type == "star-5")
        RealStar5JoinTest(durability);
    if (exp_type == "dblp-generic") 
        dblpGenericJoinTest(durability);
    if (exp_type == "employee-star")
        employeeStarJoinTest(durability);
    if (exp_type == "dblp-line-k")
        dblpLineKJoinTest(durability);
    if (exp_type == "tpc-star")
        tpcStarJoinTest(durability);
    if (exp_type == "tpc-star-k")
        tpcKStarJoinTest(durability);
    if (exp_type == "tpc-h-Q3")
        tpcQ3Join(durability);
    if (exp_type == "tpc-h-Q5")
        tpcQ5Join(durability);
    if (exp_type == "tpc-h-Q9-3")
        tpcQ9Join(durability);
    if (exp_type == "tpc-h-Q9-4")
        tpcQ9_4_Join(durability);
    return 0;
}
