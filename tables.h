#ifndef TABLES_H
#define TABLES_H

#include <functional>
#include <queue>
#include <map>
#include <vector>
#include <set>
#include <tuple>
#include <iostream>

#include <unordered_map>
#include <unordered_set>

struct VectorHasher {
    int operator()(const std::vector<int> &V) const {
        int hash = V.size();
        for(auto &i : V) {
            hash ^= i + 0x9e3779b9 + (hash << 6) + (hash >> 2);
        }
        return hash;
    }
};

struct join_result {
    // id from each table
    std::vector<int> id; 
    // join attributes
    std::vector<int> attrs;
    // join attributes global id
    std::vector<int> attr_id;
    // from which table this record comes from
    int table_id;
    // the idx of record
    int idx;
    // time interval
    int t_start;
    int t_end;
    void print() {
        std::cout << table_id << ' ';
        std::cout << '[';
        for (auto& v : id)
            std::cout << v << ':';
        std::cout << "] ";
        std::cout << idx << " ";
        std::cout << "[ ";
        for (auto& v : attrs)
            std::cout << v << ' ';
        std::cout << "] ";
        std::cout << '[' << t_start << ',' << t_end << "] ";
        for (auto v : attr_id)
            std::cout << v << ' ';
        std::cout << std::endl;
    }

    bool operator< (const join_result& a) const {
        for (int i=0; i<a.attrs.size(); ++i) {
            if (a.attrs[i] == this->attrs[i])
                continue;
            else
                return a.attrs[i] <= this->attrs[i];
        }
        return true;
    }

    bool operator== (const join_result& a) const {
        for (int i=0; i<a.attrs.size(); ++i) {
            if (a.attrs[i] != this->attrs[i])
                return false;
        }
        if (a.t_start != this->t_start || a.t_end != this->t_end)
            return false;
        return true;
    }

    // // new representation for table attributes
    // std::map<int, int> table;
};

// struct JoinRecord {
//     std::vector<int> attrs;
//     int timestamp;
// };

// for priority queue use only (time, table_id, record_id)
typedef std::tuple<int,int,int> queue_tuple;
struct queueCmp {
    bool operator()(const queue_tuple& s1, const queue_tuple& s2) {
       return std::get<0>(s1) < std::get<0>(s2);
   }
};
struct queueCmpReverse {
    bool operator()(const queue_tuple& s1, const queue_tuple& s2) {
       return std::get<0>(s1) > std::get<0>(s2);
   }
};

struct JoinTreeNode {
    JoinTreeNode() {
        parent = nullptr;
        node_id = -1;
        attrs_num = 0;
    }
    JoinTreeNode(int id) {
        parent = nullptr;
        node_id = id;
        attrs_num = 0;
    }
    JoinTreeNode* parent;
    std::vector<JoinTreeNode*> children;
    int node_id;
    int attrs_num;
    
    // the join attributes (represented by global id) represented by this leaf node
    std::vector<int> join_attrs;
    // // a min_heap (of t_start) for all combination of join attributes
    // // key: (projection) join attributes in its parent node
    // std::map<std::vector<int>, std::set<queue_tuple>>  min_t;
    // a max counter for t_start from its children's min_heap
    // key: join attributes in the current node
    std::map<std::vector<int>, queue_tuple> max_t;
    // // for leaf node use only, representing base table with record id
    // std::map<std::vector<int>, std::unordered_set<int>> base_table;


    // a min_heap (of t_start) for all combination of join attributes
    // key: (projection) join attributes in its parent node
    std::unordered_map<std::vector<int>, std::set<queue_tuple>, VectorHasher>  min_t;
    // // a max counter for t_start from its children's min_heap
    // // key: join attributes in the current node
    // std::unordered_map<std::vector<int>, queue_tuple, VectorHasher> max_t;
    // for leaf node use only, representing base table with record id
    std::unordered_map<std::vector<int>, std::unordered_set<int>, VectorHasher> base_table;

    // // A reservoir for records that are not uploaded to its parents
    // std::map<std::vector<int>, std::vector<queue_tuple>> reservoir;
    // // a max_heap (of t_start) for all combination of join attributes
    // std::map<std::vector<int>, std::priority_queue<queue_tuple, std::vector<queue_tuple>, queueCmpReverse>> max_t;
    // // the record to be purged using lazy update
    // std::map<int, std::set<int>> purge_id;
    // // check whether the given attributes exists in all of its children roots
    // std::map<std::vector<int>, std::set<int>> exists_counter;
    // std::map<std::vector<int>, std::priority_queue<queue_tuple, std::vector<queue_tuple>, queueCmp>> min_t;
};

struct YannakakisJoinTreeNode {
    YannakakisJoinTreeNode() {
        node_id = -1;
        attrs_num = 0;
    }
    YannakakisJoinTreeNode(int id) {
        node_id = id;
        attrs_num = 0;
    }
    YannakakisJoinTreeNode* parent = nullptr;
    std::vector<YannakakisJoinTreeNode*> children;
    int node_id;
    int attrs_num;
    
    // the join attributes (represented by global id) represented by this leaf node
    std::vector<int> join_attrs;

    // the original join table
    std::unordered_map<std::vector<int>, std::unordered_set<int>, VectorHasher> origin_table;

    // project maping among join attributes: parent -> child
    std::unordered_map<std::vector<int>, std::set<std::vector<int>>, VectorHasher> projection_down;

    // project maping among join attributes: parent <- child
    std::unordered_map<std::vector<int>, std::set<std::vector<int>>, VectorHasher> projection_up;

    // // non_dangling records from the original table for the final join
    // // std::vector<join_result> join_table;
    // std::unordered_map<std::vector<int>, std::unordered_set<int>, VectorHasher> join_table;

    std::unordered_set<std::vector<int>, VectorHasher> non_dangling_join_values;

    // values that are used for semi-joins
    // std::vector<join_result> join_values;

    // temporary container for intermidate results when removing dangling records
    // std::vector<join_result> non_dangling_records;
};

struct LevelHashMap {
    LevelHashMap() {}

    LevelHashMap(int id) {
        join_attr_id = id;
    }
    // join attrs global ID
    int join_attr_id;

    // base level hash map
    std::set<int> base_key;

    // child level hash map
    std::unordered_map<int, LevelHashMap*> children;

};

struct test_table {
    int row;
    std::vector<int> attrs;
    int table_id;
    int t_start;
    int t_end;
    void print() {
        std::cout << row << " : ";
        for (auto& item : attrs)
            std::cout << item << ' ';
        std::cout << '[' << t_start << ',' << t_end << ']' << std::endl;
    }
};

struct taxi_table_basic {
    void set_row (const int r) {
        row = r;
        data[0] = r;
    }
    void set_id (const int i) {
        id = i;
        data[1] = i;
    }
    void set_pickup_id (const int p) {
        pickup_id = p;
        data[2] = p;
    }
    void set_dropoff_id (const int d) {
        dropoff_id = d;
        data[3] = d;
    }
    void set_t_start (const int ts) {
        t_start = ts;
        data[4] = ts;
    }
    void set_t_end (const int te) {
        t_end = te;
        data[5] = te;
    }
    void print() {
        std::cout << row << " : ";
        std::cout << pickup_id << ' ' << dropoff_id << ' ';
        std::cout << '[' << t_start << ',' << t_end << ']' << std::endl;
    }
    int row;
    int id;
    int pickup_id;
    int dropoff_id;
    int t_start;
    int t_end;
    std::map<int, int> data;
};

#endif