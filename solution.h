#ifndef SOLUTION_H
#define SOLUTION_H

#include "tables.h"
#include "utility.h"
#include "interval_tree.h"
#include <vector>
#include <tuple>

static uint64_t value_comparisons = 0;
static uint64_t temporal_comparisons = 0;

typedef IntervalTree<int, int> intervalTree;
typedef intervalTree::interval interval;
typedef intervalTree::interval_vector intervalVector;

class Solution {
public:
	Solution() {enumeration_exit_flag = 0;}
    Solution(int num) {total_num_attrs = num, enumeration_exit_flag = 0;}
	~Solution() {}

    // naive baseline join between two tables (for testing)
    std::vector<join_result> naive_durable_join(std::vector<join_result>& left_table, std::vector<join_result>& right_table, std::vector<int>& join_attrs, std::vector<int>& union_attrs, int durability);

    // pairwise join between two tables
    std::vector<join_result> pairwise_durable_join(std::vector<join_result>& left_table, std::vector<join_result>& right_table, std::vector<int>& join_attrs, std::vector<int>& union_attrs, int durability);

    // pairwise join betweeb two tables using temporal join
    std::vector<join_result> pairwise_durable_temporal_join(std::vector<join_result>& left_table, std::vector<join_result>& right_table, std::vector<int>& join_attrs, std::vector<int>& union_attrs, int durability);

     // pairwise join between two tables
    std::vector<join_result> pairwise_forward_scan_temporal_join(std::vector<join_result>& left_table, std::vector<join_result>& right_table, std::vector<int>& join_attrs, std::vector<int>& union_attrs, int durability);

    // multiway join between tables
    // join_order specifies the join order among multiple tables
    // join_attrs specifies the join attributes 
    // (NOTE: the join_attrs index starts from only from join attributes 
    // NOT its origin attributes (which could be many))
    // flag controls the pairwise join algorithm:
    // -1 for sort-merge-join with Forward Scan (no nested loop)
    // 0 for sort-merge-join
    // 1 for temporal join
    // 2 for naive loop-based join (for ground truth testing)
    std::vector<join_result> multiway_durable_join_baseline(std::map<int, std::vector<join_result>>& join_tables, 
                        std::vector<int>& join_order, std::map<int, std::vector<int>>& join_attrs, int durability, int flag=0, bool verbose=true);

    // hierarchy join between tables
    std::vector<join_result> hierarchy_durable_join(const std::string filename, std::map<int, std::vector<join_result>>& join_tables, 
                        std::vector<int>& join_order, std::map<int, std::vector<int>>& join_attrs, int root_id, int durability);
    
    std::vector<join_result> n_star_durable_join(std::map<int, std::vector<join_result>>& join_tables, 
                        std::vector<int>& join_order, std::map<int, std::vector<int>>& join_attrs);
    
    std::vector<join_result> n_star_durable_join_v2(std::map<int, std::vector<join_result>>& join_tables, 
                        std::vector<int>& join_order, std::map<int, std::vector<int>>& join_attrs);

    // (DEPRECATED) non-hierarchy join between tables
    std::vector<join_result> non_hierarchy_durable_join(const std::string filename, std::map<int, std::vector<join_result>>& join_tables, 
                        std::vector<int>& join_order,  std::map<int, std::vector<int>>& join_attrs, int root_id, int durability);
    
    // non-hierarchy join between tables - optimized for line joins
    std::vector<join_result> non_hierarchy_durable_join_v2(const std::string filename, std::map<int, std::vector<join_result>>& join_tables, 
                        std::vector<int>& join_order,  std::map<int, std::vector<int>>& join_attrs, int root_id, int durability);
    
    // // worst-case generic join
    // std::vector<join_result> generic_join(std::map<int, std::vector<join_result>>& join_tables, int index, int join_attrs_count, int durability);

    // // // project operation for generic join
    // // std::map<int, std::vector<join_result>> project(std::map<int, std::vector<join_result>>& join_tables, int index);

    // // select operation for generic join
    // std::vector<join_result> select(std::vector<join_result>& table, int table_id, int index, int join_value);

    // // Set-Intersect operation for generic join (essentially a join)
    // std::vector<join_result> set_intersect(std::map<int, std::vector<join_result>>& join_tables, int index, int durability);

    // worst-case durable generic join
    std::vector<join_result> durable_generic_join(std::map<int, std::vector<join_result>>& join_tables, std::vector<int>& join_order, std::map<int, std::vector<int>>& join_attrs, int index, int durability);

    // // worst-case generic join
    // std::set<std::vector<int>> generic_join(std::map<int, std::vector<join_result>>& join_tables, int index, int join_attrs_count);

    // // select operation for generic join
    // std::vector<join_result> select(std::vector<join_result>& table, int index, int join_value);

    // // Set-Intersect operation for generic join (essentially a join)
    // std::set<int> set_intersect(std::map<int, std::vector<join_result>>& join_tables, int index);

    // worst-case generic join
    std::vector<std::vector<int>> generic_join(std::map<int, LevelHashMap*>& join_hash_index, std::vector<int>& global_join_attrs, int index, int join_attrs_count);

    // select operation for generic join
    LevelHashMap* select(LevelHashMap* table_index, int index, int join_value);

    // Set-Intersect operation for generic join (essentially a join)
    std::set<int> set_intersect(std::map<int, LevelHashMap*>& join_hash_index, int index);

    // yannakakis join algorithm on tables
    std::vector<join_result> yannakakis_durable_join(std::map<int, YannakakisJoinTreeNode*>& tree, std::vector<int>& join_order, int root_id, 
                        join_result& record, std::map<int, std::vector<join_result>>& join_tables, int durability, double& semi_join_time, double& report_time);

    // simple line-3 join, R(A,B) - R(B,C) - R(C,D)
    std::vector<join_result> line_3_join(std::vector<join_result>& left_table, std::vector<join_result>& middle_table, 
                        std::vector<join_result>& right_table, std::map<int, std::vector<int>>& join_attrs, int durability);
    // simple line-3 join, R(A,B) - R(B,C) - R(C,D)
    int line_3_join_counting(std::vector<join_result>& left_table, std::vector<join_result>& middle_table, 
                        std::vector<join_result>& right_table, std::map<int, std::vector<int>>& join_attrs, int durability);
    // general line-k join
    std::vector<join_result> line_k_join(std::map<int, std::vector<join_result>>& join_tables, std::map<int, std::vector<int>>& join_attrs, int durability);

    int line_k_join_counting(std::map<int, std::vector<join_result>>& join_tables, std::map<int, std::vector<int>>& join_attrs, int durability);

    std::vector<join_result> interval_join(intervalVector& result, intervalTree* iTree, int durability);

    int interval_join_count(intervalVector& result, intervalTree* iTree);
    
    // de-duplicate answer set
    std::vector<join_result> deduplicate(std::vector<join_result>& answer);

    // remove invalid join result
    std::vector<join_result> remove_invalid(std::vector<join_result>& answer);

    void difference(std::vector<join_result>& a, std::vector<join_result>& b);

    // join tree construction
    std::map<int, JoinTreeNode*> create_join_tree(const std::string filename);

    // Yannakakis join tree construction
    std::map<int, YannakakisJoinTreeNode*> create_yannakakis_join_tree(const std::string filename);

    void initialize_tree(std::map<int, YannakakisJoinTreeNode*>& tree);

    // remove expired record from Yannakakis join tree
    void remove_expired_record(std::map<int, YannakakisJoinTreeNode*>& tree, join_result& record);

    // add active record into Yannakakis join tree
    void add_active_record(std::map<int, YannakakisJoinTreeNode*>& tree, join_result& record);

    // semi_join between two tables to remove dangling records
    void semi_join(YannakakisJoinTreeNode* base, YannakakisJoinTreeNode* filter);

    void select_up(YannakakisJoinTreeNode* node, const std::vector<int>& join_attrs, std::map<int, std::set<int>>& candidates, std::map<int, std::vector<join_result>>& join_tables);

    void select_down(YannakakisJoinTreeNode* node, const std::vector<int>& join_attrs, std::map<int, std::set<int>>& candidates, std::map<int, std::vector<join_result>>& join_tables);

    // detect procedure for non-hirarchical join queries
    // if the current record is still in the join table, then we should skip it
    // to avoid duplicate reports
    // bool not_reported(std::map<int, YannakakisJoinTreeNode*>& tree, join_result& record);

    // prepare join values for semi-joins in Yannakakis algorithm
    void prepare_join_values(YannakakisJoinTreeNode* node);

    // check whether the Yannakakis join tree is non-empty to join
    bool empty_join_tree(std::map<int, YannakakisJoinTreeNode*>& tree);

    void get_total_active_records(std::map<int, YannakakisJoinTreeNode*>& tree);

    // free memory
    void delete_tree(std::map<int, JoinTreeNode*>& tree, int root_id);

    // free memory
    void delete_tree(std::map<int, YannakakisJoinTreeNode*>& tree, int root_id);

    // print tree
    void tree_viewer(std::map<int, JoinTreeNode*>& tree, int root_id);

    // print tree
    void tree_viewer(std::map<int, YannakakisJoinTreeNode*>& tree, int root_id);

    // test tree content
    void print_tree(std::map<int, JoinTreeNode*>& tree, int root_id);

    // test tree content
    void print_tree(std::map<int, YannakakisJoinTreeNode*>& tree);

    void setup_table_index(std::vector<int>& join_order, std::map<int, std::vector<join_result>>& join_tables);

    void build_level_hash_table(LevelHashMap* node, const join_result& record, std::vector<int>& join_attrs, int index);

    void setup_hash_index(std::vector<int>& join_order, std::map<int, std::vector<int>>& join_attrs, std::map<int, std::vector<join_result>>& join_tables);

    // sort results by join attributes for sort-merge join
    void sort_by_join_attrs(std::vector<join_result>& v, std::vector<int>& join_attrs);

private:
    // test if a and b join
    bool is_equal_joined(join_result& a, join_result& b);

    // test if a and b join, given join attrs
    bool is_equal_joined(join_result& a, join_result& b, std::vector<int>& join_attrs);
    
    // test if a and b is durable join by durability
    bool is_durable(join_result& a, join_result& b, int durability);

    // test if a >= b
    bool greater(join_result& a, join_result& b, std::vector<int>& join_attrs);

    // return the intersection length of intervals from two records
    int intersection(join_result& a, join_result& b);

    // output (partial) durable join result
    void join_output(join_result& target, std::vector<join_result>& buffer, std::vector<int>& union_attrs,
                        std::vector<join_result>& answer,int durability);

    // output (partial) durable join result with Forward Scan (FS)
    void FS_join_output(std::vector<join_result>& left, std::vector<join_result>& right, std::vector<int>& union_attrs,
                        std::vector<join_result>& answer,int durability);
    
    void join_output(std::map<int, std::vector<int>>& candidates, std::vector<int>& join_order, std::map<int, std::vector<int>>& join_attrs,
                    std::map<int, std::vector<join_result>>& join_tables, std::vector<join_result>& answer);

    void join_output(std::map<int, std::vector<int>>& candidates, std::map<int, std::vector<join_result>>& join_tables, int table_id, std::vector<int>& temp_answer, std::vector<join_result>& answer);

    void enumerate_join_output(std::map<int, std::vector<int>>& candidates, int table_id);
    
    // Cartesian product output size
    int join_output_size(std::map<int, std::vector<int>>& candidates);
    
    
    // enumerate join results from join tree
    void enumerate(JoinTreeNode* root, join_result& record, std::map<int, std::vector<join_result>>& join_tables,
                    std::map<int, std::vector<int>>& candidates, int durability);
    
    // update join tree by inserting a new record
    void update(std::map<int, JoinTreeNode*>& tree, join_result& record);

    // delete the record from join tree
    void purge(std::map<int, JoinTreeNode*>& tree, join_result& record);

    // quick check whether the record is in the join set
    bool check(std::map<int, JoinTreeNode*>& tree, join_result& record, int durability);

    // get max value from children's min heap
    queue_tuple get_max_from_children(JoinTreeNode* n, std::vector<int>& key);

    void yannakakis_bottom_up_semi_join(YannakakisJoinTreeNode* root);

    void yannakakis_top_down_semi_join(YannakakisJoinTreeNode* root);

    std::vector<join_result> yannakakis_join_with_durability(std::map<int, YannakakisJoinTreeNode*>& tree, std::vector<int>& join_order, 
                join_result& record, std::map<int, std::vector<join_result>>& join_tables, int durability);
    
    // std::vector<join_result> yannakakis_self_join_with_durability(std::map<int, YannakakisJoinTreeNode*>& tree, std::vector<int>& join_order, 
    //         join_result& record, std::map<int, std::vector<join_result>>& join_tables, int durability);

    // number of total attributes from all tables
    int total_num_attrs;
    int enumeration_exit_flag;
    std::map<int, std::unordered_map<std::vector<int>, std::vector<int>, VectorHasher>> join_table_index;
    std::map<int, LevelHashMap*> join_hash_index;
};

#endif
