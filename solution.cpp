#include "solution.h"
#include <fstream>
#include <functional>
#include <algorithm>
#include <numeric>
#include <assert.h>
#include <chrono>
using namespace std::chrono;

void Solution::difference(std::vector<join_result>& a, std::vector<join_result>& b) {
    std::set<std::vector<int>> a_set;
    std::set<std::vector<int>> b_set;

    for (auto record : a) {
        std::sort(record.id.begin(), record.id.end());
        a_set.insert(record.id);
    }
    for (auto record : b) {
        std::sort(record.id.begin(), record.id.end());
        b_set.insert(record.id);
    }
    
    std::cout << "size difference: " << a_set.size() << '/' << b_set.size() << std::endl;

    // assert (a_set.size() <= b_set.size());

    std::set<std::vector<int>> diff;
    std::set_difference(b_set.begin(), b_set.end(), a_set.begin(), a_set.end(), std::inserter(diff, diff.end()));

    for (auto item : diff)
        print_vector(item);
}

std::vector<join_result> Solution::deduplicate(std::vector<join_result>& answer) {
    std::set<std::vector<int>> unique;
    std::vector<join_result> unique_answer;
    for (auto record : answer) {
        auto it = unique.insert(record.id);
        if (it.second) 
            unique_answer.push_back(record);
    }
    return unique_answer;
}

std::vector<join_result> Solution::remove_invalid(std::vector<join_result>& answer) {
    std::set<std::vector<int>> unique_path;
    std::vector<join_result> final_answer;
    std::cout << "previous answer size: " << answer.size() << std::endl;
    for (const auto& record : answer) {
        std::set<int> path(record.id.begin(), record.id.end());
        if (path.size() == record.id.size())
            if (unique_path.insert(record.id).second) 
                final_answer.emplace_back(record);
    }
    return final_answer;
}

bool Solution::is_equal_joined(join_result& a, join_result& b) {
    for (int i = 0; i < a.attrs.size(); ++i) {
        if (a.attrs[i] != b.attrs[i])
            return false;
    }
    return true;
}

bool Solution::is_equal_joined(join_result& a, join_result& b, std::vector<int>& join_attrs) {
    for (auto idx : join_attrs) {
        if (a.attrs[idx] != b.attrs[idx])
            return false;
    }
    return true;
}

bool Solution::is_durable(join_result& a, join_result& b, int durability) {
    int left = MAX(a.t_start, b.t_start);
    int right = MIN(a.t_end, b.t_end);

    if (right - left + 1 >= durability)
        return true;
    else
        return false;
} 

bool Solution::greater(join_result& a, join_result& b, std::vector<int>& join_attrs) {
    for (auto i : join_attrs) {
        if (a.attrs[i] == b.attrs[i])
            continue;
        else
            return a.attrs[i] >= b.attrs[i];
    }
    return true;
}

void Solution::sort_by_join_attrs(std::vector<join_result>& v, std::vector<int>& join_attrs) {
    std::sort(v.begin(), v.end(), [&](join_result const& r1, join_result const& r2) -> bool {
            for (auto idx : join_attrs) {
                if (r1.attrs[idx] == r2.attrs[idx])
                    continue;
                else
                    return r1.attrs[idx] < r2.attrs[idx];
            }
            if (r1.t_start == r2.t_start) 
                return r1.t_end < r2.t_end;
            else
                return r1.t_start < r2.t_start;
        }
    );
}


std::map<int, JoinTreeNode*> Solution::create_join_tree(const std::string filename) {
    std::map<int, JoinTreeNode*> tree;
    std::ifstream fin(filename);
	if (!fin)
		std::cout << "FILE ERROR" << std::endl;
    
    std::cout << "load join tree structure from " << filename << std::endl;

    std::string line;

    CSVRow row;
    int root_id = 0, node_id = 0, offspring = 0;
	while(fin >> row) {
        root_id = std::stoi(row[0]);
        offspring = std::stoi(row[1]);
        tree[root_id] = new JoinTreeNode(root_id);
        for (int i = 2; i < 2 + offspring; ++i) {
            node_id = std::stoi(row[i]);
            if (tree.find(node_id) == tree.end())
                tree[node_id] = new JoinTreeNode(node_id);
            tree[node_id]->parent = tree[root_id];
            tree[root_id]->children.push_back(tree[node_id]);
        }
        for (int i = 2 + offspring; i < row.size(); ++i)
            tree[root_id]->join_attrs.push_back(std::stoi(row[i]));
        tree[root_id]->attrs_num = tree[root_id]->join_attrs.size();
	}
	fin.close();
    return tree;
}

std::map<int, YannakakisJoinTreeNode*> Solution::create_yannakakis_join_tree(const std::string filename) {
    std::map<int, YannakakisJoinTreeNode*> tree;
    std::ifstream fin(filename);
	if (!fin)
		std::cout << "FILE ERROR" << std::endl;
    
    std::cout << "load join tree structure from " << filename << std::endl;

    std::string line;

    CSVRow row;
    int root_id = 0, node_id = 0, offspring = 0;
	while(fin >> row) {
        root_id = std::stoi(row[0]);
        offspring = std::stoi(row[1]);
        tree[root_id] = new YannakakisJoinTreeNode(root_id);
        for (int i = 2; i < 2 + offspring; ++i) {
            node_id = std::stoi(row[i]);
            if (tree.find(node_id) == tree.end())
                tree[node_id] = new YannakakisJoinTreeNode(node_id);
            tree[node_id]->parent = tree[root_id];
            tree[root_id]->children.push_back(tree[node_id]);
        }
        for (int i = 2 + offspring; i < row.size(); ++i)
            tree[root_id]->join_attrs.push_back(std::stoi(row[i]));
        tree[root_id]->attrs_num = tree[root_id]->join_attrs.size();
	}
	fin.close();
    return tree;
}

void Solution::delete_tree(std::map<int, JoinTreeNode*>& tree, int root_id) {
    for (int i=0; i<tree[root_id]->children.size(); ++i) {
        delete_tree(tree, tree[root_id]->children[i]->node_id);
    }
    delete tree[root_id];
    return;
}

void Solution::delete_tree(std::map<int, YannakakisJoinTreeNode*>& tree, int root_id) {
    for (int i=0; i<tree[root_id]->children.size(); ++i) {
        delete_tree(tree, tree[root_id]->children[i]->node_id);
    }
    delete tree[root_id];
    return;
}

void Solution::tree_viewer(std::map<int, JoinTreeNode*>& tree, int root_id) {
    
    // std::cout << root_id << " (" <<  tree[root_id]->attrs_num << ')' << std::endl;
    std::cout << root_id << " (" ;
    for (auto v : tree[root_id]->join_attrs)
        std::cout << v << ',';
    std::cout << ')' << std::endl;
    if (tree[root_id]->children.empty())
        return;
    for (int i=0; i<tree[root_id]->children.size(); ++i)
        tree_viewer(tree, tree[root_id]->children[i]->node_id);
}

void Solution::tree_viewer(std::map<int, YannakakisJoinTreeNode*>& tree, int root_id) {
    
    std::cout << root_id << " (" ;
    for (auto v : tree[root_id]->join_attrs)
        std::cout << v << ',';
    std::cout << ')' << std::endl;
    if (tree[root_id]->children.empty())
        return;
    for (int i=0; i<tree[root_id]->children.size(); ++i)
        tree_viewer(tree, tree[root_id]->children[i]->node_id);
}

void Solution::print_tree(std::map<int, YannakakisJoinTreeNode*>& tree) {
    for (auto& item : tree) {
        YannakakisJoinTreeNode* node = item.second;
        std::cout << "table " << node->node_id << " : " << std::endl;
        // for (auto& item : node->origin_table) {
        //     for (int idx : item.second)
        //         std::cout << idx << " ";
        // }
        // std::cout << std::endl;
        for (auto& item : node->non_dangling_join_values) {
            std::cout << "join key: ";
            print_vector(item);
        }
    }
}

void Solution::initialize_tree(std::map<int, YannakakisJoinTreeNode*>& tree) {
    // wipe out data
    for (auto& item : tree) {
        YannakakisJoinTreeNode* node = item.second;
        node->non_dangling_join_values.clear();
    }
}

void Solution::remove_expired_record(std::map<int, YannakakisJoinTreeNode*>& tree, join_result& record) {
    int node_id = record.table_id;
    int idx = record.idx;
    std::vector<int> key = record.attrs;
    // remove the record
    tree[node_id]->origin_table[key].erase(idx);
    if (tree[node_id]->origin_table[key].empty()) {
        tree[node_id]->origin_table.erase(key);
        // remove the projected key from its parent and children
        // clean upward
        if (tree[node_id]->parent != nullptr) {
            std::vector<int> common_attrs = get_intersection(tree[node_id]->join_attrs, tree[node_id]->parent->join_attrs);
            std::vector<int> new_key = std::vector<int>(this->total_num_attrs, UNDEF);
            for (int idx : common_attrs)
                new_key[idx] = key[idx];
            tree[node_id]->projection_up[new_key].erase(key);
            if (tree[node_id]->projection_up[new_key].empty())
                tree[node_id]->projection_up.erase(new_key);
        }
        // clean downward
        // for (auto child : tree[node_id]->children) {
        for (int i=0; i<tree[node_id]->children.size(); ++i) {
            std::vector<int> common_attrs = get_intersection(tree[node_id]->join_attrs, tree[node_id]->children[i]->join_attrs);
            std::vector<int> new_key = std::vector<int>(this->total_num_attrs, UNDEF);
            for (int idx : common_attrs)
                new_key[idx] = key[idx];
            tree[node_id]->children[i]->projection_down[new_key].erase(key);
            if (tree[node_id]->children[i]->projection_down[new_key].empty())
                tree[node_id]->children[i]->projection_down.erase(new_key);
        }
    }
}

void Solution::add_active_record(std::map<int, YannakakisJoinTreeNode*>& tree, join_result& record) {
    int node_id = record.table_id;
    std::vector<int> key = record.attrs;
    // insert the record
    tree[node_id]->origin_table[key].insert(record.idx);
    // join attrs project down
    for (auto child : tree[node_id]->children) {
        std::vector<int> common_attrs = get_intersection(tree[node_id]->join_attrs, child->join_attrs);
        std::vector<int> new_key = std::vector<int>(this->total_num_attrs, UNDEF);
        for (int idx : common_attrs)
            new_key[idx] = key[idx];
        child->projection_down[new_key].insert(key);
    }
    // join attrs project up
    if (tree[node_id]->parent != nullptr) {
        // std::cout << "node id: " << node_id << " parent id: " << tree[node_id]->parent->node_id << std::endl;
        std::vector<int> common_attrs = get_intersection(tree[node_id]->join_attrs, tree[node_id]->parent->join_attrs);
        std::vector<int> new_key = std::vector<int>(this->total_num_attrs, UNDEF);
        for (int idx : common_attrs)
            new_key[idx] = key[idx];
        tree[node_id]->projection_up[new_key].insert(key);
    }
}

void Solution::prepare_join_values(YannakakisJoinTreeNode* node) {
    if (!node->non_dangling_join_values.empty())
        return;
    for (auto& item : node->origin_table)
        node->non_dangling_join_values.insert(item.first);
}

bool Solution::empty_join_tree(std::map<int, YannakakisJoinTreeNode*>& tree) {
    for (auto& item : tree) {
        YannakakisJoinTreeNode* node = item.second;
        if (node->non_dangling_join_values.empty())
            return true;
    }
    return false;
}

void Solution::get_total_active_records(std::map<int, YannakakisJoinTreeNode*>& tree) {
    int total_records = 0;
    int total_keys = 0;
    for (auto& item : tree) {
        YannakakisJoinTreeNode* node = item.second;
        total_keys += node->origin_table.size();
        for (auto& entry : node->origin_table) {
            total_records += entry.second.size();
        }
    }
    std::cout << "# of keys: " << total_keys << std::endl;
    std::cout << "# of active records: " << total_records << std::endl;
}

void Solution::print_tree(std::map<int, JoinTreeNode*>& tree, int root_id) {
    std::cout << root_id << " (" ;
    for (auto v : tree[root_id]->join_attrs)
        std::cout << v << ',';
    std::cout << ')'; 
    for (const auto& item : tree[root_id]->max_t) {
        print_vector(item.first);
        std::cout << "->" << std::get<0>(item.second) << std::endl;
    }
    std::cout << std::endl;
    if (tree[root_id]->children.empty())
        return;
    for (int i=0; i<tree[root_id]->children.size(); ++i)
        print_tree(tree, tree[root_id]->children[i]->node_id);
}

void Solution::join_output(join_result& target, std::vector<join_result>& buffer, std::vector<int>& union_attrs,
                        std::vector<join_result>& answer, int durability) {
    
    for (int i=0; i<buffer.size(); ++i) {
        join_result record;
        record.id = target.id;
        for (auto& v : buffer[i].id)
            record.id.push_back(v);
        record.attrs = std::vector<int>(this->total_num_attrs, UNDEF);
        for (auto idx : union_attrs) {
            record.attrs[idx] = MAX(record.attrs[idx], target.attrs[idx]);
            record.attrs[idx] = MAX(record.attrs[idx], buffer[i].attrs[idx]);
        }
        record.t_start = MAX(target.t_start, buffer[i].t_start);
        record.t_end = MIN(target.t_end, buffer[i].t_end);
        record.attr_id = union_attrs;
        if (record.t_end - record.t_start + 1 >= durability)
            answer.push_back(record);
    }
}

void Solution::enumerate_join_output(std::map<int, std::vector<int>>& candidates, int table_id) {
    if (table_id >= candidates.size()) {
        return;
    }
    for (int id : candidates[table_id]) {
        enumerate_join_output(candidates, table_id+1);
    }
    return;
}

void Solution::join_output(std::map<int, std::vector<int>>& candidates, std::map<int, std::vector<join_result>>& join_tables, 
                           int table_id, std::vector<int>& temp_answer, std::vector<join_result>& answer) {
    if (temp_answer.size() == candidates.size()) {
        join_result result;
        result.id = temp_answer;
        answer.emplace_back(result);
        return;
    }

    for (int id : candidates[table_id]) {
        temp_answer.push_back(join_tables[table_id][id].id[0]);
        join_output(candidates, join_tables, table_id+1, temp_answer, answer);
        temp_answer.pop_back();
    }
}

void Solution::join_output(std::map<int, std::vector<int>>& candidates, std::vector<int>& join_order, std::map<int, std::vector<int>>& join_attrs, 
            std::map<int, std::vector<join_result>>& join_tables, std::vector<join_result>& answer) {
    int table_1 = join_order[0], table_2 = join_order[1];
    std::vector<int> common_attrs, union_attrs;
    common_attrs = get_intersection(join_attrs[table_1], join_attrs[table_2]);
    union_attrs = get_union(join_attrs[table_1], join_attrs[table_2]);

    std::vector<join_result> left, right;
    for (auto id : candidates[table_1])
        left.emplace_back(join_tables[table_1][id]);
    for (auto id : candidates[table_2])
        right.emplace_back(join_tables[table_2][id]);
    std::vector<join_result> partial = naive_durable_join(left, right, common_attrs, union_attrs, 0);

    for (int i=2; i<join_order.size(); ++i) {
        table_2 = join_order[i];
        common_attrs = get_intersection(union_attrs, join_attrs[table_2]);
        union_attrs = get_union(union_attrs, join_attrs[table_2]);
        
        right.clear();
        for (auto id : candidates[table_2])
            right.emplace_back(join_tables[table_2][id]);
        partial = naive_durable_join(partial, right, common_attrs, union_attrs, 0);
    }

    for (auto item : partial)
        answer.emplace_back(item);
}

int Solution::join_output_size(std::map<int, std::vector<int>>& candidates) {
    int product = 1;
    for (const auto& item : candidates) {
        product *= item.second.size();
    }
    return product;
}

int Solution::intersection(join_result& a, join_result& b) {
    int start = MAX(a.t_start, b.t_start);
    int end = MIN(a.t_end, b.t_end);
    return end - start + 1;
}

void Solution::FS_join_output(std::vector<join_result>& left, std::vector<join_result>& right, std::vector<int>& union_attrs,
                        std::vector<join_result>& answer,int durability) {
    // std::cout << "LFET:" << std::endl;
    // for (auto& item : left) 
    //     item.print();
    // std::cout << "RIGHT:" << std::endl;
    // for (auto& item : right) 
    //     item.print();
    int left_ptr = 0, right_ptr = 0;
    int left_start = -1, left_end = -1;
    int right_start = -1, right_end = -1;

    // records in left and right are already sorted based on their interval startpoints
    while (left_ptr < left.size() && right_ptr < right.size()) {
        left_start = left[left_ptr].t_start;
        right_start = right[right_ptr].t_start;
        // process interval join based on left
        if (left_start <= right_start) {
            left_end = left[left_ptr].t_end;
            for (int i = right_ptr; i < right.size(); ++i) {
                if (right[i].t_start <= left_end) {
                    if (intersection(left[left_ptr], right[i]) >= durability) {
                        join_result record;
                        for (auto& v : left[left_ptr].id)
                            record.id.push_back(v);
                        for (auto& v : right[i].id)
                            record.id.push_back(v);
                        record.attrs = std::vector<int>(this->total_num_attrs, UNDEF);
                        for (auto idx : union_attrs) {
                            record.attrs[idx] = MAX(record.attrs[idx], left[left_ptr].attrs[idx]);
                            record.attrs[idx] = MAX(left[left_ptr].attrs[idx], right[i].attrs[idx]);
                        }
                        record.attr_id = union_attrs;
                        record.t_start = MAX(left[left_ptr].t_start, right[i].t_start);
                        record.t_end = MIN(left[left_ptr].t_end, right[i].t_end);
                        answer.push_back(record);
                        // std::cout << "ANSWER:" << std::endl;
                        // record.print();
                    }
                }
                else {
                    break;
                }
            }
            left_ptr++;
        }
        //  process interval join based on right
        else {
            right_end = right[right_ptr].t_end;
            for (int i = left_ptr; i < left.size(); ++i) {
                if (left[i].t_start <= right_end) {
                    if (intersection(left[i], right[right_ptr]) >= durability) {
                        join_result record;
                        for (auto& v : left[i].id)
                            record.id.push_back(v);
                        for (auto& v : right[right_ptr].id)
                            record.id.push_back(v);
                        record.attrs = std::vector<int>(this->total_num_attrs, UNDEF);
                        for (auto idx : union_attrs) {
                            record.attrs[idx] = MAX(record.attrs[idx], left[i].attrs[idx]);
                            record.attrs[idx] = MAX(left[i].attrs[idx], right[right_ptr].attrs[idx]);
                        }
                        record.attr_id = union_attrs;
                        record.t_start = MAX(left[i].t_start, right[right_ptr].t_start);
                        record.t_end = MIN(left[i].t_end, right[right_ptr].t_end);
                        answer.push_back(record);
                        // std::cout << "ANSWER:" << std::endl;
                        // record.print();
                    }
                }
                else {
                    break;
                }
            }
            right_ptr++;
        }
    }
}

bool Solution::check(std::map<int, JoinTreeNode*>& tree, join_result& record, int durability) {
    int node_id = record.table_id;
    JoinTreeNode *n = tree[node_id];
    std::vector<int> key;

    while (n != nullptr && n->parent != nullptr) {
        key = std::vector<int>(this->total_num_attrs, UNDEF);
        for (auto idx : n->parent->join_attrs)
            key[idx] = record.attrs[idx];
        if (n->parent->max_t.find(key) == n->parent->max_t.end())
            return false;
        if (std::get<0>(n->parent->max_t[key]) > record.t_end + 1 - durability)
            return false;
        n = n->parent;
    }
    return true;
}

std::vector<join_result> Solution::naive_durable_join(std::vector<join_result>& left_table, std::vector<join_result>& right_table, 
                                            std::vector<int>& join_attrs, std::vector<int>& union_attrs, int durability) {
    std::vector<join_result> answer;
    for (int i=0; i<left_table.size(); ++i) {
        for (int j=0; j<right_table.size(); ++j) {

            if (is_equal_joined(left_table[i], right_table[j], join_attrs) 
                && is_durable(left_table[i], right_table[j], durability)) {
                    join_result item;
                    for (auto& v : left_table[i].id)
                        item.id.push_back(v);
                    for (auto& v : right_table[j].id)
                        item.id.push_back(v);
                   
                    item.attrs = std::vector<int>(this->total_num_attrs, UNDEF);
                    for (auto idx : union_attrs) {
                        item.attrs[idx] = MAX(item.attrs[idx], left_table[i].attrs[idx]);
                        item.attrs[idx] = MAX(item.attrs[idx], right_table[j].attrs[idx]);
                    }
                    item.t_start = MAX(left_table[i].t_start, right_table[j].t_start);
                    item.t_end = MIN(left_table[i].t_end, right_table[j].t_end);
                    item.attr_id = union_attrs;
                    answer.push_back(item);
                    //std::cout << i << '/' << left_table.size() << ' ' << j << '/' << right_table.size() << std::endl;
            }
            // left_table[i].print();
            // right_table[j+1].print();
        }
    }
    return answer;
}

std::vector<join_result> Solution::pairwise_durable_join(std::vector<join_result>& left_table, std::vector<join_result>& right_table, 
                                            std::vector<int>& join_attrs, std::vector<int>& union_attrs, int durability) {
    std::vector<join_result> answer;
    std::vector<join_result> active_records;

    int left_ptr = 0, right_ptr = 0;
    while (left_ptr < left_table.size() && right_ptr < right_table.size()) {
        if (is_equal_joined(left_table[left_ptr], right_table[right_ptr], join_attrs)) {
            active_records.push_back(right_table[right_ptr]);
            right_ptr++;
        }
        else {
            // clean up active_records and output join result
            if (!active_records.empty()) {
                join_output(left_table[left_ptr], active_records, union_attrs, answer, durability);
                int advance = 1;
                for (; left_ptr + advance < left_table.size(); ++advance) {
                    if (is_equal_joined(left_table[left_ptr], left_table[left_ptr + advance], join_attrs)) {
                        join_output(left_table[left_ptr + advance], active_records, union_attrs, answer, durability);
                    }
                    else
                        break;
                }
                left_ptr += advance;
                active_records.clear();
                if (left_ptr >= left_table.size())
                    break;
            }
            // navigate for next round of join
            else {
                // left greater than right
                if (greater(left_table[left_ptr], right_table[right_ptr], join_attrs)) {
                    right_ptr++;
                }
                // left less than right
                else {
                    left_ptr++;
                }
            }
        }
    }

    // handle leftover
    if (!active_records.empty()) {
        if (left_ptr < left_table.size()) {
            for (int advance=0; left_ptr + advance < left_table.size(); ++advance) {
                if (is_equal_joined(left_table[left_ptr], left_table[left_ptr + advance], join_attrs)) {
                    join_output(left_table[left_ptr + advance], active_records, union_attrs, answer, durability);
                }
            }
            active_records.clear();
        }
    }

    return answer;
}

std::vector<join_result> Solution::pairwise_durable_temporal_join(std::vector<join_result>& left_table, std::vector<join_result>& right_table, std::vector<int>& join_attrs, std::vector<int>& union_attrs, int durability) {
    clock_t ts, te;
    clock_t index_time = 0, query_time = 0;
    std::vector<join_result> answer;
    // build a set of interval trees for left table
    std::map<std::vector<int>, intervalVector> left_table_intervals;
    std::vector<int> key;

    for (int i = 0; i < left_table.size(); ++i) {
        key = std::vector<int>(this->total_num_attrs, UNDEF);
        for (auto idx : join_attrs)
            key[idx] = left_table[i].attrs[idx];
        left_table_intervals[key].push_back(interval(left_table[i].t_start, left_table[i].t_end, i));
    }
    ts = clock();
    std::map<std::vector<int>, intervalTree*> left_forest;
    for (auto& item : left_table_intervals)
        left_forest[item.first] = new intervalTree(std::move(item.second), 16, 1);
    te = clock();
    index_time += te - ts;

    // build a set of interval tress for right table
    std::map<std::vector<int>, intervalVector> right_table_intervals;
    for (int i = 0; i < right_table.size(); ++i) {
        key = std::vector<int>(this->total_num_attrs, UNDEF);
        for (auto idx : join_attrs)
            key[idx] = right_table[i].attrs[idx];
        right_table_intervals[key].push_back(interval(right_table[i].t_start, right_table[i].t_end, i));
    }
    ts = clock();
    std::map<std::vector<int>, intervalTree*> right_forest;
    for (auto& item : right_table_intervals) 
        right_forest[item.first] = new intervalTree(std::move(item.second), 16, 1);
    te = clock();
    index_time += te - ts;

    // tempoal join
    auto left_it = left_forest.begin();
    auto right_it = right_forest.begin();
    std::vector<int> left_key, right_key;
    while (left_it != left_forest.end() && right_it != right_forest.end()) {
        left_key = left_it->first;
        right_key = right_it->first;
        int direction = compare(left_key, right_key, join_attrs);
        if (direction == 0) {
            // do interval tree search
            std::vector<join_result> partial_ans;
            if (left_table_intervals[left_key].size() <= right_table_intervals[right_key].size()) {
                partial_ans = interval_join(left_table_intervals[left_key], right_forest[right_key], durability);
            }
            else {
                partial_ans = interval_join(right_table_intervals[right_key], left_forest[left_key], durability);
            }
            for (auto& item : partial_ans) {
                // assert (item.id.size() == 2);
                int left_idx = left_table_intervals[left_key].size() <= right_table_intervals[right_key].size() ? item.id[0] : item.id[1];
                int right_idx = left_table_intervals[left_key].size() <= right_table_intervals[right_key].size() ? item.id[1] : item.id[0];
                item.attrs = std::vector<int>(this->total_num_attrs, UNDEF);
                for (int idx : union_attrs) {
                    //std::cout << left_table[item.id[0]].attrs.size() << ' ' << right_table[item.id[1]].attrs.size() << std::endl;
                    item.attrs[idx] = MAX(item.attrs[idx], left_table[left_idx].attrs[idx]);
                    item.attrs[idx] = MAX(item.attrs[idx], right_table[right_idx].attrs[idx]);
                }
                std::vector<int> ids;
                for (int record_id : left_table[left_idx].id)
                    ids.push_back(record_id);
                for (int record_id : right_table[right_idx].id)
                    ids.push_back(record_id);
                item.id = ids;
                item.attr_id = union_attrs;
                // print_vector(item.id);
                // print_vector(union_attrs);
                answer.emplace_back(item);
            }
            //std::cout << answer.size() << std::endl;
            right_it++;
            left_it++;
        }
        else if (direction == -1) {
            right_it++;
        }
        else {
            left_it++;
        }
    }
    return answer;
}

std::vector<join_result> Solution::pairwise_forward_scan_temporal_join(std::vector<join_result>& left_table, std::vector<join_result>& right_table, 
                                            std::vector<int>& join_attrs, std::vector<int>& union_attrs, int durability) {
    std::vector<join_result> answer;
    std::vector<join_result> left_records;
    std::vector<join_result> right_records;

    int left_ptr = 0, right_ptr = 0;
    while (left_ptr < left_table.size() && right_ptr < right_table.size()) {
        if (is_equal_joined(left_table[left_ptr], right_table[right_ptr], join_attrs)) {
            right_records.push_back(right_table[right_ptr]);
            right_ptr++;
        }
        else {
            // clean up active_records and output join result
            if (!right_records.empty()) {
                left_records.push_back(left_table[left_ptr]);
                int advance = 1;
                for (; left_ptr + advance < left_table.size(); ++advance) {
                    if (is_equal_joined(left_table[left_ptr], left_table[left_ptr + advance], join_attrs)) {
                        left_records.push_back(left_table[left_ptr + advance]);
                    }
                    else {
                        break;
                    }
                }
                left_ptr += advance;
                // Join Output : Forward scan on left & right records
                FS_join_output(left_records, right_records, union_attrs, answer, durability);
                left_records.clear();
                right_records.clear();
                if (left_ptr >= left_table.size())
                    break;
            }
            // navigate for next round of join
            else {
                // left greater than right
                if (greater(left_table[left_ptr], right_table[right_ptr], join_attrs)) {
                    right_ptr++;
                }
                // left less than right
                else {
                    left_ptr++;
                }
            }
        }
    }

    // handle leftover
    if (!right_records.empty()) {
        if (left_ptr < left_table.size()) {
            for (int advance=0; left_ptr + advance < left_table.size(); ++advance) {
                if (is_equal_joined(left_table[left_ptr], left_table[left_ptr + advance], join_attrs)) {
                        left_records.push_back(left_table[left_ptr + advance]);
                }
                else {
                    break;
                }
            }
            // Join Output : Forward scan on left & right records
            FS_join_output(left_records, right_records, union_attrs, answer, durability);
            right_records.clear();
            left_records.clear();
        }
    }

    return answer;
}


std::vector<join_result> Solution::multiway_durable_join_baseline(std::map<int, std::vector<join_result>>& join_tables, 
                            std::vector<int>& join_order, std::map<int, std::vector<int>>& join_attrs, int durability, int flag, bool verbose) {
    clock_t ts = clock();
    // early termination if one of the table is empty
    for (int id : join_order) {
        if (join_tables[id].empty())
            return std::vector<join_result>();
    }
    // A baseline solution by pairwise durable join 
    std::vector<int> common_attrs, union_attrs;
    common_attrs = get_intersection(join_attrs[join_order[0]], join_attrs[join_order[1]]);
    union_attrs = get_union(join_attrs[join_order[0]], join_attrs[join_order[1]]);
    if (verbose) {
        std::cout << "===round 1===" << std::endl;
        std::cout << "join attrs:";
        print_vector(common_attrs);
        std::cout << "union attrs:";
        print_vector(union_attrs);
    }
    // initial pair join
    std::vector<join_result> partial_ans;
    if (flag == 0) {
        if (verbose)
            std::cout << "*************using sort-merge join*************" << std::endl;
        // sort two tables by common join attrs
        sort_by_join_attrs(join_tables[join_order[0]], common_attrs);
        sort_by_join_attrs(join_tables[join_order[1]], common_attrs);
        partial_ans = pairwise_durable_join(join_tables[join_order[0]], join_tables[join_order[1]], common_attrs, union_attrs, durability);
        // partial_ans = pairwise_durable_join(join_tables[join_order[0]], join_tables[join_order[1]], common_attrs, union_attrs, 0);
    }
    else if (flag == 1) {
        if (verbose)
            std::cout << "*************using temporal join*************" << std::endl;
        partial_ans = pairwise_durable_temporal_join(join_tables[join_order[0]], join_tables[join_order[1]], common_attrs, union_attrs, durability);
    }
    else if (flag == -1) {
        if (verbose)
            std::cout << "*************using Forward Scan join*************" << std::endl;
        // sort two tables by common join attrs
        sort_by_join_attrs(join_tables[join_order[0]], common_attrs);
        sort_by_join_attrs(join_tables[join_order[1]], common_attrs);
        partial_ans = pairwise_forward_scan_temporal_join(join_tables[join_order[0]], join_tables[join_order[1]], common_attrs, union_attrs, durability);
    }
    else {
        if (verbose)
            std::cout << "*************using loop-based join*************" << std::endl;
        partial_ans = naive_durable_join(join_tables[join_order[0]], join_tables[join_order[1]], common_attrs, union_attrs, durability);
        // partial_ans = naive_durable_join(join_tables[join_order[0]], join_tables[join_order[1]], common_attrs, union_attrs, 0);
    }
    if (verbose)
        std::cout << "intermediate result size: " << partial_ans.size() << std::endl;
    // pairwise join for rest tables one-by-one
    // (need projection on Intermediate join results if necessary)
    for (int i=2; i<join_order.size(); ++i) {
       
        if (partial_ans.empty())
            return std::vector<join_result>();
        
        common_attrs = get_intersection(partial_ans[0].attr_id, join_attrs[join_order[i]]);
        union_attrs = get_union(partial_ans[0].attr_id, join_attrs[join_order[i]]);
        if (verbose) {
            std::cout << "===round " << i << "===" << std::endl;
            std::cout << "join attrs:";
            print_vector(common_attrs);
            std::cout << "union attrs:";
            print_vector(union_attrs);
        }
        
        if (flag == 0) {
            sort_by_join_attrs(partial_ans, common_attrs);
            sort_by_join_attrs(join_tables[join_order[i]], common_attrs);
            partial_ans = pairwise_durable_join(partial_ans, join_tables[join_order[i]], common_attrs, union_attrs, durability);
            // partial_ans = pairwise_durable_join(partial_ans, join_tables[join_order[i]], common_attrs, union_attrs, 0);
        }
        else if (flag == 1) {
            partial_ans = pairwise_durable_temporal_join(partial_ans, join_tables[join_order[i]], common_attrs, union_attrs, durability);
        }
        else if (flag == -1) {
            sort_by_join_attrs(partial_ans, common_attrs);
            sort_by_join_attrs(join_tables[join_order[i]], common_attrs);
            partial_ans = pairwise_forward_scan_temporal_join(partial_ans, join_tables[join_order[i]], common_attrs, union_attrs, durability);
        }
        else {
            partial_ans = naive_durable_join(partial_ans, join_tables[join_order[i]], common_attrs, union_attrs, durability);
        }
        if (verbose)
            std::cout << "intermediate result size: " << partial_ans.size() << std::endl;
    }

    std::vector<join_result> ans;

    for (auto item : partial_ans) {
        if (item.t_end - item.t_start + 1 >= durability)
            ans.push_back(item);
    }
    if (verbose)
        std::cout << "intermediate result size: " << ans.size() << std::endl;
    // return results
    clock_t te = clock();
    if (verbose)
        std::cout << "overall time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << std::endl;
    return ans;
}

queue_tuple Solution::get_max_from_children(JoinTreeNode* n, std::vector<int>& key) {
    int max_value = 0, max_tid = 0, max_rid = 0;
    int value, tid, rid;
    for (int i=0; i < n->children.size(); ++i) {
        // if any of its children has an empty min_heap
        // then it shouldn't contribute to its parent, set as infinity
        if (n->children[i]->min_t[key].empty())
            return std::make_tuple(INT_MAX, -1, -1);
        // retrieve min
        auto it = n->children[i]->min_t[key].begin();
        value = std::get<0>(*it);
        tid = std::get<1>(*it);
        rid = std::get<2>(*it);
        if (value > max_value) 
            max_value = value, max_tid = tid, max_rid = rid;
    }
    queue_tuple result(max_value, max_tid, max_rid);
    return result;
}

void Solution::enumerate(JoinTreeNode* root, join_result& record, std::map<int, std::vector<join_result>>& join_tables,
                        std::map<int, std::vector<int>>& candidates, int durability) {
    std::vector<int> common_attrs = get_intersection(root->join_attrs, record.attr_id);
    std::vector<int> key = std::vector<int>(this->total_num_attrs, UNDEF);
    for (auto idx : common_attrs)
            key[idx] = record.attrs[idx];

    // if the current node is leaf node, output results
    if (root->children.size() == 0) {
        
        int table_id = root->node_id;

        if (table_id == record.table_id) {
            candidates[table_id].push_back(record.idx);
        }
        else {
            // std::cout << root->node_id << ':';
            // print_vector(record.attrs);
            // std::cout << root->node_id << ':' << root->base_table.size() << std::endl;
            for (int rid : root->base_table[key]) {
                if (is_equal_joined(join_tables[table_id][rid], record, common_attrs) 
                        && is_durable(join_tables[table_id][rid], record, durability)) {
                    candidates[table_id].push_back(join_tables[table_id][rid].idx);
                }
            }
            return;
        }
    }
    else {
        // if the common attrs are subset of the record's attrs
        // recursively go to its children
        if (is_subset(root->join_attrs, record.attr_id)) {
            if (std::get<0>(root->max_t[key]) <= record.t_end + 1 - durability) {
                for (int i=0; i < root->children.size(); ++i) {
                    enumerate(root->children[i], record, join_tables, candidates, durability);
                }
            }
            else {
                return;
            }
        }
        // else need to get a union of records that join on projection of the common attrs
        else {
            // locate the position that the superkey matches with the key (which should be a prefix)
            auto lower_pos = root->max_t.upper_bound(key);
            int greater;
            while (lower_pos != root->max_t.end()) {
                greater = compare(lower_pos->first, key, common_attrs);
                if (greater < 0)
                    break;
                else {
                    // assert (greater == 0);
                    if (std::get<0>(lower_pos->second) <= record.t_end + 1 - durability) {
                        for (int i=0; i < root->children.size(); ++i) {
                            join_result temp = record;
                            temp.attrs = lower_pos->first;
                            temp.attr_id = root->join_attrs;
                            enumerate(root->children[i], temp, join_tables, candidates, durability);
                        }
                    }
                    lower_pos++;
                }
            }
            // for (auto item : root->max_t) {
            //     int direction = compare(item.first, key, common_attrs);
            //     // super key != key, continue to scan
            //     if (direction == 1)
            //         continue;
            //     else if (direction == -1)
            //         continue;
            //     // equal! recursively enumerate
            //     else {
            //         assert (direction == 0);
            //         if (std::get<0>(item.second) <= record.t_end - durability) {
                        
            //             for (int i=0; i < root->children.size(); ++i) {
            //                 join_result temp = record;
            //                 temp.attrs = item.first;
            //                 temp.attr_id = root->join_attrs;
                            
            //                 enumerate(root->children[i], temp, join_tables, candidates, durability);
            //             }
            //         }
            //     }
            // }
        }
    }
}
// TODO: Optimize!!
void Solution::purge(std::map<int, JoinTreeNode*>& tree, join_result& record) {
    int table_id = record.table_id;
    // int record_id = record.id[0];
    int record_id = record.idx;
    int t = record.t_start;
    queue_tuple prev_min, prev_max;

    queue_tuple tuple_key(t, table_id, record_id);
    std::vector<int> key, projected_key;
    
    // assert (tree[table_id]->base_table[record.attrs].find(record_id) != tree[table_id]->base_table[record.attrs].end());
    // tree[table_id]->base_table[record.attrs].erase(record_id);
    projected_key = std::vector<int>(this->total_num_attrs, UNDEF);
    for (auto idx : tree[table_id]->parent->join_attrs) {
        projected_key[idx] = record.attrs[idx];
    }
    tree[table_id]->base_table[projected_key].erase(record_id);

    // recursively update the join tree
    JoinTreeNode* n = tree[table_id];
    JoinTreeNode* temp = nullptr;
    while (n != nullptr && n->parent != nullptr) {
        key = std::vector<int>(this->total_num_attrs, UNDEF);
        projected_key = std::vector<int>(this->total_num_attrs, UNDEF);

        for (auto idx : n->parent->join_attrs)
            key[idx] = record.attrs[idx];

        // update min_heap
        prev_min = *(n->min_t[key].begin());
        n->min_t[key].erase(tuple_key);
        // // min value doesn't change, no need to propagate the changes.
        // if (prev_min == *(n->min_t[key].begin()))
        //     return;
        // // update max
        // if (n->parent->max_t.find(key) == n->parent->max_t.end())
        //     return;
        prev_max = n->parent->max_t[key];
        n->parent->max_t[key] = get_max_from_children(n->parent, key);
        // // max value doesn't change, no need to propagate the changes.
        // if (prev_max == n->parent->max_t[key])
        //     return;
        // temp = n->parent;
        n = n->parent;
        if (n == nullptr || n->parent == nullptr)
            return;
        
        for (auto idx : n->parent->join_attrs)
            projected_key[idx] = record.attrs[idx];
        // update its min_heap accordingly
        n->min_t[projected_key].insert(n->max_t[key]);
    }

}

void Solution::update(std::map<int, JoinTreeNode*>& tree, join_result& record) {
    
    queue_tuple prev_min, prev_max;
    // update base table
    int table_id = record.table_id;
    queue_tuple qt(record.t_start, table_id, record.idx);
    JoinTreeNode* n = tree[table_id];
    
    // get join attribute as key for this record
    std::vector<int> key = record.attrs;

    // get projected key from its parent
    // assert (n->parent != nullptr);
    std::vector<int> projected_key = std::vector<int>(this->total_num_attrs, UNDEF);
    for (auto idx : n->parent->join_attrs) {
        projected_key[idx] = record.attrs[idx];
    }
    
    prev_min = *(tree[table_id]->min_t[projected_key].begin());
    tree[table_id]->min_t[projected_key].insert(qt);
    // tree[table_id]->base_table[key].insert(record.idx);
    tree[table_id]->base_table[projected_key].insert(record.idx);

    // min doesn't change, no need to propagate the changes
    if (prev_min == *(tree[table_id]->min_t[projected_key].begin()))
        return;

    // update internal nodes in join tree by min/max rules
    while (n != nullptr && n->parent != nullptr) {
        key = projected_key;

        // if the current key does not exist in all its children, stop update
        for (int i = 0; i < n->parent->children.size(); ++i) {
            if (n->parent->children[i]->min_t[key].empty())
                return;
        }
        prev_max = n->parent->max_t[key];
        // else, update the annotation by get max from offspring's min heap
        n->parent->max_t[key] = get_max_from_children(n->parent, key);
        // max doesn't change, no need to propagate the changes
        if (prev_max == n->parent->max_t[key])
            return;

        // recursively go up the join tree and update
        n = n->parent;

        if (n == nullptr || n->parent == nullptr)
            return;

        projected_key = std::vector<int>(this->total_num_attrs, UNDEF);
        
        for (auto idx : n->parent->join_attrs)
            projected_key[idx] = record.attrs[idx];
        prev_min = *(n->min_t[projected_key].begin());
        n->min_t[projected_key].insert(n->max_t[key]);
        if (prev_min == *(n->min_t[projected_key].begin()))
            return;
    }
    return;
}

// std::vector<join_result> Solution::non_hierarchy_durable_join(const std::string filename, std::map<int, std::vector<join_result>>& join_tables,
//                         std::vector<int>& join_order, std::map<int, std::vector<int>>& join_attrs, int root_id, int durability) {
//     std::vector<join_result> answer;

//     clock_t ts, te;

//     // build join tree
//     std::map<int, YannakakisJoinTreeNode*> tree = create_yannakakis_join_tree(filename);
//     YannakakisJoinTreeNode* root = tree[root_id];

//     tree_viewer(tree, root_id);

//     ts = clock();
//     // sort left endpoints of intervals from all join tables
//     std::vector<join_result> data;
//     // sort right endpoints of intervals from all join tables
//     std::vector<join_result> data_by_right;
//     // get all endpoints of intervals
//     std::set<int> timestamps;
//     for (auto& item : join_tables) {
//         for (auto& record : item.second) {
//             data.emplace_back(record);
//             data_by_right.emplace_back(record);
//             timestamps.insert(record.t_start);
//             timestamps.insert(record.t_end);
//         }
//     }
//     std::cout << "total record: " << data.size() << "/" << data_by_right.size() << std::endl;

//     clock_t sort_ts = clock();
//     std::sort(data.begin(), data.end(), 
//             [](join_result const& a, join_result const& b)->bool {
//                 return a.t_start < b.t_start;
//             });
//     std::sort(data_by_right.begin(), data_by_right.end(), 
//             [](join_result const& a, join_result const& b)->bool {
//                 if (a.t_end == b.t_end)
//                     return a.idx < b.idx;
//                 return a.t_end < b.t_end;
//             });
//     clock_t sort_te = clock();
//     std::cout << "sorting time: " << (double) (sort_te - sort_ts) / CLOCKS_PER_SEC << std::endl;

//     clock_t report_ts, report_te, report_total=0;
//     clock_t add_ts, add_te, add_total=0;
//     clock_t delete_ts, delete_te, delete_total=0;
//     double semi_join_time_total = 0, report_time_total = 0;
//     // sweep line on time domain to prepare active records for Yannakakis algorithm
//     int left_idx = 0, right_idx = 0;
//     for (int t : timestamps) {
//         int flag = 0;
//         // The start of valid interval from a record
//         // add this record into the active set
//         while (left_idx < data.size() && t == data[left_idx].t_start) {
//             // std::cout << right_idx << "--/--" << left_idx << std::endl;
//             // data[left_idx].print();
//             add_ts = clock();
//             // add record into active set
//             add_active_record(tree, data[left_idx]);
//             left_idx++;
//             add_te = clock();
//             add_total += add_te - add_ts;
//             // std::cout << "end" << std::endl;
//         }
//         // The end of valid interval from a record
//         // report join result and remove 
//         while (right_idx < data_by_right.size() && t == data_by_right[right_idx].t_end) {
//             // data_by_right[right_idx].print();
//             // std::cout << right_idx << "/" << left_idx << std::endl;
//             // get_total_active_records(tree);
//             report_ts = clock();
//             // // issue yannakakis algorithm on current active records
//             // int node_id = data_by_right[right_idx].table_id;
//             // std::vector<join_result> some_answer = yannakakis_durable_join(tree, join_order, root_id, 
//             //             data_by_right[right_idx], join_tables, durability, semi_join_time_total, report_time_total);
//             // clock_t semi_join_ts = clock();
//             if (flag == 0) {
//                 YannakakisJoinTreeNode* root = tree[root_id];
//                 // bottom-up semi-join traversal
//                 yannakakis_bottom_up_semi_join(root);
//                 // top-down semi-join traversal
//                 yannakakis_top_down_semi_join(root);
//                 // TODO: make a copy of the join tables after semi-joins
//                 flag = 1;
//             }
//             // clock_t semi_join_te = clock();
//             // semi_join_time_total += (double) (semi_join_te - semi_join_ts) / CLOCKS_PER_SEC;
//             std::vector<join_result> some_answer = yannakakis_join_with_durability(tree, join_order, data_by_right[right_idx], join_tables, durability);
//             report_te = clock();
//             report_total += report_te - report_ts;
//             // add to answer
//             for (auto& item : some_answer)
//                 answer.emplace_back(item);
//             // print_tree(tree);
//             delete_ts = clock();
//             // remove the record from active set
//             remove_expired_record(tree, data_by_right[right_idx]);
//             // initialize_tree(tree);
//             delete_te = clock();
//             delete_total += delete_te - delete_ts;
//             right_idx++;
//             // std::cout << semi_join_time_total << "/" << report_time_total << std::endl;
//         }
//         if (flag == 1) {
//             initialize_tree(tree);
//             flag = 0;
//         }
//     }
//     te = clock();
//     // free tree memory
//     delete_tree(tree, root_id);
//     std::cout << "delete time: " << (double) delete_total / CLOCKS_PER_SEC << std::endl;
//     // std::cout << "report time: " << report_time_total << std::endl;
//     std::cout << "semi join time: " << semi_join_time_total << std::endl;
//     std::cout << "overall report total: " << (double) report_total / CLOCKS_PER_SEC << std::endl;
//     std::cout << "add time: " << (double) add_total / CLOCKS_PER_SEC << std::endl;
//     // std::cout << "Overall time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << std::endl;
//     std::cout << "Overall time usage: " << (double) (delete_total + add_total + report_total) / CLOCKS_PER_SEC << std::endl;
//     return answer;
// }

std::vector<join_result> Solution::non_hierarchy_durable_join_v2(const std::string filename, std::map<int, std::vector<join_result>>& join_tables, 
                        std::vector<int>& join_order,  std::map<int, std::vector<int>>& join_attrs, int root_id, int durability) {
    std::vector<join_result> answer;

    // build join tree
    std::map<int, YannakakisJoinTreeNode*> tree = create_yannakakis_join_tree(filename);
    YannakakisJoinTreeNode* root = tree[root_id];

    tree_viewer(tree, root_id);

    // sort left endpoints of intervals from all join tables
    std::vector<join_result> data;
    // sort right endpoints of intervals from all join tables
    std::vector<join_result> data_by_right;
    // get all endpoints of intervals
    std::set<int> timestamps;
    for (auto& item : join_tables) {
        for (auto& record : item.second) {
            data.emplace_back(record);
            data_by_right.emplace_back(record);
            timestamps.insert(record.t_start);
            timestamps.insert(record.t_end);
        }
    }
    std::cout << "total record: " << data.size() << "/" << data_by_right.size() << std::endl;

    clock_t sort_ts = clock();
    std::sort(data.begin(), data.end(), 
            [](join_result const& a, join_result const& b)->bool {
                return a.t_start < b.t_start;
            });
    std::sort(data_by_right.begin(), data_by_right.end(), 
            [](join_result const& a, join_result const& b)->bool {
                if (a.t_end == b.t_end)
                    return a.idx < b.idx;
                return a.t_end < b.t_end;
            });
    clock_t sort_te = clock();
    std::cout << "sorting time: " << (double) (sort_te - sort_ts) / CLOCKS_PER_SEC << std::endl;

    clock_t report_ts, report_te, report_total=0;
    clock_t add_ts, add_te, add_total=0;
    clock_t delete_ts, delete_te, delete_total=0;
    double semi_join_time_total = 0, report_time_total = 0;
    int answer_cnt = 0;
    clock_t start = clock();
    // sweep line on time domain to prepare active records for Yannakakis algorithm
    int left_idx = 0, right_idx = 0;
    for (int t : timestamps) {
        // std::cout << "at time: " << t << std::endl;
        while (left_idx < data.size() && t == data[left_idx].t_start) {
            // add record into active set
            add_active_record(tree, data[left_idx]);
            left_idx++;
        }
        // The end of valid interval from a record
        // report join result and remove 
        while (right_idx < data_by_right.size() && t == data_by_right[right_idx].t_end) {
            // std::cout << "==================" << std::endl;
            // data_by_right[right_idx].print();
            // std::cout << '\r' << right_idx << std::flush;
            report_ts = clock();
            std::map<int, std::set<int>> join_candidates_idx;
            std::map<int, std::vector<join_result>> join_candidates;
            int node_id = data_by_right[right_idx].table_id;
            join_candidates[node_id].emplace_back(data_by_right[right_idx]);
            // select down the join tree (for join candidates from other tables according to the current record)
            select_down(tree[node_id], data_by_right[right_idx].attrs, join_candidates_idx, join_tables);
            // select up the join tree
            select_up(tree[node_id], data_by_right[right_idx].attrs, join_candidates_idx, join_tables);
            // do the joins
            // bool print_flag = true;
            // for (int idx : join_order) {
            //     if (join_candidates[idx].empty())
            //         print_flag = false;
            // } 
            // if (print_flag) {
            //     data_by_right[right_idx].print();
            //     for (int idx : join_order) {
            //         std::cout << "====table : " << idx  << " ====" << std::endl;
            //         for (auto item : join_candidates[idx])
            //             item.print();
            //     }
            // }
            bool valid = true;
            for (const auto& candidate_idx : join_candidates_idx) {
                if (candidate_idx.second.empty())
                    valid = false;
            }
            if (valid) {
                for (const auto& candidate_idx : join_candidates_idx) {
                    int table_id = candidate_idx.first;
                    for (int idx : candidate_idx.second)
                        join_candidates[table_id].emplace_back(join_tables[table_id][idx]);
                }
                std::vector<join_result> partial_answer = multiway_durable_join_baseline(join_candidates, join_order, join_attrs, durability, -1, false);
                for (auto& item : partial_answer)
                    answer.emplace_back(item);
            }
            remove_expired_record(tree, data_by_right[right_idx]);
            right_idx++;
            report_te = clock();
            report_total += report_te - report_ts;
        }
    }
    clock_t end = clock();
    delete_tree(tree, root_id);
    // std::cout << "answer size: " << answer_cnt << std::endl;
    std::cout << "report time: " << (double) report_total / CLOCKS_PER_SEC << std::endl;
    std::cout << "overall time: " << (double) (end - start) / CLOCKS_PER_SEC << std::endl;
    return answer;
}

std::vector<join_result> Solution::hierarchy_durable_join(const std::string filename, std::map<int, std::vector<join_result>>& join_tables, 
                            std::vector<int>& join_order, std::map<int, std::vector<int>>& join_attrs, int root_id, int durability) {

    // clock_t ts, te, output_time=0, sorting_time=0, update_time=0, delete_time=0, enum_time = 0, count_time = 0;
    bool valid;
    int join_size = 0;
    std::vector<join_result> answer;

    // build a join tree from the join query
    std::map<int, JoinTreeNode*> tree = create_join_tree(filename);
    JoinTreeNode* root = tree[root_id];

    tree_viewer(tree, root_id);

    // sort left endpoints of intervals from all join tables
    std::vector<join_result> data;
    // sort right endpoints of intervals from all join tables
    std::vector<join_result> data_by_right;
    // get all endpoints of intervals
    std::set<int> timestamps;
    for (auto item : join_tables) {
        for (auto record : item.second) {
            data.emplace_back(record);
            data_by_right.emplace_back(record);
            timestamps.insert(record.t_start);
            timestamps.insert(record.t_end);
        }
    }
    std::sort(data.begin(), data.end(), 
            [](join_result const& a, join_result const& b)->bool {
                return a.t_start < b.t_start;
            });
    std::sort(data_by_right.begin(), data_by_right.end(), 
            [](join_result const& a, join_result const& b)->bool {
                return a.t_end < b.t_end;
            });

    // a sweeping line to process each record in data
    int left_idx = 0, right_idx = 0;
    
    int total_size = timestamps.size();
    int cnt = 0;
    auto ts = high_resolution_clock::now();
    for (int t : timestamps) {
        // std::cout << "\r" << cnt << '/' << total_size << std::flush;
        cnt++;
        //std::cout << "current timestamp:" << t << std::endl;
        while (t == data[left_idx].t_start) {
            // update the structure by inserting a new record
            update(tree, data[left_idx]);
            left_idx++;
        }
        while (t == data_by_right[right_idx].t_end) {
            std::map<int, std::vector<int>> candidates;
            // enumerate
            valid = check(tree, data_by_right[right_idx], durability);
            if (valid) {
                enumerate(tree[root_id], data_by_right[right_idx], join_tables, candidates, durability);
                if (candidates.size() == join_tables.size()) {
                    // join_output(candidates, join_order, join_attrs, join_tables, answer);
                    std::vector<int> temp_answer;
                    join_output(candidates, join_tables, 0, temp_answer, answer);
                    // enumerate_join_output(candidates, 0);
                }
                // ts = clock();
                // if (candidates.size() == join_tables.size()) {
                //     join_size += join_output_size(candidates);
                // }
                // te = clock();
                // count_time += te - ts;
            }
            
            // delete the corresponding record from join tree
            purge(tree, data_by_right[right_idx]);
            right_idx++;
        }
    }
    auto te = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(te - ts);
    std::cout << "Done. Release memory..." << left_idx << ' ' << right_idx << std::endl;
    // std::cout << "enum/output/sorting/update/delete time usage: " << (double) enum_time / CLOCKS_PER_SEC << '/'
    //     << (double) output_time / CLOCKS_PER_SEC << '/' << (double) sorting_time / CLOCKS_PER_SEC << '/' 
    //     << (double) update_time / CLOCKS_PER_SEC << '/' << (double) delete_time / CLOCKS_PER_SEC  << std::endl;
    // std::cout << "count time usage: " << (double) count_time / CLOCKS_PER_SEC << std::endl;
    // std::cout << "join size count: " << join_size << std::endl;
    // clean up tree memory
    delete_tree(tree, root_id);
    std::cout << "total time usage: " << duration.count()  << std::endl;
    return answer;
}

std::vector<join_result> Solution::n_star_durable_join(std::map<int, std::vector<join_result>>& join_tables, 
                        std::vector<int>& join_order, std::map<int, std::vector<int>>& join_attrs) {
    std::vector<join_result> answer;
    std::vector<int> common_join_attr_idxs = join_attrs[0];
    std::map<int, std::unordered_map<int, std::set<int>>> active_tuples;
    for (int table_id : join_order) {
        active_tuples[table_id] = std::unordered_map<int, std::set<int>>{};
        common_join_attr_idxs = get_intersection(common_join_attr_idxs, join_attrs[table_id]);
    }
    print_vector(common_join_attr_idxs);
    int common_join_attr_idx = common_join_attr_idxs[0];
    // sort left endpoints of intervals from all join tables
    std::vector<join_result> data;
    // sort right endpoints of intervals from all join tables
    std::vector<join_result> data_by_right;
    // get all endpoints of intervals
    std::set<int> timestamps;
    for (auto item : join_tables) {
        for (auto record : item.second) {
            data.emplace_back(record);
            data_by_right.emplace_back(record);
            timestamps.insert(record.t_start);
            timestamps.insert(record.t_end);
        }
    }
    clock_t sort_ts = clock();
    std::sort(data.begin(), data.end(), 
            [](join_result const& a, join_result const& b)->bool {
                return a.t_start < b.t_start;
            });
    std::sort(data_by_right.begin(), data_by_right.end(), 
            [](join_result const& a, join_result const& b)->bool {
                return a.t_end < b.t_end;
            });
    clock_t sort_te = clock();

    clock_t ts = clock();
    // a sweeping line to process each record in data
    int left_idx = 0, right_idx = 0;
    int table_id, record_idx, join_value;
    for (int t : timestamps) {
        while (left_idx < data.size() && t == data[left_idx].t_start) {
            table_id = data[left_idx].table_id;
            record_idx = data[left_idx].idx;
            join_value = data[left_idx].attrs[common_join_attr_idx];
            active_tuples[table_id][join_value].insert(record_idx);
            left_idx++;
        }
        while (right_idx < data_by_right.size() && t == data_by_right[right_idx].t_end) {
            table_id = data_by_right[right_idx].table_id;
            record_idx = data_by_right[right_idx].idx;
            join_value = data_by_right[right_idx].attrs[common_join_attr_idx];
            bool valid = true;
            for (auto& item : active_tuples) {
                if (item.second.find(join_value) == item.second.end()) {
                    valid = false;
                    break;
                }
            }
            if (valid) {
                // report results
                std::vector<join_result> last_answer;
                std::vector<join_result> curr_answer;
                for (int i=0; i<join_order.size(); ++i) {
                    std::set<int> candidate = (join_order[i] == table_id) ? std::set<int>{record_idx} : active_tuples[join_order[i]][join_value];
                    if (i == 0) {
                        for (int idx : candidate)
                            last_answer.emplace_back(join_tables[join_order[i]][idx]);
                    }
                    else {
                        for (auto& record : last_answer) {
                            for (int idx : candidate) {
                                join_result join_record = join_tables[join_order[i]][idx];
                                join_result match;
                                match.id = record.id;
                                for (auto& v : join_record.id)
                                    match.id.push_back(v);
                                for (int j=0; j<match.attrs.size(); ++j) {
                                    match.attrs[j] = record.attrs[j];
                                    match.attrs[j] = join_record.attrs[j];
                                }
                                curr_answer.push_back(match);
                            }
                        }
                        std::swap(last_answer, curr_answer);
                        curr_answer.clear();
                    }
                }
                for (auto& item : last_answer)
                    answer.emplace_back(item);
            }
            active_tuples[table_id][join_value].erase(record_idx);
            if (active_tuples[table_id][join_value].empty())
                active_tuples[table_id].erase(join_value);
            right_idx++;
        }
    }
    clock_t te = clock();
    std::cout << "overall time usage: " << (double) (te - ts + (sort_te - sort_ts) / 2) / CLOCKS_PER_SEC << std::endl;
    return answer;
}

std::vector<join_result> Solution::n_star_durable_join_v2(std::map<int, std::vector<join_result>>& join_tables, 
                        std::vector<int>& join_order, std::map<int, std::vector<int>>& join_attrs) {
    std::vector<join_result> answer;
    std::vector<int> common_join_attr_idxs = join_attrs[0];
    std::map<int, std::unordered_map<std::vector<int>, std::set<int>, VectorHasher>> active_tuples;
    for (int table_id : join_order) {
        active_tuples[table_id] = std::unordered_map<std::vector<int>, std::set<int>, VectorHasher>{};
        common_join_attr_idxs = get_intersection(common_join_attr_idxs, join_attrs[table_id]);
    }
    print_vector(common_join_attr_idxs);

    // sort left endpoints of intervals from all join tables
    std::vector<join_result> data;
    // sort right endpoints of intervals from all join tables
    std::vector<join_result> data_by_right;
    // get all endpoints of intervals
    std::set<int> timestamps;
    for (auto item : join_tables) {
        for (auto record : item.second) {
            data.emplace_back(record);
            data_by_right.emplace_back(record);
            timestamps.insert(record.t_start);
            timestamps.insert(record.t_end);
        }
    }
    clock_t sort_ts = clock();
    std::sort(data.begin(), data.end(), 
            [](join_result const& a, join_result const& b)->bool {
                return a.t_start < b.t_start;
            });
    std::sort(data_by_right.begin(), data_by_right.end(), 
            [](join_result const& a, join_result const& b)->bool {
                return a.t_end < b.t_end;
            });
    clock_t sort_te = clock();

    clock_t ts = clock();
    // a sweeping line to process each record in data
    int left_idx = 0, right_idx = 0;
    int table_id, record_idx;
    std::vector<int> join_value = std::vector<int>(this->total_num_attrs, UNDEF);
    for (int t : timestamps) {
        while (left_idx < data.size() && t == data[left_idx].t_start) {
            table_id = data[left_idx].table_id;
            record_idx = data[left_idx].idx;
            for (int idx : common_join_attr_idxs)
                join_value[idx] = data[left_idx].attrs[idx];
            active_tuples[table_id][join_value].insert(record_idx);
            left_idx++;
        }
        while (right_idx < data_by_right.size() && t == data_by_right[right_idx].t_end) {
            table_id = data_by_right[right_idx].table_id;
            record_idx = data_by_right[right_idx].idx;
            for (int idx : common_join_attr_idxs)
                join_value[idx] = data_by_right[right_idx].attrs[idx];
            bool valid = true;
            for (auto& item : active_tuples) {
                if (item.second.find(join_value) == item.second.end()) {
                    valid = false;
                    break;
                }
            }
            if (valid) {
                // report results
                std::vector<join_result> last_answer;
                std::vector<join_result> curr_answer;
                for (int i=0; i<join_order.size(); ++i) {
                    std::set<int> candidate = (join_order[i] == table_id) ? std::set<int>{record_idx} : active_tuples[join_order[i]][join_value];
                    if (i == 0) {
                        for (int idx : candidate)
                            last_answer.emplace_back(join_tables[join_order[i]][idx]);
                    }
                    else {
                        for (auto& record : last_answer) {
                            for (int idx : candidate) {
                                join_result join_record = join_tables[join_order[i]][idx];
                                join_result match;
                                match.id = record.id;
                                for (auto& v : join_record.id)
                                    match.id.push_back(v);
                                for (int j=0; j<match.attrs.size(); ++j) {
                                    match.attrs[j] = record.attrs[j];
                                    match.attrs[j] = join_record.attrs[j];
                                }
                                curr_answer.push_back(match);
                            }
                        }
                        std::swap(last_answer, curr_answer);
                        curr_answer.clear();
                    }
                }
                for (auto& item : last_answer)
                    answer.emplace_back(item);
            }
            active_tuples[table_id][join_value].erase(record_idx);
            if (active_tuples[table_id][join_value].empty())
                active_tuples[table_id].erase(join_value);
            right_idx++;
        }
    }
    clock_t te = clock();
    std::cout << "overall time usage: " << (double) (te - ts + (sort_te - sort_ts) / 2) / CLOCKS_PER_SEC << std::endl;
    return answer;
}

void Solution::select_up(YannakakisJoinTreeNode* node, const std::vector<int>& join_attrs, std::map<int, std::set<int>>& candidates, std::map<int, std::vector<join_result>>& join_tables) {
    if (node == nullptr || node->parent == nullptr)
        return;
    
    int table_id = node->parent->node_id;
    std::vector<int> common_attrs = get_intersection(node->join_attrs, node->parent->join_attrs);
    std::vector<int> key = std::vector<int>(this->total_num_attrs, UNDEF);
    for (int idx : common_attrs)
        key[idx] = join_attrs[idx];
    for (auto& new_key : node->projection_down[key]) {
        for (int idx : node->parent->origin_table[new_key]) {
            candidates[table_id].insert(idx);
        }
        select_up(node->parent, new_key, candidates, join_tables);
    }
}

void Solution::select_down(YannakakisJoinTreeNode* node, const std::vector<int>& join_attrs, std::map<int, std::set<int>>& candidates, std::map<int, std::vector<join_result>>& join_tables) {
    if (node == nullptr)
        return;
    
    for (int i=0; i<node->children.size(); ++i) {
        int table_id = node->children[i]->node_id;
        std::vector<int> common_attrs = get_intersection(node->join_attrs, node->children[i]->join_attrs);
        std::vector<int> key = std::vector<int>(this->total_num_attrs, UNDEF);
        for (int idx : common_attrs)
            key[idx] = join_attrs[idx];
        for (auto& new_key : node->children[i]->projection_up[key]) {
            for (int idx : node->children[i]->origin_table[new_key]) {
                candidates[table_id].insert(idx);
            }
            select_down(node->children[i], new_key, candidates, join_tables);
        }
    }
}

void Solution::semi_join(YannakakisJoinTreeNode* base, YannakakisJoinTreeNode* filter) {
    std::unordered_set<std::vector<int>, VectorHasher> matching_values;
    // if (base == filter->parent) {
    //     // std::cout << "base node: " << base->node_id << " filter node: " << filter->node_id << std::endl;
    //     // std::cout << "filter keys: ";
    //     // for (auto v : filter->non_dangling_join_values)
    //     //     print_vector(v);
    //     // std::cout << "parent keys: ";
    //     // for (auto v : filter->parent_table)
    //     //     print_vector(v.first);
    //     for (auto& key : filter->non_dangling_join_values) {
    //         if (filter->parent_table.find(key) != filter->parent_table.end())
    //             matching_values.insert(key);
    //     }
    // }
    // else {
    //     for (auto& key : base->non_dangling_join_values) {
    //         if (base->parent_table.find(key) != base->parent_table.end())
    //             matching_values.insert(key);
    //     }
    // }
    // if the join attrs are the same, directly search based on the join attrs
    if (base->join_attrs == filter->join_attrs) {
        std::cout << "equal case" << std::endl; 
        for (auto& item : base->origin_table) {
            std::vector<int> key = item.first;
            if (filter->non_dangling_join_values.find(key) != filter->non_dangling_join_values.end())
                matching_values.insert(key);
        }
    }
    // else, linear search on the common attrs
    else {
        std::vector<int> common_attrs = get_intersection(base->join_attrs, filter->join_attrs);
        // // base is the parent
        // if (base == filter->parent) {
        //     if (!base->non_dangling_join_values.empty()) {
        //         // assert (base->non_dangling_join_values.size() == 1);
        //         std::vector<int> projected_key = std::vector<int>(this->total_num_attrs, UNDEF);
        //         std::vector<int> key = *(base->non_dangling_join_values.begin());
        //         for (int idx : common_attrs)
        //             projected_key[idx] = key[idx];
        //         if (filter->projection_down.find(projected_key) != filter->projection_down.end())
        //             matching_values.insert(key);
        //     }
        //     else {
        //         for (auto& key : filter->non_dangling_join_values) {
        //             std::vector<int> common_key = std::vector<int>(this->total_num_attrs, UNDEF);
        //             for (int idx : common_attrs)
        //                 common_key[idx] = key[idx];
        //             for (auto& projected_key : filter->projection_down[common_key]) {
        //                 if (base->origin_table.find(projected_key) != base->origin_table.end())
        //                     matching_values.insert(projected_key);
        //             }
        //         }
        //     }
        // }
        // // filter is the parent
        // else {
        //     for (auto& key : filter->non_dangling_join_values) {
        //         std::vector<int> common_key = std::vector<int>(this->total_num_attrs, UNDEF);
        //         for (int idx : common_attrs)
        //             common_key[idx] = key[idx];
        //         for (auto& projected_key : base->projection_up[common_key]) {
        //             if (base->non_dangling_join_values.find(projected_key) != base->non_dangling_join_values.end())
        //                 matching_values.insert(projected_key);
        //         }
        //     }
        // }
        for (auto& key : filter->non_dangling_join_values) {
            std::vector<int> common_key = std::vector<int>(this->total_num_attrs, UNDEF);
            for (int idx : common_attrs)
                common_key[idx] = key[idx];
            // base is parent
            if (base == filter->parent) {
                for (auto& projected_key : filter->projection_down[common_key]) {
                    if (base->origin_table.find(projected_key) != base->origin_table.end())
                        matching_values.insert(projected_key);
                }
            }
            // filter is parent
            else {
                for (auto& projected_key : base->projection_up[common_key]) {
                    if (base->non_dangling_join_values.find(projected_key) != base->non_dangling_join_values.end())
                        matching_values.insert(projected_key);
                }
            }
        }
        // for (auto& item : base->origin_table) {
        //     std::vector<int> key = item.first;
        //     std::vector<int> common_key = std::vector<int>(this->total_num_attrs, UNDEF);
        //     for (int idx : common_attrs)
        //         common_key[idx] = key[idx];
        //     if (join_value_set.find(common_key) != join_value_set.end()) {
        //         matching_values.insert(key);
        //     }
        // }
    }
    if (matching_values.empty())
        return;
    if (base->non_dangling_join_values.empty()) {
        base->non_dangling_join_values = matching_values;
    }
    else {
        // for (auto it = base->non_dangling_join_values.begin(); it != base->non_dangling_join_values.end(); ) {
        //     if (matching_values.find(*it) == matching_values.end()) {
        //         it = base->non_dangling_join_values.erase(it);
        //     }
        //     else {
        //         ++it;
        //     }
        // }
        std::unordered_set<std::vector<int>, VectorHasher> non_dangling_values;
        for (auto value : matching_values) {
            if (base->non_dangling_join_values.find(value) != base->non_dangling_join_values.end())
                non_dangling_values.insert(value);
        }
        base->non_dangling_join_values = non_dangling_values;
    }
    // std::cout << "matching values for node " << base->node_id << " : ";
    // for (auto values : base->non_dangling_join_values)
    //     print_vector(values);
}

void Solution::yannakakis_bottom_up_semi_join(YannakakisJoinTreeNode* node) {
    // leaf node, prepare join values
    if (node->children.size() == 0) {
        prepare_join_values(node);
        return;
    }
    for (int i=0; i<node->children.size(); ++i) {
        yannakakis_bottom_up_semi_join(node->children[i]);
    }
    // semi-join to remove dangling records bottom up
    for (int i=0; i<node->children.size(); ++i) {
        semi_join(node, node->children[i]);
    }
    return;
}

void Solution::yannakakis_top_down_semi_join(YannakakisJoinTreeNode* node) {
    // leaf node, stop
    if (node->children.size() == 0) 
        return;
    
    // if a join table contains empty non-dangling record, stop
    if (node->non_dangling_join_values.size() == 0)
        return;
    
    // semi-join remove dangling records top-down
    // semi-join on its child tables to filter dangling records
    for (int i=0; i<node->children.size(); ++i) {
        semi_join(node->children[i], node);
    }
    for (int i=0; i<node->children.size(); ++i) {
        yannakakis_top_down_semi_join(node->children[i]);
    }
}

std::vector<join_result> Solution::yannakakis_join_with_durability(std::map<int, YannakakisJoinTreeNode*>& tree, std::vector<int>& join_order, 
            join_result& record, std::map<int, std::vector<join_result>>& join_tables, int durability) {
    std::vector<join_result> answer;
    // if there is any empty join table, just return
    for (auto& item : tree) {
        if (item.second->non_dangling_join_values.size() == 0)
            return answer;
        // if (item.first == record.table_id) {
        //     if (item.second->non_dangling_join_values.find(record.attrs) == item.second->non_dangling_join_values.end()) {
        //         return answer;
        //     }
        //     else {
        //         item.second->non_dangling_join_values.clear();
        //         // just keep the current record to participate in join
        //         item.second->non_dangling_join_values.insert(record.attrs);
        //     }
        // }
    }
    // join 
    std::vector<join_result> partial_answer;
    for (int i=0; i<join_order.size(); ++i) {
        YannakakisJoinTreeNode* table = tree[join_order[i]];
        if (i == 0) {
            for (auto& key : table->non_dangling_join_values) {
                for (int idx : table->origin_table[key]) {
                    partial_answer.emplace_back(join_tables[table->node_id][idx]);
                }
            }
        }
        else {
            if (partial_answer.size() == 0)
                return answer;
            std::vector<int> common_attrs = get_intersection(partial_answer[0].attr_id, table->join_attrs);
            std::vector<int> union_attrs = get_union(partial_answer[0].attr_id, table->join_attrs);
            std::vector<join_result> join_candidates;
            for (auto& key : table->non_dangling_join_values) {
                for (int idx : table->origin_table[key]) {
                    join_candidates.emplace_back(join_tables[table->node_id][idx]);
                }
            }
            if (join_candidates.size() == 0)
                return answer;
            // sort_by_join_attrs(partial_answer, common_attrs);
            // sort_by_join_attrs(join_candidates, common_attrs);
            // partial_answer = pairwise_forward_scan_temporal_join(partial_answer, join_candidates, common_attrs, union_attrs, durability);
        }
    }
    // answer = partial_answer;
    return answer;
}

// std::vector<join_result> Solution::yannakakis_self_join_with_durability(std::map<int, YannakakisJoinTreeNode*>& tree, std::vector<int>& join_order, 
//             join_result& record, std::map<int, std::vector<join_result>>& join_tables, int durability) {
//     std::vector<join_result> answer;
//     // join 
//     std::vector<join_result> partial_answer;
//     for (int i=0; i<join_order.size(); ++i) {
//         YannakakisJoinTreeNode* table = tree[join_order[i]];
//         if (i == 0) {
//             // for (auto& item : table->origin_table) {
//             //     for (int idx : item.second) {
//             //         partial_answer.emplace_back(join_tables[table->node_id][idx]);
//             //     }
//             // }
//             for (auto& key : table->non_dangling_join_values) {
//                 for (int idx : table->origin_table[key]) {
//                     partial_answer.emplace_back(join_tables[table->node_id][idx]);
//                 }
//             }
//         }
//         else {
//             if (partial_answer.size() == 0)
//                 return answer;
//             std::vector<int> common_attrs = get_intersection(partial_answer[0].attr_id, table->join_attrs);
//             std::vector<int> union_attrs = get_union(partial_answer[0].attr_id, table->join_attrs);
//             std::vector<join_result> join_candidates;
//             // for (auto& item : table->origin_table) {
//             //     for (int idx : item.second) {
//             //         join_candidates.emplace_back(join_tables[table->node_id][idx]);
//             //     }
//             // }
//             for (auto& key : table->non_dangling_join_values) {
//                 for (int idx : table->origin_table[key]) {
//                     join_candidates.emplace_back(join_tables[table->node_id][idx]);
//                 }
//             }
//             sort_by_join_attrs(partial_answer, common_attrs);
//             sort_by_join_attrs(join_candidates, common_attrs);
//             partial_answer = pairwise_forward_scan_temporal_join(partial_answer, join_candidates, common_attrs, union_attrs, durability);
//         }
//     }
//     answer = partial_answer;
//     return answer;
// }

std::vector<join_result> Solution::yannakakis_durable_join(std::map<int, YannakakisJoinTreeNode*>& tree, std::vector<int>& join_order, 
                int root_id, join_result& record, std::map<int, std::vector<join_result>>& join_tables, int durability, double& semi_join_time, double& report_time) {
    std::vector<join_result> answer;
    clock_t ts, te;
    // if there is any empty join table, just return
    for (auto& item : tree) {
        if (item.second->origin_table.size() == 0) {
            return answer;
        }
    }
    int join_node_id = record.table_id;
    // assert (tree[join_node_id]->non_dangling_join_values.empty());
    tree[join_node_id]->non_dangling_join_values.insert(record.attrs);

    // initialize the join tree
    YannakakisJoinTreeNode* root = tree[root_id];
    ts = clock();
    // bottom-up semi-join traversal
    yannakakis_bottom_up_semi_join(root);
    te = clock();
    semi_join_time += (double) (te - ts) / CLOCKS_PER_SEC;
    // if any node of the tree becomes empty, return empty
    if(empty_join_tree(tree)) {
        return answer;
    }
    ts = clock();
    // top-down semi-join traversal
    yannakakis_top_down_semi_join(root);
    te = clock();
    semi_join_time += (double) (te - ts) / CLOCKS_PER_SEC;
    // join bottom up
    ts = clock();
    answer = yannakakis_join_with_durability(tree, join_order, record, join_tables, durability);
    te = clock();
    report_time += (double) (te - ts) / CLOCKS_PER_SEC;
    return answer;
}

std::vector<join_result> Solution::durable_generic_join(std::map<int, std::vector<join_result>>& join_tables, std::vector<int>& join_order, std::map<int, std::vector<int>>& join_attrs,
                            int index, int durability) {
    std::vector<join_result> answer;
    std::set<int> global_attr_ids;
    for (auto& item : join_attrs) {
        for (int id : item.second)
            global_attr_ids.insert(id);
    }
    std::vector<int> global_join_attrs(global_attr_ids.begin(), global_attr_ids.end());
    clock_t ts = clock();
    // get join values from generic join
    std::vector<std::vector<int>> join_values = generic_join(this->join_hash_index, global_join_attrs, index, global_join_attrs.size());
    clock_t te = clock();
    std::cout << "generic join time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << std::endl;
    ts = clock();
    // do durability check
    for (auto& key : join_values) {
        // if (key.size() == 4 && key[0] == key[2] && key[1] == key[3]) {
        //     // print_vector(key);
        //     continue;
        // }
        std::map<int, std::vector<join_result>> new_join_tables;
        for (int table_id : join_order) {
            std::vector<int> partial_key = std::vector<int>(this->total_num_attrs, UNDEF);
            for (int attr_idx : join_attrs[table_id])
                partial_key[attr_idx] = key[attr_idx];
            for (int idx : join_table_index[table_id][partial_key])
                new_join_tables[table_id].emplace_back(join_tables[table_id][idx]);
        }
        std::vector<join_result> some_answer = multiway_durable_join_baseline(new_join_tables, join_order, join_attrs, durability, -1, false);
        for (auto& item : some_answer)
            answer.emplace_back(item);
    }
    te = clock();
    std::cout << "durability check time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << std::endl;
    return answer;
}

std::vector<std::vector<int>> Solution::generic_join(std::map<int, LevelHashMap*>& join_hash_index, std::vector<int>& global_join_attrs, int index, int join_attrs_count) {
    // the last join attr index
    // std::cout << "current index: " << index << std::endl;
    std::vector<std::vector<int>> result;
    if (index == join_attrs_count - 1) {
        std::set<int> unique_values = set_intersect(join_hash_index, global_join_attrs[index]);
        for (int value : unique_values) {
            std::vector<int> base = std::vector<int>(this->total_num_attrs, UNDEF);
            base[global_join_attrs[index]] = value;
            result.emplace_back(base);
        }
        return result;
    }
    std::set<int> unique_join_values = set_intersect(join_hash_index, global_join_attrs[index]);

    for (int join_value : unique_join_values) {
        std::map<int, LevelHashMap*> partial_join_hash_index;
        for (auto& item : join_hash_index) {
            int table_id = item.first;
            partial_join_hash_index[table_id] = select(join_hash_index[table_id], global_join_attrs[index], join_value);
        }
        std::vector<std::vector<int>> partial_answer = generic_join(partial_join_hash_index, global_join_attrs, index+1, join_attrs_count);
        for (auto partial_join_result : partial_answer) {
            partial_join_result[global_join_attrs[index]] = join_value;
            result.emplace_back(partial_join_result);
            // std::cout << "get a partial answer!" << std::endl;
            // print_vector(partial_join_result);
        }
    }
    return result;
}

LevelHashMap* Solution::select(LevelHashMap* hash_index, int index, int join_value) {
    if (hash_index->join_attr_id != index)
        return hash_index;

    if (hash_index->children.empty())
        return hash_index;
    
    return hash_index->children[join_value];
}

std::set<int> Solution::set_intersect(std::map<int, LevelHashMap*>& join_table_index, int index) {
    std::vector<std::set<int>> projected_join_tables;
    for (auto& item : join_table_index) {
        if (item.second == nullptr)
            return std::set<int>();
        if (item.second->join_attr_id == index) 
            projected_join_tables.emplace_back(item.second->base_key);
    }
    // assert(projected_join_tables.size() == 2);
    // std::set<int> last_intersection = projected_join_tables[0];
    // std::set<int> curr_intersection;
    // for (int i=1; i<projected_join_tables.size(); ++i) {
    //     std::set_intersection(last_intersection.begin(), last_intersection.end(), projected_join_tables[i].begin(), projected_join_tables[i].end(),
    //                          std::inserter(curr_intersection, curr_intersection.begin()));
    //     std::swap(last_intersection, curr_intersection);
    //     curr_intersection.clear();
    // }
    // std::cout << "set intersect values: " << std::endl;
    // for (int v : answer)
    //     std::cout << v << ',';
    // std::cout << std::endl;
    std::set<int> intersection;
    if (projected_join_tables[0].size() <= projected_join_tables[1].size()) {
        for (int v : projected_join_tables[0]) {
            if (projected_join_tables[1].find(v) != projected_join_tables[1].end())
                intersection.insert(v);
        }
    }
    else {
        for (int v : projected_join_tables[1]) {
            if (projected_join_tables[0].find(v) != projected_join_tables[0].end())
                intersection.insert(v);
        }
    }
    return intersection;

}

// std::set<std::vector<int>> Solution::generic_join(std::map<int, std::vector<join_result>>& join_tables, int index, int join_attrs_count) {
//     // the last join attr index
//     std::set<std::vector<int>> result;
//     if (index == join_attrs_count - 1) {
//         std::set<int> unique_values = set_intersect(join_tables, index);
//         for (int value : unique_values) {
//             std::vector<int> base = std::vector<int>(this->total_num_attrs, UNDEF);
//             base[index] = value;
//             result.insert(base);
//         }
//         return result;
//     }

//     std::set<int> unique_join_values = set_intersect(join_tables, index);

//     for (int join_value : unique_join_values) {
//         std::map<int, std::vector<join_result>> partial_join_tables;
//         for (auto& item : join_tables) {
//             int table_id = item.first;
//             partial_join_tables[table_id] = select(item.second, index, join_value);
//         }
//         std::set<std::vector<int>> partial_answer = generic_join(partial_join_tables, index+1, join_attrs_count);
//         for (auto partial_join_result : partial_answer) {
//             partial_join_result[index] = join_value;
//             result.insert(partial_join_result);
//         }
//     }
//     return result;
// }

// std::vector<join_result> Solution::select(std::vector<join_result>& table, int index, int join_value) {
//     std::vector<join_result> selected;
//     // if the current join attr is not in the table
//     if (std::find(table[0].attr_id.begin(), table[0].attr_id.end(), index) == table[0].attr_id.end()) {
//         return table;
//     }
//     for (auto& record : table) {
//         if (record.attrs[index] == join_value)
//             selected.emplace_back(record);
//     }
//     return selected;
// }

// std::set<int> Solution::set_intersect(std::map<int, std::vector<join_result>>& join_tables, int index) {
//     std::vector<std::set<int>> projected_join_tables;
//     // value projection based on the current join attr index
//     for (auto& item : join_tables) {
//         int table_id = item.first;
//         std::vector<join_result> table = item.second;
//         // std::cout << table.size() << std::endl;
//         if (std::find(table[0].attr_id.begin(), table[0].attr_id.end(), index) != table[0].attr_id.end()) {
//             std::set<int> values;
//             for (auto& record : table)
//                 values.insert(record.attrs[index]);
//             projected_join_tables.emplace_back(values);
//         }
//     }
//     // set intersection
//     if (projected_join_tables.size() == 1) {
//         std::cout << "SHOULD NOT BE HERE!" << std::endl;
//         return projected_join_tables[0];
//     }
//     std::vector<int> last_intersection(projected_join_tables[0].begin(), projected_join_tables[0].end());
//     std::vector<int> curr_intersection;
//     for (int i=1; i<projected_join_tables.size(); ++i) {
//         std::set_intersection(last_intersection.begin(), last_intersection.end(), projected_join_tables[i].begin(), projected_join_tables[i].end(),
//                              std::back_inserter(curr_intersection));
//         std::swap(last_intersection, curr_intersection);
//         curr_intersection.clear();
//     }
//     std::set<int> answer(last_intersection.begin(), last_intersection.end());
//     return answer;
// }

// std::vector<join_result> Solution::generic_join(std::map<int, std::vector<join_result>>& join_tables, int index, int join_attrs_count, int durability) {
//     // std::cout << "current join index: " << index << std::endl;
//     // the last join attr index
//     if (index == join_attrs_count - 1) {
//         return set_intersect(join_tables, index, durability);
//     }
//     std::vector<join_result> answer;
//     // std::vector<join_result> answer_buffer;
//     std::vector<join_result> partial_join_results = set_intersect(join_tables, index, durability);
//     // for (auto item : partial_join_results) 
//     //     item.print();
//     std::vector<int> join_attr_idx = {index};
//     // sort_by_join_attrs(partial_join_results, join_attr_idx);
//     std::unordered_map<int, std::vector<join_result>> distinct_join_values;
//     for (auto& record : partial_join_results) {
//         distinct_join_values[record.attrs[index]].emplace_back(record);
//     }
//     for (auto value_group : distinct_join_values) {
//         int join_value = value_group.first;
//         // std::cout << "current join index: " << index << " current join value: " << join_value << std::endl;
//         // record.print();
//         // process unique join values
//         // if (distinct_join_value.find(join_value) == distinct_join_value.end()) {
//         // answer_buffer.clear();
//         // distinct_join_value.insert(join_value);
//         std::map<int, std::vector<join_result>> partial_join_tables;
//         for (auto& item : join_tables) {
//             int table_id = item.first;
//             partial_join_tables[table_id] = select(item.second, table_id, index, join_value);
//         }
//         // std::cout << "partial join tables ready" << std::endl;
//         // for (auto item : partial_join_tables) {
//         //     std::cout << "=====table : " << item.first << std::endl;
//         //     for (auto entry : item.second)
//         //         entry.print();
//         // }
//         std::vector<join_result> partial_answer = generic_join(partial_join_tables, index+1, join_attrs_count, durability);
//         // std::cout << "partial answers: " << std::endl;
//         // for (auto item : partial_answer) {
//         //     item.print();
//         // }
//         if (partial_answer.empty())
//             continue;
//         std::vector<int> common_attrs = get_intersection(partial_answer[0].attr_id, (value_group.second)[0].attr_id);
//         std::vector<int> union_attrs = get_union(partial_answer[0].attr_id, (value_group.second)[0].attr_id);
//         sort_by_join_attrs(partial_answer, common_attrs);
//         sort_by_join_attrs(value_group.second, common_attrs);
//         partial_answer = pairwise_forward_scan_temporal_join(value_group.second, partial_answer, common_attrs, union_attrs, durability);
//         for (auto& entry : partial_answer)
//             answer.emplace_back(entry);
//     }
//     return answer;
// }

// std::vector<join_result> Solution::select(std::vector<join_result>& table, int table_id, int index, int join_value) {
//     std::vector<join_result> selected;
//     // if the current join attr is not in the table
//     if (std::find(table[0].attr_id.begin(), table[0].attr_id.end(), index) == table[0].attr_id.end()) {
//         return table;
//     }
//     // for (auto& record : table) {
//     //     if (record.attrs[index] == join_value)
//     //         selected.emplace_back(record);
//     // }
//     // std::cout << join_table_index.at(0).at(0).at(646)[0] << std::endl; 
//     // for (int idx : join_table_index.at(table_id).at(index).at(join_value)) {
//     std::vector<int> table_idx = join_table_index.at(table_id).at(index).at(join_value);
//     // for (int i=0; i<join_table_index.at(table_id).at(index).at(join_value).size(); ++i) {
//     // for (int idx : table_idx) {
//     //     // std::cout << join_table_index.at(index).at(join_value).size() << std::endl;
//     //     // int index = join_table_index.at(table_id).at(index).at(join_value)[i];
//     //     selected.emplace_back(table[idx]);
//     // }
//     for (int i=0; i<table_idx.size(); ++i) {
        
//     }
//     return selected;
// }

// std::vector<join_result> Solution::set_intersect(std::map<int, std::vector<join_result>>& join_tables, int index, int durability) {
//     std::map<int, std::vector<join_result>> projected_join_tables;
//     std::vector<int> join_order;
//     // std::cout << "set intersecting" << std::endl;
//     for (auto& item : join_tables) {
//         int table_id = item.first;
//         std::vector<join_result> table = item.second;
//         // std::cout << table.size() << std::endl;
//         if (std::find(table[0].attr_id.begin(), table[0].attr_id.end(), index) != table[0].attr_id.end()) {
//             projected_join_tables[table_id] = table;
//             join_order.push_back(table_id);
//         }
//     }
//     // print_vector(join_order);
//     // set-intersect
//     std::vector<join_result> answer;
//     std::vector<join_result> partial_answer;
//     for (int i=0; i<join_order.size(); ++i) {
//         if (i == 0) {
//             partial_answer = projected_join_tables[join_order[i]];
//         }
//         else {
//             if (partial_answer.empty())
//                 return answer;
//             std::vector<int> common_attrs = std::vector<int>{index};
//             std::vector<int> union_attrs = get_union(partial_answer[0].attr_id, projected_join_tables[join_order[i]][0].attr_id);
//             sort_by_join_attrs(partial_answer, common_attrs);
//             sort_by_join_attrs(projected_join_tables[join_order[i]], common_attrs);
//             partial_answer = pairwise_forward_scan_temporal_join(partial_answer, projected_join_tables[join_order[i]], common_attrs, union_attrs, durability);
//         }
//     }
//     answer = partial_answer;
//     return answer;
// }

std::vector<join_result> Solution::interval_join(intervalVector& result, intervalTree* iTree, int durability) {
    std::vector<join_result> ans;
    for (auto &item : result) {
        intervalVector overlaps = iTree->findOverlapping_with_durability(item.start, item.stop, durability);
        for (auto &it : overlaps) {
            join_result match;
            match.id.push_back(item.value);
            match.id.push_back(it.value);
            match.t_start = MAX(item.start, it.start);
            match.t_end = MIN(item.stop, it.stop);
            ans.emplace_back(match);
        }
    }
    return ans;
}

int Solution::interval_join_count(intervalVector& result, intervalTree* iTree) {
    int ans = 0;
    for (auto &item : result) {
        ans += iTree->countOverlapping(item.start, item.stop);
    }
    return ans;
}

int Solution::line_3_join_counting(std::vector<join_result>& left_table, std::vector<join_result>& middle_table, 
                        std::vector<join_result>& right_table, std::map<int, std::vector<int>>& join_attrs, int durability) {
    clock_t ts, te;
    clock_t index_time = 0, query_time = 0;
    int answer = 0;
    // build a set of interval trees for left table
    std::map<std::vector<int>, intervalVector> left_table_intervals;
    std::vector<int> key;
    std::vector<int> left_attrs = get_intersection(join_attrs[0], join_attrs[1]);
    std::vector<int> right_attrs = get_intersection(join_attrs[1], join_attrs[2]);
    // for (auto const& item : left_table) {
    for (int i = 0; i < left_table.size(); ++i) {
        key = std::vector<int>(this->total_num_attrs, UNDEF);
        for (auto idx : left_attrs)
            key[idx] = left_table[i].attrs[idx];
        left_table_intervals[key].push_back(interval(left_table[i].t_start + durability/2, left_table[i].t_end - durability/2, i));
    }
    ts = clock();
    std::map<std::vector<int>, intervalTree*> left_forest;
    for (auto& item : left_table_intervals)
        left_forest[item.first] = new intervalTree(std::move(item.second), 16, 1);
    te = clock();
    index_time += te - ts;

    // build a set of interval tress for right table
    std::map<std::vector<int>, intervalVector> right_table_intervals;
    // for (auto const& item : right_table) {
    for (int i = 0; i < right_table.size(); ++i) {
        key = std::vector<int>(this->total_num_attrs, UNDEF);
        // for (auto idx : join_attrs[2])
        for (auto idx : right_attrs)
            key[idx] = right_table[i].attrs[idx];
        right_table_intervals[key].push_back(interval(right_table[i].t_start + durability/2, right_table[i].t_end - durability/2, i));
    }
    ts = clock();
    std::map<std::vector<int>, intervalTree*> right_forest;
    for (auto& item : right_table_intervals) 
        right_forest[item.first] = new intervalTree(std::move(item.second), 16, 1);
    te = clock();
    index_time += te - ts;

    std::cout << "indexing time: " << (double) index_time / CLOCKS_PER_SEC << std::endl;

    
    std::vector<int> union_attrs = get_union(join_attrs[0], join_attrs[1]);
    union_attrs = get_union(union_attrs, join_attrs[2]);
    std::vector<int> left_key, right_key;

    ts = clock();
    // simple line-3 algorithm
    int cnt = 0;
    for (auto & record : middle_table) {
        cnt++;
        left_key = std::vector<int>(this->total_num_attrs, UNDEF);
        right_key = std::vector<int>(this->total_num_attrs, UNDEF);
        for (int idx : left_attrs)
            left_key[idx] = record.attrs[idx];
        for (int idx : right_attrs)
            right_key[idx] = record.attrs[idx];
        if (left_forest.find(left_key) == left_forest.end() || right_forest.find(right_key) == right_forest.end())
                continue;
        if (left_table_intervals[left_key].size() <= right_table_intervals[right_key].size()) {
            intervalVector left_overlaps 
                = left_forest[left_key]->findOverlapping(record.t_start + durability/2, record.t_end - durability/2);
            if (left_overlaps.empty())
                continue;
            for (auto &item : left_overlaps) {
                item.start = MAX(item.start, record.t_start);
                item.stop = MIN(item.stop, record.t_end);
            }
            answer += interval_join_count(left_overlaps, right_forest[right_key]);
        }
        else {
            intervalVector right_overlaps
                = right_forest[right_key]->findOverlapping(record.t_start + durability/2, record.t_end - durability/2);
            
            if (right_overlaps.empty())
                continue;
            for (auto &item : right_overlaps) {
                item.start = MAX(item.start, record.t_start);
                item.stop = MIN(item.stop, record.t_end);
            }
            answer += interval_join_count(right_overlaps, left_forest[left_key]);
        }
    }
    te = clock();
    query_time = te - ts;
    std::cout << "query time: " << (double) query_time / CLOCKS_PER_SEC << std::endl; 
    std::cout << "total time usage: " << (double) (query_time + index_time) / CLOCKS_PER_SEC << std::endl;
    return answer;
}

std::vector<join_result> Solution::line_3_join(std::vector<join_result>& left_table, std::vector<join_result>& middle_table, 
                        std::vector<join_result>& right_table, std::map<int, std::vector<int>>& join_attrs, int durability) {
    clock_t ts, te;
    clock_t index_time = 0, query_time = 0;
    std::vector<join_result> answer;
    // build a set of interval trees for left table
    std::map<std::vector<int>, intervalVector> left_table_intervals;
    std::vector<int> key;
    std::vector<int> left_attrs = get_intersection(join_attrs[0], join_attrs[1]);
    std::vector<int> right_attrs = get_intersection(join_attrs[1], join_attrs[2]);

    // for (auto const& item : left_table) {
    for (int i = 0; i < left_table.size(); ++i) {
        key = std::vector<int>(this->total_num_attrs, UNDEF);
        for (auto idx : left_attrs)
            key[idx] = left_table[i].attrs[idx];
        left_table_intervals[key].push_back(interval(left_table[i].t_start, left_table[i].t_end, i));
    }
    ts = clock();
    std::map<std::vector<int>, intervalTree*> left_forest;
    for (auto& item : left_table_intervals)
        left_forest[item.first] = new intervalTree(std::move(item.second), 16, 1);
    te = clock();
    index_time += te - ts;

    // build a set of interval tress for right table
    std::map<std::vector<int>, intervalVector> right_table_intervals;
    // for (auto const& item : right_table) {
    for (int i = 0; i < right_table.size(); ++i) {
        key = std::vector<int>(this->total_num_attrs, UNDEF);
        // for (auto idx : join_attrs[2])
        for (auto idx : right_attrs)
            key[idx] = right_table[i].attrs[idx];
        right_table_intervals[key].push_back(interval(right_table[i].t_start, right_table[i].t_end, i));
    }
    ts = clock();
    std::map<std::vector<int>, intervalTree*> right_forest;
    for (auto& item : right_table_intervals) 
        right_forest[item.first] = new intervalTree(std::move(item.second), 16, 1);
    te = clock();
    index_time += te - ts;

    std::cout << "indexing time: " << (double) index_time / CLOCKS_PER_SEC << std::endl;

    
    std::vector<int> union_attrs = get_union(join_attrs[0], join_attrs[1]);
    union_attrs = get_union(union_attrs, join_attrs[2]);
    std::vector<int> left_key, right_key;

    ts = clock();
    // simple line-3 algorithm
    int cnt = 0;
    for (auto & record : middle_table) {
        // std::cout << "\r" << cnt << '/' << middle_table.size() << std::flush;
        cnt++;
        left_key = std::vector<int>(this->total_num_attrs, UNDEF);
        right_key = std::vector<int>(this->total_num_attrs, UNDEF);
        for (int idx : left_attrs)
            left_key[idx] = record.attrs[idx];
        for (int idx : right_attrs)
            right_key[idx] = record.attrs[idx];
        if (left_forest.find(left_key) == left_forest.end() || right_forest.find(right_key) == right_forest.end())
                continue;
        if (left_table_intervals[left_key].size() <= right_table_intervals[right_key].size()) {
            // std::cout << "=====left" << std::endl;
            // print_vector(left_key);
            // std::cout << left_forest[left_key]->intervals.size() << ' ' << record.t_start << ' ' << record.t_end << ' ' << durability << std::endl;
            intervalVector left_overlaps 
                = left_forest[left_key]->findOverlapping_with_durability(record.t_start, record.t_end, durability);
            // std::cout << "partial size: " << left_overlaps.size() << std::endl;
            if (left_overlaps.empty())
                continue;
            for (auto &item : left_overlaps) {
                item.start = MAX(item.start, record.t_start);
                item.stop = MIN(item.stop, record.t_end);
            }
            std::vector<join_result> partial_ans = interval_join(left_overlaps, right_forest[right_key], durability);
            for (auto &item : partial_ans) {
                // item.id.push_back(record.id[0]);
                item.attrs = std::vector<int>(this->total_num_attrs, UNDEF);
                std::vector<int> ids;
                for (int rid : left_table[item.id[0]].id) {
                    ids.push_back(rid);
                    for (auto& idx : join_attrs[0]) {
                        item.attrs[idx] = left_table[item.id[0]].attrs[idx];
                    }
                }
                for (int rid : right_table[item.id[1]].id) {
                    ids.push_back(rid);
                    for (auto& idx : join_attrs[2]) {
                        item.attrs[idx] = right_table[item.id[1]].attrs[idx];
                    }
                }
                for (int rid : record.id) {
                    ids.push_back(rid);
                }
                item.id = ids;
                item.attr_id = union_attrs;
                answer.emplace_back(item);
            }
        }
        else {
            // std::cout << "=====right" << std::endl;
            // print_vector(right_key);
            // std::cout << right_forest[right_key]->intervals.size() << ' ' << record.t_start << ' ' << record.t_end << ' ' << durability << std::endl;
            intervalVector right_overlaps
                = right_forest[right_key]->findOverlapping_with_durability(record.t_start, record.t_end, durability);
            // std::cout << "partial size: " << right_overlaps.size() << std::endl;
            if (right_overlaps.empty())
                continue;
            for (auto &item : right_overlaps) {
                item.start = MAX(item.start, record.t_start);
                item.stop = MIN(item.stop, record.t_end);
            }
            std::vector<join_result> partial_ans = interval_join(right_overlaps, left_forest[left_key], durability);
            for (auto &item : partial_ans) {
                // item.id.push_back(record.id[0]);
                item.attrs = std::vector<int>(this->total_num_attrs, UNDEF);
                std::vector<int> ids;
                for (int rid : right_table[item.id[0]].id) {
                    ids.push_back(rid);
                    for (auto& idx : join_attrs[2]) {
                        item.attrs[idx] = right_table[item.id[0]].attrs[idx];
                    }
                }
                for (int rid : left_table[item.id[1]].id) {
                    ids.push_back(rid);
                    for (auto& idx : join_attrs[0]) {
                        item.attrs[idx] = left_table[item.id[1]].attrs[idx];
                    }
                }
                for (int rid : record.id)
                    ids.push_back(rid);
                item.id = ids;
                item.attr_id = union_attrs;
                answer.emplace_back(item);
            }
        }
    }
    te = clock();
    query_time = te - ts;
    std::cout << "query time: " << (double) query_time / CLOCKS_PER_SEC << std::endl; 
    std::cout << "total time usage: " << (double) (query_time + index_time) / CLOCKS_PER_SEC << std::endl;
    return answer;
}

std::vector<join_result> Solution::line_k_join(std::map<int, std::vector<join_result>>& join_tables, std::map<int, std::vector<int>>& join_attrs, int durability) {
    assert (join_tables.size() >= 3);

    int table_size = join_tables.size() / 3;

    std::vector<join_result> answer;

    std::map<int, std::vector<join_result>> left_part, middle_part, right_part;
    std::map<int, std::vector<int>> left_part_attrs, middle_part_attrs, right_part_attrs;
    std::vector<int> left_join_order, middle_join_order, right_join_order;
    int idx = 0;
    for (; idx < table_size; ++idx)
        left_part[idx] = join_tables[idx], left_part_attrs[idx] = join_attrs[idx], left_join_order.push_back(idx);
    for (; idx < 2 * table_size; ++idx)
        middle_part[idx - table_size] = join_tables[idx], middle_part_attrs[idx - table_size] = join_attrs[idx], middle_join_order.push_back(idx - table_size);
    for (; idx < join_tables.size(); ++idx)
        right_part[idx - 2*table_size] = join_tables[idx], right_part_attrs[idx - 2*table_size] = join_attrs[idx], right_join_order.push_back(idx - 2*table_size);
    assert (left_part.size() + middle_part.size() + right_part.size() == join_tables.size());

    clock_t ts, te;
    ts = clock();
    std::vector<join_result> left_table, middle_table, right_table; 
    if (left_part.size() == 1)
        left_table = left_part.begin()->second;
    // else if (left_part.size() == 3) {
    //     left_table = line_3_join(left_part[0], left_part[1], left_part[2], left_part_attrs, durability);
    // }
    else
        left_table = multiway_durable_join_baseline(left_part, left_join_order, left_part_attrs, durability, -1);
    if (middle_part.size() == 1)
        middle_table = middle_part.begin()->second;
    // else if (middle_part.size() == 3) {
    //     middle_table = line_3_join(middle_part[0], middle_part[1], middle_part[2], middle_part_attrs, durability);
    // }
    else
        middle_table = multiway_durable_join_baseline(middle_part, middle_join_order, middle_part_attrs, durability, -1);
    if (right_part.size() == 1)
        right_table = right_part.begin()->second;
    // else if (right_part.size() == 3) {
    //     right_table = line_3_join(right_part[0], right_part[1], right_part[2], right_part_attrs, durability);
    // }
    else
        right_table = multiway_durable_join_baseline(right_part, right_join_order, right_part_attrs, durability, -1);
    // std::cout << left_table.size() << ' ' << middle_table.size() << ' ' << right_table.size() << std::endl;
    if (!left_table.empty() && !middle_table.empty() && !right_table.empty()) {
        std::map<int, std::vector<int>> combined_join_attrs;
        combined_join_attrs[0] = left_table[0].attr_id;
        combined_join_attrs[1] = middle_table[0].attr_id;
        combined_join_attrs[2] = right_table[0].attr_id;
        // print_vector(combined_join_attrs[0]);
        // print_vector(combined_join_attrs[1]);
        // print_vector(combined_join_attrs[2]);
        answer = line_3_join(left_table, middle_table, right_table, combined_join_attrs, durability);
    }
    te = clock();
    std::cout << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << std::endl;

    return answer;
}

int Solution::line_k_join_counting(std::map<int, std::vector<join_result>>& join_tables, std::map<int, std::vector<int>>& join_attrs, int durability) {
    assert (join_tables.size() >= 3);

    int table_size = join_tables.size() / 3;

    int answer = 0;

    std::map<int, std::vector<join_result>> left_part, middle_part, right_part;
    std::map<int, std::vector<int>> left_part_attrs, middle_part_attrs, right_part_attrs;
    std::vector<int> left_join_order, middle_join_order, right_join_order;
    int idx = 0;
    for (; idx < table_size; ++idx)
        left_part[idx] = join_tables[idx], left_part_attrs[idx] = join_attrs[idx], left_join_order.push_back(idx);
    for (; idx < 2 * table_size; ++idx)
        middle_part[idx - table_size] = join_tables[idx], middle_part_attrs[idx - table_size] = join_attrs[idx], middle_join_order.push_back(idx - table_size);
    for (; idx < join_tables.size(); ++idx)
        right_part[idx - 2*table_size] = join_tables[idx], right_part_attrs[idx - 2*table_size] = join_attrs[idx], right_join_order.push_back(idx - 2*table_size);
    assert (left_part.size() + middle_part.size() + right_part.size() == join_tables.size());

    clock_t ts, te;
    ts = clock();
    std::vector<join_result> left_table, middle_table, right_table; 
    if (left_part.size() == 1)
        left_table = left_part.begin()->second;
    // else if (left_part.size() == 3) {
    //     left_table = line_3_join(left_part[0], left_part[1], left_part[2], left_part_attrs, durability);
    // }
    else
        left_table = multiway_durable_join_baseline(left_part, left_join_order, left_part_attrs, durability);
    if (middle_part.size() == 1)
        middle_table = middle_part.begin()->second;
    // else if (middle_part.size() == 3) {
    //     middle_table = line_3_join(middle_part[0], middle_part[1], middle_part[2], middle_part_attrs, durability);
    // }
    else
        middle_table = multiway_durable_join_baseline(middle_part, middle_join_order, middle_part_attrs, durability);
    if (right_part.size() == 1)
        right_table = right_part.begin()->second;
    // else if (right_part.size() == 3) {
    //     right_table = line_3_join(right_part[0], right_part[1], right_part[2], right_part_attrs, durability);
    // }
    else
        right_table = multiway_durable_join_baseline(right_part, right_join_order, right_part_attrs, durability);
    // std::cout << left_table.size() << ' ' << middle_table.size() << ' ' << right_table.size() << std::endl;
    if (!left_table.empty() && !middle_table.empty() && !right_table.empty()) {
        std::map<int, std::vector<int>> combined_join_attrs;
        combined_join_attrs[0] = left_table[0].attr_id;
        combined_join_attrs[1] = middle_table[0].attr_id;
        combined_join_attrs[2] = right_table[0].attr_id;
        // print_vector(combined_join_attrs[0]);
        // print_vector(combined_join_attrs[1]);
        // print_vector(combined_join_attrs[2]);
        answer += line_3_join_counting(left_table, middle_table, right_table, combined_join_attrs, durability);
    }
    te = clock();
    std::cout << "time usage: " << (double) (te - ts) / CLOCKS_PER_SEC << std::endl;

    return answer;
}

void Solution::setup_table_index(std::vector<int>& join_order, std::map<int, std::vector<join_result>>& join_tables) {
    std::cout << "building hash tables...";
    for (int table_id : join_order) {
        join_table_index[table_id] = std::unordered_map<std::vector<int>, std::vector<int>, VectorHasher>{};
        for (int idx=0; idx<join_tables[table_id].size(); ++idx) {
            join_table_index[table_id][join_tables[table_id][idx].attrs].push_back(idx);
        }
    }
    std::cout << "done!" << std::endl;
}

void Solution::build_level_hash_table(LevelHashMap* node, const join_result& record, std::vector<int>& join_attrs, int index) {
    if (node == nullptr || index >= join_attrs.size())
        return;
    int join_attr_idx = join_attrs[index];
    int value_key = record.attrs[join_attr_idx];
    node->join_attr_id = join_attr_idx;
    node->base_key.insert(value_key);
    if (node->children.find(value_key) == node->children.end()) {
        node->children[value_key] = new LevelHashMap();
    }
    build_level_hash_table(node->children[value_key], record, join_attrs, index+1);
}

void Solution::setup_hash_index(std::vector<int>& join_order, std::map<int, std::vector<int>>& join_attrs, std::map<int, std::vector<join_result>>& join_tables) {
    std::cout << "building hierarchical hash tables...";
    for (int table_id : join_order) {
        join_hash_index[table_id] = new LevelHashMap();
        for (auto& record : join_tables[table_id])
            build_level_hash_table(join_hash_index[table_id], record, join_attrs[table_id], 0);
    }
    std::cout << "done!" << std::endl;
}