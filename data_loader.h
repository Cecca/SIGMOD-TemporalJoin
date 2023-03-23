#ifndef DATA_LOADER_H
#define DATA_LOADER_H

#include "utility.h"
#include "tables.h"
#include <exception>
#include <map>
#include <vector>
#include <string>
#include <fstream>
#include <sstream>
#include <iostream>
#include <algorithm> 
#include <assert.h>

class TableLoader {
public:
	TableLoader() {}
	~TableLoader() {
		data_tables.clear();
	}

    void load_taxi_table(const int id, const std::string filename, const std::map<int, std::string>& attrs, bool skip_header);

    void load_taxi_table_v2(const int id, const std::string filename, const std::map<int, std::string>& attrs, bool skip_header);

    void load_test_table(const int id, const std::string filename, int num_of_attrs);

    void load_test_table(const int id);
    
    void sort_by_attr(const int id, const std::map<int, std::string>& sorted_attrs);

    void sort_by_attr(const int id, const std::vector<int>& attrs);

    void data_viewer(const int id, const int limit);

    void data_viewer_test(const int id, const int limit);

    // prepare raw data to join tables
    std::vector<join_result> prepare(const int id, const std::vector<int>& attrs, int type);

    std::vector<join_result> prepare(const int id, const std::vector<int>& attrs, const std::vector<int>& attrs_id);

    std::vector<join_result> prepare(const int id, const int num_total_attrs, 
                            const std::vector<int>& attrs, std::vector<int>& attrs_id, int durability, bool shrink=false);

    std::vector<taxi_table_basic> get_table (const int id) {
        return data_tables[id];
    }
    
    std::vector<taxi_table_basic> get_table_index (const int id) {
        return data_table_index[id];
    }

    std::map<int, std::vector<taxi_table_basic>> get_db () {
        return data_tables;
    }

    std::map<int, std::vector<taxi_table_basic>> get_db_index() {
        return data_table_index;
    }
    
private:
    // for taxi data
	std::map<int, std::vector<taxi_table_basic>> data_tables;
    std::map<int, std::vector<std::string>> table_attrs;
    std::map<int, std::vector<taxi_table_basic>> data_table_index;
    // for general (test) data
    // std::map<int, std::vector<test_table>> test_tables;
    std::map<int, std::vector<test_table>> test_tables_index;
};

void TableLoader::data_viewer(const int id, const int limit) {
    for (int i=0; i<limit; ++i) {
        // std::cerr << data_table_index[id][i].pickup_id << ' ' << data_table_index[id][i].dropoff_id << std::endl;
        data_table_index[id][i].print();
    }
}

void TableLoader::data_viewer_test(const int id, const int limit) {
    for (int i=0; i<limit; ++i) {
        // std::cerr << test_tables_index[id][i].t_start << ' ' << test_tables_index[id][i].t_end << std::endl;
        test_tables_index[id][i].print();
    }
}

std::vector<join_result> TableLoader::prepare(const int id, const std::vector<int>& attrs, int type) {
    std::vector<join_result> table;
    // general data
    if (type == 0) {
        for (auto& item : test_tables_index[id]) {
            join_result record;
            record.id.push_back(item.row);
            for (auto& idx :  attrs)
                record.attrs.push_back(item.attrs[idx]);
            record.t_start = item.t_start;
            record.t_end = item.t_end;
            record.table_id = id;
            table.push_back(record);
        }
    }
    return table;
}

std::vector<join_result> TableLoader::prepare(const int id, const std::vector<int>& attrs, const std::vector<int>& attrs_id) {
    assert (attrs.size() == attrs_id.size());
    std::vector<join_result> table;
    for (auto& item : test_tables_index[id]) {
        join_result record;
        record.id.push_back(item.row);
        for (auto& idx :  attrs)
            record.attrs.push_back(item.attrs[idx]);
        record.t_start = item.t_start;
        record.t_end = item.t_end;
        record.table_id = id;
        record.attr_id = attrs_id;
        
        table.emplace_back(record);
    }
    return table;
}

std::vector<join_result> TableLoader::prepare(const int id, const int num_total_attrs, 
                                const std::vector<int>& attrs, std::vector<int>& attrs_id, int durability, bool shrink) {
    assert (attrs.size() == attrs_id.size());
    std::vector<join_result> table;
    for (auto& item : test_tables_index[id]) {
        // durability filter
        if (item.t_end - item.t_start + 1 < durability)
            continue;
        join_result record;
        record.t_start = item.t_start;
        record.t_end = shrink ? item.t_end - durability + 1 : item.t_end;
        record.id.push_back(item.row);
        record.attrs = std::vector<int>(num_total_attrs,UNDEF);
        for (int idx = 0; idx < attrs.size(); ++idx)
            record.attrs[attrs_id[idx]] = item.attrs[attrs[idx]];
        // print_vector(record.attrs);
        record.table_id = id;
        record.attr_id = attrs_id;
        // record the idx of this record in the filtered table
        record.idx = table.size();

        table.emplace_back(record);
    }

    return table;
}

void TableLoader::sort_by_attr(const int id, const std::vector<int>& sort_attrs) {
    // sort tables by join attributes
    std::sort(test_tables_index[id].begin(), test_tables_index[id].end(), 
                [&](test_table const& r1, test_table const& r2) -> bool {
                    for (auto const& idx : sort_attrs) {
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
    std::cerr << "--> data sorted." << std::endl;
}

void TableLoader::load_test_table(const int id) {
    std::cerr << "copying data from origin table..." << std::endl;
    test_tables_index[id] = test_tables_index[0];
    std::cerr << "total records: " << test_tables_index[id].size() << std::endl;
}

void TableLoader::load_test_table(const int id, const std::string filename, int num_of_attrs) {
    std::ifstream fin(filename);
    if (!fin) {
        std::cerr << "FILE ERROR" << std::endl;
        throw std::exception();
    }
    
    std::cerr << "load tables from " << filename << std::endl;

	std::string line;
	int cnt = 0;
    int exception_flag = 0;

    CSVRow row;
	while(fin >> row) {
        test_table record;
        record.row = cnt;
        for (int i = 0; i < num_of_attrs; ++i) {
            if (row[i].empty()) {
                exception_flag = 1;
                break;
            }
            record.attrs.push_back(std::stoi(row[i]));
        }
        if (exception_flag)
            break;
        record.t_start = std::stoi(row[num_of_attrs]);
        record.t_end = std::stoi(row[num_of_attrs+1]);
        record.table_id = id;

        test_tables_index[id].emplace_back(record);
        // if (cnt > 100000)
        //     break;
		// if (cnt % 1000 == 0) {
		// 	std::cerr << '\r' << cnt << std::flush;
        // }
        cnt++;
	}
	fin.close();
	std::cerr << "total records: " << test_tables_index[id].size() << "/" << cnt << std::endl;
}

void TableLoader::sort_by_attr(const int id, const std::map<int, std::string>& sorted_attrs) {
    // create a sorted data table from original data table
    for (auto& item : data_tables[id])
        data_table_index[id].push_back(item);
    
    // iteratively sorted by user-specified attr
    for (auto const& item : sorted_attrs) {
        
        std::cerr << "sort table by " << item.second << "; ";
        if (item.second == "pickup_id") {
            std::stable_sort(data_table_index[id].begin(), data_table_index[id].end(), 
                    [](taxi_table_basic const& r1, taxi_table_basic const& r2) -> bool {
                        return r1.pickup_id < r2.pickup_id;
            });
        }
        else if (item.second == "dropoff_id") {
            std::stable_sort(data_table_index[id].begin(), data_table_index[id].end(), 
                    [](taxi_table_basic const& r1, taxi_table_basic const& r2) -> bool {
                        return r1.dropoff_id < r2.dropoff_id;
            });
        }
        else {
            std::cerr << "should not be here" << std::endl;
        }
    }
    std::cerr << "--> data sorted." << std::endl;
}

void TableLoader::load_taxi_table(const int id, const std::string filename, const std::map<int, std::string>& attrs, bool skip_header) {
    std::ifstream fin(filename);
	if (!fin)
		std::cerr << "FILE ERROR" << std::endl;
    
    std::cerr << "load tables from " << filename << std::endl;

    for (auto const& item : attrs) 
        table_attrs[id].push_back(item.second);
	std::string line;
	int cnt = 0;
    int exception_flag = 0;

    CSVRow row;
	while(fin >> row) {
        if (cnt == 0 && skip_header == true) {
            cnt++;
            continue;
        }
        taxi_table_basic record;
        // record.row = cnt;
        record.set_row(cnt);
        for (auto const& item : attrs) {
            if (row[item.first].empty()) {
                exception_flag = 1;
                break;
            }
            if (item.second == "id") {
                //std::cerr << row[item.first] << std::endl;
                // record.id = std::stoi(row[item.first]);
                record.set_id(std::stoi(row[item.first]));
            } else if (item.second == "pickup_id") {
                //std::cerr << row[item.first] << std::endl;
                // record.pickup_id = std::stoi(row[item.first]);
                record.set_pickup_id(std::stoi(row[item.first]));
            } else if (item.second == "dropoff_id") {
                //std::cerr << row[item.first] << std::endl;
                // record.dropoff_id = std::stoi(row[item.first]);
                record.set_dropoff_id(std::stoi(row[item.first]));
            } else if (item.second == "t_start") {
                //std::cerr << int(getEpochTime(row[item.first])) << std::endl;
                // record.t_start = int(getEpochTime(row[item.first]));
                record.set_t_start(int(getEpochTime(row[item.first])));
            } else if (item.second == "t_end") {
                //std::cerr << int(getEpochTime(row[item.first])) << std::endl;
                // record.t_end = int(getEpochTime(row[item.first]));
                record.set_t_end(int(getEpochTime(row[item.first])));
            } else {
                std::cerr << "Never should be here!" << std::endl;
            }
        }
        if (exception_flag)
            break;
        data_tables[id].push_back(record);
		// if (cnt % 1000 == 0) {
		// 	std::cerr << '\r' << cnt << std::flush;
        // }
        cnt++;
	}
	fin.close();
	std::cerr << "total records: " << data_tables[id].size() << "/" << cnt << std::endl;
}

void TableLoader::load_taxi_table_v2(const int id, const std::string filename, const std::map<int, std::string>& attrs, bool skip_header) {
    std::ifstream fin(filename);
	if (!fin)
		std::cerr << "FILE ERROR" << std::endl;
    
    std::cerr << "load tables from " << filename << std::endl;

	std::string line;
	int cnt = 0;
    int exception_flag = 0;

    CSVRow row;
	while(fin >> row) {
        if (cnt == 0 && skip_header == true) {
            cnt++;
            continue;
        }
        test_table record;
        record.row = cnt;
        for (auto const& item : attrs) {
            if (row[item.first].empty()) {
                exception_flag = 1;
                break;
            }
            if (item.second == "id") {
                record.attrs.push_back(std::stoi(row[item.first]));
            } else if (item.second == "pickup_id") {
                record.attrs.push_back(std::stoi(row[item.first]));
            } else if (item.second == "dropoff_id") {
                record.attrs.push_back(std::stoi(row[item.first]));
            } else if (item.second == "t_start") {
                record.t_start = int(getEpochTime(row[item.first]));
            } else if (item.second == "t_end") {
                record.t_end = int(getEpochTime(row[item.first]));
            } else {
                std::cerr << "Never should be here!" << std::endl;
            }
        }
        record.table_id = id;
        if (exception_flag)
            break;
        test_tables_index[id].push_back(record);
		// if (cnt % 1000 == 0) {
		// 	std::cerr << '\r' << cnt << std::flush;
        // }
        cnt++;
	}
	fin.close();
	std::cerr << "total records: " << test_tables_index[id].size() << "/" << cnt << std::endl;
}

#endif
