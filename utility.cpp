#include "utility.h"
#include <set>
#include <tuple>
#include <assert.h>
#include <string>
#include <vector>

std::time_t getEpochTime(const std::string& dateTime) {
    // date time format
    static const std::string dateTimeFormat{"%Y-%m-%d %H:%M:%S"};

    std::stringstream ss{dateTime};

    std::tm dt;

    ss >> std::get_time(&dt, dateTimeFormat.c_str());

    return std::mktime(&dt);
}

void print_vector(std::vector<int> const& value) {
    for (auto v : value)
        std::cerr << v << ',';
    std::cerr << std::endl;
}

std::vector<int> get_union(std::vector<int>& v1, std::vector<int>& v2) {
    std::set<int> all;
    for (auto v : v1)
        all.insert(v);
    for (auto v : v2)
        all.insert(v);
    std::vector<int> ans(all.begin(), all.end());
    // assert (ans.size() < v1.size() + v2.size());
    return ans;
}

std::vector<int> get_intersection(std::vector<int>& v1, std::vector<int>& v2) {
    std::vector<int> common;
    for (auto i : v1) {
        for (auto j : v2)
            if (i == j)
                common.push_back(i);
    }
    // assert (common.size() > 0);
    return common;
}

bool vector_equal(std::vector<int>& a, std::vector<int>& b, std::vector<int> compare_idx) {
    for (int idx : compare_idx) {
        if (a[idx] != b[idx])
            return false;
    }
    return true;
}

bool vector_greater(std::vector<int>& a, std::vector<int>& b, std::vector<int> compare_idx) {
    for (int idx : compare_idx) {
        if (a[idx] == b[idx])
            continue;
        else
            return a[idx] >= b[idx];
    }
    return true;
}

bool is_subset(std::vector<int>& a, std::vector<int>& b) {
    std::set<int> b_set(b.begin(), b.end());
    for (auto v : a) {
        if (b_set.find(v) != b_set.end())
            continue;
        else
            return false;
    }
    return true;
}

// compare vectors based on given idx
// if a > b, return -1
// if a < b, return 1
// if a == b, return 0
int compare(std::vector<int> const& a, std::vector<int> const& b, std::vector<int>& idx) {
    // assert (a.size() == b.size());
    for (int i : idx) {
        if (a[i] == b[i])
            continue;
        else if (a[i] < b[i])
            return 1;
        else
            return -1;
    }
    return 0;
}

bool tuple_equal (std::tuple<int,int,int> const& a, std::tuple<int,int,int> const& b) {
    if ((std::get<0>(a) == std::get<0>(b)) && 
        (std::get<1>(a) == std::get<1>(b)) && (std::get<2>(a) == std::get<2>(b)))
        return true;
    else
        return false;
}

void print_tuple (std::tuple<int,int,int> const& t) {
    std::cerr << std::get<0>(t) << ' ' << std::get<1>(t) << ' ' << std::get<2>(t) << std::endl;
}