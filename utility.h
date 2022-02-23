#ifndef UTILITY_H
#define UTILITY_H

#include <ctime>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#define MIN(A, B) (((A)<(B))?(A):(B))
#define MAX(A, B) (((A)>(B))?(A):(B))
#define UNDEF -1
#define INT_MAX 2147483647
#define INT_MIN -2147483648
#define TIME_ORIGIN 1572393600

class CSVRow {
    public:
    friend std::istream& operator>>(std::istream& str, CSVRow& data) {
        data.readNextRow(str);
        return str;
    }  
        std::string const& operator[](std::size_t index) const {
            return m_data[index];
        }
        std::size_t size() const {
            return m_data.size();
        }
        void readNextRow(std::istream& str) {
            std::string         line;
            std::getline(str, line);

            std::stringstream   lineStream(line);
            std::string         cell;

            m_data.clear();
            while(std::getline(lineStream, cell, ','))
            {
                m_data.push_back(cell);
            }
        }
    private:
        std::vector<std::string> m_data;
};

std::time_t getEpochTime(const std::string& dateTime);
void print_vector(std::vector<int> const& value);
std::vector<int> get_union(std::vector<int>& v1, std::vector<int>& v2);
std::vector<int> get_intersection(std::vector<int>& v1, std::vector<int>& v2);
bool is_subset(std::vector<int>& a, std::vector<int>& b);
bool vector_equal(std::vector<int>& a, std::vector<int>& b, std::vector<int> compare_idx);
bool vector_greater(std::vector<int>& a, std::vector<int>& b, std::vector<int> compare_idx);
int compare(std::vector<int> const& a, std::vector<int> const& b, std::vector<int>& idx);
bool tuple_equal (std::tuple<int,int,int> const& a, std::tuple<int,int,int> const& b);
void print_tuple (std::tuple<int,int,int> const& t);

#endif