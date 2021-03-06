#pragma once

#include <string>
#include <iostream>
#include <map>
#include <fstream>
#include <vector>

struct Pair {
    std::string key;
    std::string value;
    Pair(std::string k, std::string v) {
        key=k;
        value=v;
    }
};

/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the map task*/
struct BaseMapperInternal {

		/* DON'T change this function's signature */
		BaseMapperInternal();

		/* DON'T change this function's signature */
		void emit(const std::string& key, const std::string& val);

		/* NOW you can add below, data members and member functions as per the need of your implementation*/
    std::map<char, std::vector<Pair>> map;
    std::vector<std::string> fileNames;
    
    void writeToFile(const std::string& name) {
        for (char c='a';c<='z';c++) {
            std::fstream file;
            file.open(name+"_"+c, std::fstream::out | std::fstream::app);
            std::vector<Pair> v = map[c];
            if (v.size()==0)
                continue;
            fileNames.push_back(name+"_"+c);
            for (auto pair : v) {
                file<<pair.key<<" "<<pair.value<<std::endl;
            }
            file.close();
        }
    }
};


/* CS6210_TASK Implement this function */
inline BaseMapperInternal::BaseMapperInternal() {
    for (char c='a';c<='z';c++) {
        std::vector<Pair> vector;
        map.insert(std::pair<char, std::vector<Pair>&>(c, vector));
    }
}


/* CS6210_TASK Implement this function */
inline void BaseMapperInternal::emit(const std::string& key, const std::string& val) {
    Pair p(key, val);
    char c=key[0];
    if (c>='A' && c<='Z')
        c += 'a'-'A';
    map[c].push_back(p);
}


/*-----------------------------------------------------------------------------------------------*/


/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the reduce task*/
struct BaseReducerInternal {

		/* DON'T change this function's signature */
		BaseReducerInternal();

		/* DON'T change this function's signature */
		void emit(const std::string& key, const std::string& val);

		/* NOW you can add below, data members and member functions as per the need of your implementation*/
    std::vector<std::string> vector;
    
    void writeToFile(std::string fileName) {
        std::fstream file;
        file.open(fileName, std::fstream::out | std::fstream::trunc);
        for (auto content : vector) {
            file<<content<<std::endl;
        }
        file.close();
    }
};


/* CS6210_TASK Implement this function */
inline BaseReducerInternal::BaseReducerInternal() {

}


/* CS6210_TASK Implement this function */
inline void BaseReducerInternal::emit(const std::string& key, const std::string& val) {
    vector.push_back(key+" "+val);
}
