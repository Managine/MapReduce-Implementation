#pragma once

#include <string>
#include <stdio.h>
#include <vector>
#include <iostream>
#include <fstream>
using namespace std;
/* CS6210_TASK: Create your data structure here for storing spec from the config file */
struct MapReduceSpec {
    vector<string> inputFileName;
    int numberOfMapSplit = 0;
    int size = 0;
    int num_workers = 0;
    vector<std::string> worker_ipaddr;
    int output_files = 0;
    string output_dir;
    string user_id;
    int map_kilobyte = 0;
    
    
};


/* CS6210_TASK: Populate MapReduceSpec data structure with the specification from the config file */
inline bool read_mr_spec_from_config_file(const string& config_filename, MapReduceSpec& mr_spec) {
    ifstream configfile (config_filename);
    vector<string> val;
    if (configfile.is_open())
    {
        string line;
        while ( getline (configfile,line) )
        {
            int i = 0;
            while(i<line.size()){
                if (line[i] == '='){
                    val.push_back(line.substr(i+1));
                    break;
                }
				i++;
            }
        }
        configfile.close();
        
        //Get num_workers
        mr_spec.num_workers = stoi(val[0]);
        
        //Get workder ip address
        int i = 0;
        vector<int> pos;
		pos.push_back(-1);
        while(i < val[1].size()){
            if (val[1][i] == ','){
                pos.push_back(i);
            }
			i++;
        }
        for (i = 0; i < pos.size(); i++){
            mr_spec.worker_ipaddr.push_back(val[1].substr(pos[i]+1, 15));
			
        }   

        //Get input files
        i = 0;
        pos.clear();
		pos.push_back(-1);
        while(i < val[2].size()){
            if (val[2][i] == ','){
                pos.push_back(i);
            }
			i++;
        }
		pos.push_back(val[2].size());
        for (i = 0; i < pos.size()-1; i++){
            mr_spec.inputFileName.push_back(val[2].substr(pos[i]+1, pos[i+1]-pos[i]-1));
        }
        
        //Get output Directory
        mr_spec.output_dir = val[3];
        
        //Get Number of output files
        mr_spec.output_files = stoi(val[4]);
        
        //Get Number of split maps
        mr_spec.map_kilobyte = stoi(val[5]);
        
        //Get user ID
        mr_spec.user_id = val[6];
    }
    else{
        return false;
    }
    return true;
}


/* CS6210_TASK: validate the specification read from the config file */
inline bool validate_mr_spec(const MapReduceSpec& mr_spec) {
    if(mr_spec.num_workers < 0){
        return false;
    }
    for (int i = 0; i < mr_spec.inputFileName.size(); i++){
		fstream file(mr_spec.inputFileName[i]);
        if(!file.is_open())
            return false;
		file.close();
    }
    return true;
}
