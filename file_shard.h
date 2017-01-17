#pragma once

#include <vector>
#include <fstream>
#include "mapreduce_spec.h"
#include <string>


/* CS6210_TASK: Create your own data structure here, where you can hold information about file splits,
     that your master would use for its own bookkeeping and to convey the tasks to the workers for mapping */
struct FileShard {
    std::string fileName;
    std::string fileName2;
    int offset;
    int size;
    
    FileShard(std::string name) {
        fileName=name;
        fileName2="";
    }
};


/* CS6210_TASK: Create fileshards from the list of input files, map_kilobytes etc. using mr_spec you populated  */ 
inline bool shard_files(const MapReduceSpec& mr_spec, std::vector<FileShard>& fileShards) {    
	try {
        for (auto fileName : mr_spec.inputFileName) {
            std::fstream file(fileName, std::fstream::in);
            file.seekg(0, file.end);
            int length = file.tellg();
            file.seekg(0, file.beg);
            int offset = 0;        

            if (fileShards.size()!=0 && fileShards.back().size<mr_spec.map_kilobyte*1024) {
				offset += mr_spec.map_kilobyte*1024 - fileShards.back().size;
				fileShards.back().fileName2 = fileName;
				if (offset<length) {
		            file.seekg(offset);
		            while (file.tellg()!=length && file.get()!='\n')
		                offset++;
		            fileShards.back().size+=offset;
		            offset++;
				} else {
					fileShards.back().size+=length;
				}
            }
            while (offset + mr_spec.map_kilobyte*1024<length) {
                FileShard shard(fileName);
                shard.offset=offset;
				offset += mr_spec.map_kilobyte*1024;
                file.seekg(offset);
                while (file.tellg()!=length && file.get()!='\n')
                    offset++;
                offset++;
                shard.size=offset-shard.offset-1;
                fileShards.push_back(shard);
            }
            FileShard shard(fileName);
			shard.size=length-offset;
			if (shard.size>0) {
		        shard.offset=offset;
		        fileShards.push_back(shard);
			}
            file.close();
        }
    } catch (...) {
        return false;
    }
	return true;
}
