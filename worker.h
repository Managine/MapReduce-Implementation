#pragma once

#include <mr_task_factory.h>
#include "mr_tasks.h"
#include <fstream>
#include <string>
#include <vector>
#include "masterworker.grpc.pb.h"
#include "masterworker.pb.h"
#include <grpc++/grpc++.h>

using namespace masterworker;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

/* CS6210_TASK: Handle all the task a Worker is supposed to do.
	This is a big task for this project, will test your understanding of map reduce */

extern std::shared_ptr<BaseMapper> get_mapper_from_task_factory(const std::string& user_id);
extern std::shared_ptr<BaseReducer> get_reducer_from_task_factory(const std::string& user_id);

class Worker {

	public:
		/* DON'T change the function signature of this constructor */
		Worker(std::string ip_addr_port);

		/* DON'T change this function's signature */
		bool run();

	private:
		/* NOW you can add below, data members and member functions as per the need of your implementation*/
        std::string server_address;

		class MRService final : public MR::Service {
		
		public:
			MRService(const std::string address): server_address(address) {}
		
		private:
			Status sendTask(ServerContext* context, const TaskInfo* request,
				                 TaskReply* reply) override {
				int type = request->tasktype();
				if (type == 0) {    //map
				    auto mapper = get_mapper_from_task_factory(request->userid());
				    
				    std::string fileName = request->filename();
				    int offset = request->offset();
				    std::fstream file(fileName, std::fstream::in);
				    file.seekg(offset);
				    
				    char tmp[256];
				    while (file.tellg()<offset+request->size() && !file.eof()) {
				        file.getline(tmp, 256);
				        mapper->map(tmp);
				    }
				    int passedSize = (int)file.tellg()-offset;
				    file.close();
				    
				    if (request->filename2().size()!=0) {
				        fileName = request->filename2();
				        std::fstream file(fileName, std::fstream::in);
				        file.seekg(0);
				        
				        while (file.tellg()<request->size()-passedSize && !file.eof()) {
				            file.getline(tmp, 256);
				            mapper->map(tmp);
				        }
				    }
				    mapper->impl_->writeToFile(server_address);
				    
				    reply->set_workerid(server_address);
				    std::vector<std::string>& names= mapper->impl_->fileNames;
				    for (auto name:names) {
				        TaskReplyInfo* info = reply->add_replyinfos();
				        info->set_filename(name);
				    }
				    
				} else {    //reduce
				    std::map<std::string, std::vector<std::string>> map;
				    int n = request->reduceinfos_size();
				    std::string key;
				    std::string val;
					std::string returnName="";
				    for (int i=0;i<n;i++) {
				        std::string name = request->reduceinfos(i).filename();
				        std::fstream file(name, std::fstream::in);
						char c=*(name.end()-1);
						if (returnName.find(c)==std::string::npos)
							returnName+=c;
						int line=0;
				        while (file >> key) {
							line++;
							file >> val;
				            map[key].push_back(val);
				        }
				    }
				    
				    auto reducer = get_reducer_from_task_factory(request->userid());
				    for (std::map<std::string, std::vector<std::string>>::iterator it=map.begin(); it!=map.end(); ++it) {
				        reducer->reduce(it->first, it->second);
					}

				    reducer->impl_->writeToFile(request->outputdir()+"//"+returnName);
					reply->set_workerid(server_address);
				}
				return Status::OK;
			}
		
			std::string server_address;
		};
};


/* CS6210_TASK: ip_addr_port is the only information you get when started.
	You can populate your other class data members here if you want */
Worker::Worker(std::string ip_addr_port) {
    server_address = ip_addr_port;
}

/* CS6210_TASK: Here you go. once this function is called your woker's job is to keep looking for new tasks 
	from Master, complete when given one and again keep looking for the next one.
	Note that you have the access to BaseMapper's member BaseMapperInternal impl_ and 
	BaseReduer's member BaseReducerInternal impl_ directly, 
	so you can manipulate them however you want when running map/reduce tasks*/
bool Worker::run() {
	/*  Below 5 lines are just examples of how you will call map and reduce
		Remove them once you start writing your own logic */     
	MRService service(server_address);
    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cerr << "Worker listening on " << server_address << std::endl;
    server->Wait();
    return true;
}
