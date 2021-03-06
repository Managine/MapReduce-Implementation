#pragma once

#include "mapreduce_spec.h"
#include "file_shard.h"
#include <map>
#include <math.h>
#include <stdio.h>
#include <vector>
#include <algorithm>
#include <grpc++/grpc++.h>
#include <grpc/support/log.h>
#include "masterworker.pb.h"
#include "masterworker.grpc.pb.h"

using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerCompletionQueue;
using grpc::Status;
using namespace grpc;
using namespace masterworker;

using namespace std;
/* CS6210_TASK: Handle all the bookkeeping that Master is supposed to do.
	This is probably the biggest task for this project, will test your understanding of map reduce */

class Master {
public:
    /* DON'T change the function signature of this constructor */
    Master(const MapReduceSpec&, const std::vector<FileShard>&);
    
    /* DON'T change this function's signature */
    bool run();
    
    ~Master() {
    }
    
    void sendInfo(string& address, int type, FileShard fs, vector<string>& filenames ) {
        stub_=MR::NewStub(CreateChannel(address, InsecureChannelCredentials()));
        TaskInfo query;
        query.set_tasktype(type);
        query.set_userid(userID);
        if (type == 0){
            query.set_filename(fs.fileName);
            query.set_offset(fs.offset);
            query.set_filename2(fs.fileName2);
            query.set_size(fs.size);
        }
        else{
            query.set_outputdir(outputDIR);
            for(int i = 0; i < filenames.size(); i++){
                ReduceInfo* info = query.add_reduceinfos();
                info-> set_filename(filenames[i]);
            }
        }
        
        AsyncClientCall* call = new AsyncClientCall;
        call->response_reader = stub_->AsyncsendTask(&call->context, query, &cq);
        call->response_reader->Finish(&call->reply, &call->status, (void*)call);
    }
    
    TaskReply getInfo(bool* error) {
        void* got_tag;
        bool ok = false;
        TaskReply res;
        
        
        cq.Next(&got_tag, &ok);
        AsyncClientCall* call = static_cast<AsyncClientCall*>(got_tag);
        GPR_ASSERT(ok);
        if (call->status.ok()){
            *error = 0;
            res = call->reply;
        }
        
        else{
            std::cerr << "RPC failed" << std::endl;
            *error = 1;
        }
        
        delete call;
        
        return res;
    }
    
private:
    /* NOW you can add below, data members and member functions as per the need of your implementation*/
    vector<int> worker_aval;
    vector<FileShard> map_jobs;
	vector<vector<string>> red_jobs;
    int* map_job_status; //1 is finished, 0 is unfinished, 2 is sent but don't know status yet
	int* red_job_status;
	int* map_job_assign;
	int* red_job_assign;

    vector<string> worker_addr;
    string userID;
    string outputDIR;
    int outputfiles;

    CompletionQueue cq;
    std::unique_ptr<MR::Stub> stub_;
    
    struct AsyncClientCall {
        TaskReply reply;
        ClientContext context;
        Status status;
        std::unique_ptr<ClientAsyncResponseReader<TaskReply>> response_reader;
    };
};


/* CS6210_TASK: This is all the information your master will get from the framework.
	You can populate your other class data members here if you want */
Master::Master(const MapReduceSpec& mr_spec, const std::vector<FileShard>& file_shards) {
    for (int i = 0; i < mr_spec.num_workers; i++)
        worker_aval.push_back(1);
    
    map_jobs = file_shards;
	map_job_status=new int[file_shards.size()];
    for (int i = 0; i < file_shards.size(); i++){
        map_job_status[i] = 0;
    }


    
    worker_addr = mr_spec.worker_ipaddr;
    
    userID = mr_spec.user_id;
    outputDIR = mr_spec.output_dir;
    outputfiles = mr_spec.output_files;
}


/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {
    int map_finish_jobs = 0;
    int red_finish_jobs = 0;
    int map_fail_jobs = 0;
    int red_fail_jobs = 0;
    int total_map_jobs = map_jobs.size();
    int total_red_jobs = outputfiles;
    
    map<char,vector<string>> files;
	map_job_assign = new int[worker_aval.size()];
	red_job_assign = new int[worker_aval.size()];
	for (int i=0;i<worker_aval.size();i++) {
		map_job_assign[i]=-1;
		red_job_assign[i]=-1;
	}
    while(map_finish_jobs != total_map_jobs){
        //find an available worker
        int current_worker_id = -1;
        for(int i = 0; i < worker_aval.size(); i++){
            if (worker_aval[i] == 1){
                current_worker_id = i;
                break;
            }
        }
        
        //find an available job
        int current_job_id = -1;      
        if(map_finish_jobs + map_fail_jobs == total_map_jobs){
        	for(int i = 0; i < total_map_jobs; i++){
	            if (map_job_status[i] == 2){
	                current_job_id = i;
	                break;
	            }
        	}
			map_fail_jobs--;
        }
        else{
        	for(int i = 0; i < total_map_jobs; i++){
	            if (map_job_status[i] == 0){
	                current_job_id = i;
	                break;
            	}
        	}
        }
        //map: send request
        if (current_worker_id != -1 && current_job_id != -1) {
			vector<string> v;
			map_job_assign[current_worker_id] = current_job_id;
			map_job_status[current_job_id] = 2;
            sendInfo(worker_addr[current_worker_id], 0, map_jobs[current_job_id], v);
			worker_aval[current_worker_id] = 0;
			continue;
		}
        //map: get replies
        TaskReply reply;
        bool rpc_error = 0;
        reply = getInfo(&rpc_error);
        int finish_worker_id;
        int finish_job_id;
        if(rpc_error == 0){
            //find corrent worker id, change it to available
            for (int i = 0; i < worker_addr.size(); i++){
                if (worker_addr[i] == reply.workerid()){
                    finish_worker_id = i;
                    break;
                }
            }
 
            worker_aval[finish_worker_id] = 1;
            finish_job_id = map_job_assign[finish_worker_id];
			map_job_assign[finish_worker_id]=-1;
            map_job_status[finish_job_id] = 1;
			
            for(int i = 0; i < reply.replyinfos_size(); i++){
				string res = reply.replyinfos(i).filename();
				vector<string>& temp = files[*(res.end()-1)];
				vector<string>::iterator it = find(temp.begin(), temp.end(), res);
				
				if( it == temp.end())
                	temp.push_back(res);
            }
			
            
            map_finish_jobs ++;
        }
        else{
        	map_fail_jobs ++;
        }
    }
    
    // reduce: send request
    int alphab_offset = 0;
    int jobs_per_worker = 26 / outputfiles;
    
	//assign all reduce jobs
	for (int i = 0; i < outputfiles; i++){
		vector<string> FNames;
		for(int j = 0; j < jobs_per_worker+(i<26%outputfiles? 1:0); j++){
		    if (j + alphab_offset > 26){   	
				break;
	    	}
	     	else
	            FNames.insert(FNames.end(), files['a'+j+alphab_offset].begin(), files['a'+j+alphab_offset].end());
	   	}
	    alphab_offset += jobs_per_worker+(i<26%outputfiles? 1:0);
		vector<string> t(FNames);
		red_jobs.push_back(t);
	}

	red_job_status= new int[red_jobs.size()];
    for (int i = 0; i < red_jobs.size(); i++){
        red_job_status[i] = 0;
    }

    while(red_finish_jobs != total_red_jobs){
        //find an available worker
        int current_worker_id = -1;
        for(int i = 0; i < worker_aval.size(); i++){
            if (worker_aval[i] == 1){
                current_worker_id = i;
                break;
            }
        }
        
        //find an available job
        int current_job_id = -1;      
        if(red_finish_jobs + red_fail_jobs == total_red_jobs){
        	for(int i = 0; i < total_red_jobs; i++){
	            if (red_job_status[i] == 2){
	                current_job_id = i;
	                break;
	            }
        	}
			red_fail_jobs--;
        }
        else{
        	for(int i = 0; i < total_red_jobs; i++){
	            if (red_job_status[i] == 0){
	                current_job_id = i;
	                break;
            	}
        	}
        }

        //reduce: send request
        if (current_worker_id != -1 && current_job_id != -1) {
			FileShard f("t");
			red_job_assign[current_worker_id] = current_job_id;

            sendInfo(worker_addr[current_worker_id], 1, f, red_jobs[current_job_id]);
			worker_aval[current_worker_id] = 0;
			red_job_status[current_job_id] = 2;
			continue;
		}

        //reduce: get replies
        int finish_worker_id;
        int finish_job_id;
        TaskReply reply;
        bool rpc_error = 0;
        reply = getInfo(&rpc_error);
        if(rpc_error == 0){
            //find corrent worker id, change it to available
            for (int i = 0; i < worker_addr.size(); i++){
                if (worker_addr[i] == reply.workerid()){
                    finish_worker_id = i;
                    break;
                }
            }

            worker_aval[finish_worker_id] = 1;
            finish_job_id = red_job_assign[finish_worker_id];
			red_job_assign[finish_worker_id]=-1;
            red_job_status[finish_job_id] = 1;

            red_finish_jobs ++;
            
            for (auto name:red_jobs[finish_job_id])
                remove(name);
        } else {
			red_fail_jobs ++;
		}
    }
	return true;
}
