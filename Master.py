import socket
import sys
import json
import numpy as np
from threading import Thread, Lock
import time

jobs = {};
map_ = [];
reduce_ = [];
conf = None;

algo = None;

# MASTER <--> CLIENT (thread 1)
# thread 1
# master <-- client (job)
def accept_jobs(lock_mr):
	s = socket.socket();
	s.bind(('localhost', 5000));
	s.listen();

	while True:
		c, addr = s.accept();
		job = json.loads(c.recv(1024).decode());

		start = time.time();
		f = open('arrival_times.txt', 'a');
		f.write(job['job_id'] + ',' + str(start) + '\n');
		f.close();

		##################
		lock_mr.acquire();
		jobs[job['job_id']] = {'M':len(job['map_tasks']), 'R':job['reduce_tasks']}
		map_.extend(job['map_tasks']);
		lock_mr.release();
		##################

#--- Done ---#

# MASTER <--> WORKER (thread 2)
# master <-- worker (ack)
def check_updates(lock_conf, lock_mr):
	s = socket.socket();
	s.bind(('localhost', 5001));
	s.listen();

	while True:
		c, addr = s.accept();
		id_, job_done = c.recv(1024).decode().split('*');
		id_ = int(id_);
		job_done = json.loads(job_done);
		job_id, tsk = job_done['task_id'].split('_');

		lock_mr.acquire();
		# print("JOBS:", int(job_id), jobs, tsk[0]);
		if job_id in jobs:
			jobs[job_id][tsk[0]] -= 1;
			if jobs[job_id]['M'] == 0:
				reduce_.extend(jobs[job_id]['R']);
				jobs.pop(job_id);
		lock_mr.release();

		for i, c in enumerate(conf['workers']):
			if id_ == c['worker_id']:
				lock_conf.acquire();
				conf['workers'][i]['slots']+=1;
				lock_conf.release();
				break;
		print("GOT --> ", job_done);


# thread 3
# master --> worker (job)
def send_job_to_worker(worker_details, job):
	# worker_details --> {id, slots, port}
	s = socket.socket();
	s.connect(('localhost', worker_details['port']));
	# give only job to worker
	s.send(json.dumps(job).encode());

# thread for task scheduling
def scheduler(lock_mr, lock_conf):
	while True:
		if map_ != []:
			worker_idx = algo(conf);

			lock_mr.acquire();
			m = map_.pop();
			lock_mr.release();

			lock_conf.acquire();
			conf['workers'][worker_idx]['slots']-=1;
			lock_conf.release();

			print("MAP Sending --> ", conf['workers'][worker_idx], " Job --> ", m);
			send_job_to_worker(conf['workers'][worker_idx], m);

		if reduce_ != []:
			worker_idx = algo(conf);

			lock_mr.acquire();
			r = reduce_.pop();				
			lock_mr.release();

			lock_conf.acquire();
			conf['workers'][worker_idx]['slots']-=1;
			lock_conf.release();

			print("REDUCE Sending --> ", conf['workers'][worker_idx], " Job --> ", r);
			send_job_to_worker(conf['workers'][worker_idx], r);

#Thread 4
def logs_state_of_workers():
	t = 0.0;
	f = open('logs_state.txt', 'a');
	while True:
		x = json.dumps(conf);
		f.write(str(t) + '*' +x + '\n');
		time.sleep(0.25);
		t+=0.25;
	f.close();

# Scheduling Algorithms
# Send worker_idx of free sloted worker
def RANDOM(conf):
	while True:
		rand_worker_idx = np.random.choice([0,1,2]);
		if conf['workers'][rand_worker_idx]['slots'] > 0:
			return rand_worker_idx;
	return -1;

cur = 0;
def RR(conf):
	global cur;
	while True:
		if conf['workers'][cur]['slots'] > 0:
			h = cur;
			cur = (cur+1)%3;	
			return h;
		cur = (cur+1)%3;

def LL(conf):
	while True:
		m = 0; idx = -1;
		for rand_worker_idx in range(3):
			slots = conf['workers'][rand_worker_idx]['slots'];
			if slots > m:
				m = slots;
				idx = rand_worker_idx;
		if(m == 0): time.sleep(1);
		else: return idx;

if __name__ == '__main__':
	conf, scheduling_algo = sys.argv[1:];
	f = open(conf, 'r');
	# scheduling_algo = 'RR'
	# f = open('config.json', 'r');

	conf = json.load(f);
	algo = globals()[scheduling_algo];	

	conf['workers'] = sorted(conf['workers'], key = lambda x: x['worker_id'])

	
	lock_mr = Lock();
	lock_conf = Lock();

	t1 = Thread(target = accept_jobs, args = (lock_mr,));
	t2 = Thread(target = check_updates, args = (lock_conf, lock_mr));
	t3 = Thread(target = scheduler, args = (lock_mr, lock_conf));
	t4 = Thread(target = logs_state_of_workers)

	t1.start();
	t2.start();
	t3.start();
	t4.start();

	t1.join();
	t2.join();
	t3.join();
	t4.join();

	# print(jobs);

#python Master.py config.json RANDOM
#python request.py 10
#python Worker.py 4000 1

# thread 1 and 3 --> map_ and reduce_ common
# thread 2 and 3 --> conf common

# %HOMEDRIVE%%HOMEPATH%
