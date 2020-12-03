import socket
import sys
import time
import json
from threading import Thread, Lock

# thread 1
# MASTER <--> WORKER
jobs = [];

def accept_jobs(port):
	s = socket.socket();
	s.bind(('localhost', int(port)));
	s.listen();

	while True:
		print("Listening...")
		c, addr = s.accept();
		job = json.loads(c.recv(1024).decode());

		start = time.time();
		f = open('log_task.txt', 'a');
		f.write(id_+','+job['task_id']+','+str(start)+'\n');
		f.close()

		jobs.append(job);

# thread 2
# send ack to master
def update_master(job):
	s = socket.socket();
	s.connect((('localhost', 5001)));
	job_message = json.dumps(job);
	ack = str(id_)+'*'+str(job_message);
	s.send(ack.encode());

# work for duration specified
def work(job):

	print();
	print("Job id:", job);
	print("...", job['duration'])
	time.sleep(job['duration']);
	print("Done...", job);

	end = time.time();
	f = open('log_task.txt', 'a');
	f.write(id_+','+job['task_id']+','+str(end)+'\n');
	f.close();

	update_master(job);


if __name__ == '__main__':
	port, id_ = sys.argv[1:];
	# port = '4000'
	# id_ = 1;

	listen_thread = Thread(target = accept_jobs, args = (port,));
	listen_thread.start();
	while True:
		if jobs != []:
			j = jobs.pop();
			Thread(target = work, args = (j,)).start();
