import json;
import matplotlib.pyplot as plt 
import numpy as np 

def mean(lis):
	s = 0;
	for x in lis: s+=x;
	return s/len(lis);

def median(lis):
	lis.sort();
	n = len(lis);
	m = int(n/2);
	if n%2 == 0: return (lis[m]+lis[m-1])/2;
	else: return lis[m];

# part 1 --tsk-1-- --Done--
def task_completion_time(arr):
	dic={}
	for x in arr:
		lis = x.strip().split(',');
		if lis[0] not in dic:
			dic[lis[0]] = {lis[1]: [float(lis[2])]};
		else:
			if lis[1] not in dic[lis[0]]:
				dic[lis[0]][lis[1]] = [float(lis[2])];
			else:
				dic[lis[0]][lis[1]].append(float(lis[2]));

	for k in dic:
		print(dic[k]);
		print();

	d = {}
	for k in dic:
		i = 0; lis = [];
		for kd in dic[k]:
			if(len(dic[k][kd]) != 2): break;
			else: lis.append(abs(dic[k][kd][1] - dic[k][kd][0]));
		d[k] = lis;

	for k in sorted(d.keys()):
		print("Worker:", k);
		print("Mean:", mean(d[k]), end = ' ');
		print("Median:", median(d[k]));
		print()


# part 1 --tsk-2-- --Done--
def job_completion_time(lt, at):
	lis = [];
	print("Job Completion Time")
	for at_line in at:
		jid, tat = at_line.strip().split(',');
		for lt_line in lt[::-1]:
			worker_id, task, tlt = lt_line.strip().split(',');
			prefix = jid+'_R';
			if prefix == task[:len(prefix)]:
				lis.append(float(tlt) - float(tat))
				print(jid,":", float(tlt) - float(tat));
				break;
	print()
	print("Mean Time:")
	print(mean(lis));

# part 2
def time_v_slots(arr, cf):
	y1 = [];
	y2 = [];
	y3 = [];
	x = [];
	for line in arr:
		t, conf = line.strip().split('*');
		conf = json.loads(conf);
		if(float(t) > 50): break;
		x.append(float(t));
		y1.append(cf['workers'][0]['slots'] - conf['workers'][0]['slots']);
		y2.append(cf['workers'][1]['slots'] - conf['workers'][1]['slots']);
		y3.append(cf['workers'][2]['slots'] - conf['workers'][2]['slots']);
	plt.xlabel('time');
	plt.ylabel('tasks');
	plt.plot(x, y1, label = "Worker " + str(cf['workers'][0]['worker_id']));
	plt.plot(x, y2, label = "Worker " + str(cf['workers'][1]['worker_id']));
	plt.plot(x, y3, label = "Worker " + str(cf['workers'][2]['worker_id']));
	plt.legend();
	plt.show();

if __name__ == '__main__':
	flt = open('log_task.txt', 'r');
	lt = flt.readlines();
	task_completion_time(lt);
	flt.close();

	fat = open('arrival_times.txt', 'r');
	at = fat.readlines();
	job_completion_time(lt, at);
	fat.close();

	fls = open('logs_state.txt', 'r');
	fc = open('config.json', 'r');
	conf = json.load(fc);
	ls = fls.readlines();
	time_v_slots(ls, conf);
	fc.close();
	fls.close();

