#!/usr/bin/python3

import findspark
findspark.init()
import time
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SQLContext
import sys
import requests
import json

def helper(event):
	id_ = event['playerId'];
	keys = ['accurate_key','key','accurate_normal','normal',\
			'duel_neutral','duel_won','duel', \
			'free_kick_goal','free_kick_eff','free_kick', \
			'shot_goal','shot_not_goal','shot', \
			'foul', 'own_goal'];
	c = [];
	for k in keys:
		c.append(((id_, k), 0));

	if event['eventId'] == 8:
		a = 0; k = 0;
		for x in event['tags']:
			if(x['id'] == 1801): a = 1;
			if(x['id'] == 302): k = 1;
		if k:
			if a:
				c.append(((id_, 'accurate_key'), 1));
			c.append(((id_, 'key'), 1));
		else: 
			if a:
				c.append(((id_, 'accurate_normal'), 1));
			c.append(((id_, 'normal'), 1));
		return c;

	elif event['eventId'] == 1:
		status = 0;
		for x in event['tags']:
			if(x['id'] == 702): status = 1;break;
			if(x['id'] == 703): status = 2;break;
		if status == 1: c.append(((id_, 'duel_neutral'), 1));
		if status == 2: c.append(((id_, 'duel_won'), 1));
		c.append(((id_, 'duel'), 1));

	elif event['eventId'] == 3:
		g = 0; eff = 0;
		for x in event['tags']:
			if(x['id'] == 1801): eff = 1;
			if(x['id'] == 101): g = 1;
		if event['subEventId'] == 35 and g: c.append(((id_, 'free_kick_goal'), 1));
		elif eff: c.append(((id_, 'free_kick_eff'), 1));
		c.append(((id_, 'free_kick'), 1));

	elif event['eventId'] == 10:
		g = 0; k = 0;
		for x in event['tags']:
			if(x['id'] == 1801): k = 1;
			if(x['id'] == 101): g = 1;
		if k and g: c.append(((id_, 'shot_goal'), 1))
		elif k: c.append(((id_, 'shot_not_goal'), 1))
		c.append(((id_, 'shot'), 1))

	elif event['eventId'] == 2: c.append(((id_, 'foul'), 1));

	for x in event['tags']:
		if x['id'] == 102: c.append(((id_, 'own_goal'), 1));

	return c;

def make_profile(match, x):
	id_ = x[0];
	prop = x[1];
	rating = 0;
	
	pass_accuracy = duel_eff = fk_eff = shot_eff = 0;

	passes = (prop['normal'] + 2 * prop['key']);
	if(passes): pass_accuracy = (prop['accurate_normal'] + 2 * prop['accurate_key'])/passes;
	
	if(prop['duel']): duel_eff = (prop['duel_won'] + 0.5 * prop['duel_neutral'])/prop['duel'];
	
	if(prop['free_kick']): fk_eff = (prop['free_kick_eff'] + prop['free_kick_goal'])/prop['free_kick'];
	
	if(prop['shot']): shot_eff = (prop['shot_not_goal'] + 0.5 * prop['shot_goal'])/prop['shot'];
	
	shot_on_target = prop['shot_not_goal'] + prop['shot_goal'];

	rate = 1.05;
	for i, team in enumerate(match['teamsData']):
		subs = match['teamsData'][team]['formation']['substitutions'];
		for s in subs:
			if s['playerIn'] == id_: 
				rate = (90 - s['minute'])/90;
			if s['playerOut'] == id_: 
				rate = s['minute']/90;

	contrib = rate*(pass_accuracy + duel_eff + fk_eff + shot_eff)/4;
	performance = contrib * pow((1 - 0.005), prop['foul']);
	performance = performance * pow((1 - 0.05), prop['own_goal']);
	rating = performance;

	# separate teams
	t = '';
	f = '';
	for i, team in enumerate(match['teamsData']):
		for fs in ['bench', 'lineup']:
			for player in match['teamsData'][team]['formation'][fs]:
				if(id_ == player['playerId']): 
					t = chr(i+97); 
					f = fs[0];
					break;

	id_ = str(id_)+f+t; 

	return (id_, {'foul': prop['foul'], 'goals': prop['shot_goal'] + prop['free_kick_goal'], \
			'own_goal': prop['own_goal'], 'pass_accuracy': pass_accuracy, 'shot_on_target': shot_on_target, \
			'rating': rating});

def update_profile(nv, cv):
	# key not in nv ---> [];
	# key not in cv ---> None;
	if cv == None:
		nv[0]['rating'] = str((float(nv[0]['rating'][:-1]) + 0.5)/2) + 'n';
		return nv[0];
	if nv == []:
		cv['rating'] = cv['rating'][:-1] + 'o';
		return cv;
	nv = nv[0];
	nv['rating'] = str((float(cv['rating'][:-1]) + float(nv['rating'][:-1])) / 2) + 'n';
	for x in ['foul', 'goals', 'own_goal', 'shot_on_target']:
		nv[x] += cv[x];
	nv['pass_accuracy'] = (cv['pass_accuracy'] + nv['pass_accuracy']) / 2;
	return nv;

def update_chem(nv, cv):
	# key not in nv ---> [];
	# key not in cv ---> None;
	if cv == None:
		return (nv[0] + 0.5)/2;
	if nv == []:
		return cv;
	return (nv[0]+cv)/2;

def combinations(x):
	lis = x[1];
	c = [];
	n = len(lis);
	for i in range(n):
		for j in range(n):
			if i == j: continue;
			id1 = lis[i][0];
			id2 = lis[j][0];
			cr1 = lis[i][1];
			cr2 = lis[j][1];
			chem = abs((cr1+cr2)/2);
			if(id1[-1] == id2[-1]):
				if (cr1 < 0 and cr2 > 0) or (cr1 > 0 and cr2 < 0):
					chem = -chem;
			else:
				if (cr1 > 0 and cr2 > 0) or (cr1 < 0 and cr2 < 0):
					chem = -chem;
			c.append(((id1, id2), chem))
	return c;

def find_all_profiles(match):
	all_ = [];
	for i, t in enumerate(match['teamsData']):
		ti = chr(i+97);
		for formations in ['bench', 'lineup']:
			for m in match['teamsData'][t]['formation'][formations]:
				all_.append((str(m['playerId'])+formations[0]+ti, {'foul': 0, 'goals': 0, 'own_goal': 0, \
					'pass_accuracy': 0, 'shot_on_target': 0, 'rating': 0}));
	return all_;

def add_dict(x,y):
	for a in x: x[a] += y[a];
	return x;

def change_rating(x):
	for a in x[1]:
		if(a == 'rating'):
			x[1][a] = str(x[1][a])+'o';
	return x;

def split_operate(rdd):
	try:
		data = rdd.map(lambda x: json.loads(x));
		mat = data.first();
		match = data.filter(lambda x: x == mat);
		event = data.filter(lambda x: x != mat);

		data = event.flatMap(lambda e: helper(e)) \
					.filter(lambda x: x!=None) \
					.reduceByKey(lambda x, y: x + y) \
					.map(lambda x: (x[0][0], {x[0][1]: x[1]})) \
					.reduceByKey(lambda x, y: {**x, **y}) \
					.map(lambda x: make_profile(mat, x));

		profiles = match.flatMap(find_all_profiles) \
						.union(data) \
						.reduceByKey(add_dict) \
						.map(change_rating);
		return profiles;

	except Exception as e:
		print(e);
		return rdd;

if __name__ == '__main__':
	conf = SparkConf();
	conf.setAppName("BigData")
	sc = SparkContext(conf = conf)

	ssc = StreamingContext(sc, 5);
	ssc.checkpoint('checkpoint');

	dataStream = ssc.socketTextStream('localhost', 6100);

	def check_in_team(x):
		if(x[0] == ''): return 0;
		if(x[0][-1] != 'a' or x[0][-1] != 'b'): return 0;
		return 1;

	player_profile = dataStream.transform(split_operate);
	changed_ratings = player_profile.map(lambda x: (x[0], float(x[1]['rating'][:-1]))) \
									.filter(lambda x: x[0]!='' and (x[0][-1] == 'a' or x[0][-1] == 'b'));
	player_profile = player_profile.map(lambda x: (x[0][:-2], x[1])).updateStateByKey(update_profile);

	new_ratings = player_profile.filter(lambda x: x[1]['rating'][-1] == 'n') \
								.map(lambda x: (x[0], float(x[1]['rating'][:-1]))) \
								.filter(lambda x: x[0]!='') \

	chemistry = changed_ratings.map(lambda x: (1,[(x[0], x[1])])) \
					.reduceByKey(lambda x, y: x+y) \
					.flatMap(combinations);
					
	sum_chem = chemistry.filter(lambda x: x[0][0][-1] == x[0][1][-1] and x[0][0][-2] == 'l' and x[0][1][-2] == 'l') \
						.map(lambda x: (x[0][0], x[1])) \
						.reduceByKey(lambda x,y: (x+y)/11);
	chemistry = chemistry.updateStateByKey(update_chem);

	def fg(x):
		s = 0;
		t = '';
		for val in x[1]:
			if(type(val)!=str):
				s += val;
			else:
				t = val;
		return (x[0]+t, s);

	common = sum_chem.map(lambda x: (x[0][:-2], [x[1], x[0][-2:]])) \
							.union(new_ratings) \
							.reduceByKey(lambda x, y: x+[y]) \
							.filter(lambda x: type(x[1]) == list) \
							.map(fg);

	def predict(x):
		c = x.collect();
		f = c[0];
		s = c[1];
		a = (0.5+f[1]-(f[1]+s[1])/2)*100;
		b = 100 - a;

		if(f[0] == 'a'):
			print('PREDICT-->',f[0],': ',a);
			print('PREDICT-->',s[0],': ',b);
		else:
			print('PREDICT-->',s[0],': ',b);
			print('PREDICT-->',f[0],': ',a);

	prediction = sum_chem.union(common) \
						.reduceByKey(lambda x,y: x*y) \
						.map(lambda x: (x[0][-1], x[1])) \
						.reduceByKey(lambda x,y: (x+y)/11) \

	prediction.foreachRDD(predict);


	dataStream.pprint();

	ssc.start();
	ssc.awaitTermination(15);
	ssc.stop();
# spark-submit test.py > out.txt
