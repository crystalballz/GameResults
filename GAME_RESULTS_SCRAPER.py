#!/usr/bin/python
# -*- coding: utf-8 -*-

# PROGRAM:		GAME RESULTS SCRAPER
# DESCRIPTION:	This program scrapes espn for teams in a 
#				given league, then scrapes the individual team 
#				sites for game results. Event type and event 
#				numberby type are added and the final table 
#				is then saved as a csv.
# AUTHOR:		Cristobal Mitchell
# CREATED:		08/10/2017
# MODIFIED:		01/14/2018
# VERSION:		1.0

# DEBUG FLAG
debug = False

# IMPORTS
import pandas as pd
import numpy as np
import requests
import shutil
import os
from bs4 import BeautifulSoup
from datetime import datetime, date
#from multiprocessing import Process

# GLOBAL VARIABLES
leagues = ['mlb','nba','nhl','nfl','wnba','mens-college-basketball','college-football']
years = [2015,2016,2017,2018]
team_map_file = '\\GameResults\\Inputs\\team_mapping.csv'
archive_path = '\\GameResults\\Outputs\\Archive\\'
result_files = ['\\GameResults\\Outputs\\nba_results.csv',\
				 '\\GameResults\\Outputs\\mlb_results.csv',\
				 '\\GameResults\\Outputs\\nhl_results.csv',\
				 '\\GameResults\\Outputs\\wnba_results.csv',\
				 '\\GameResults\\Outputs\\nfl_results.csv',\
				 '\\GameResults\\Outputs\\ncaam_results.csv',\
				 '\\GameResults\\Outputs\\ncaaf_results.csv',\
				 '\\GameResults\\GAME_RESULTS.csv']


# FUNCTIONS
def getTeams(leagues):
	'''
	THIS FUNCTION ITERATES THROUGH  THE LIST OF LEAGUES PROVIDED AND 
	SCRAPES FOR TEAM NAMES, URLS, DIVISION, AND SHORT NAME USED
	'''
	
	url = 'http://www.espn.com/{0}/teams'
	teams = []
	prefix_1 = []
	prefix_2 = []
	team_urls = []
	league_ = []
	division_ = []

	for league in leagues:
		r = requests.get(url.format(league))
		soup = BeautifulSoup(r.text, 'html.parser')
		divisions = soup.find_all('div', class_='mod-container mod-open-list mod-teams-list-medium mod-no-footer')
		
		for division in divisions:
			
			if league not in ['college-football','mens-college-basketball']:
				divs = division.find_all('div', class_='mod-header stathead')
				
				if league not in ['nhl']:
					tables = division.find_all('ul', class_='medium-logos')
				elif league == 'nhl':
					tables = division.find_all('ul') 
				for table in tables:
					lis = table.find_all('li')
					for li in lis:
						try:
							if league not in ['nhl','wnba']:
								info = li.h5.a
							elif league == 'nhl':
								info = li.div.h5.a		
							else:
								info = li.find_all('a')[1]
							teams.append(info.text)
							division_.append(divs[0].h4.contents[0])
							team_url = info['href']
							team_urls.append(team_url)
							prefix_1.append(team_url.split('/')[-2])
							prefix_2.append(team_url.split('/')[-1])
							league_.append(league)

							if debug:
								print info.text, league, divs[0].h4.contents[0]

						except:
							pass	
			
			else:
				divs = division.find_all('div', class_='mod-header colhead')
				for div in divs:
					tables = division.find_all('ul', class_ = 'medium-logos')
					for table in tables:
						lis = table.find_all('li')
						for li in lis:
							try:
								info = li.h5.a
								teams.append(info.text)
								division_.append(div.h4.contents[0])
								team_url = info['href']
								team_urls.append(team_url)
								prefix_1.append(team_url.split('/')[-2])
								prefix_2.append(team_url.split('/')[-1])
								league_.append(league)

								if debug:
									print info.text, league, divs[0].h4.contents[0]

							except:
								pass
	
	dic = {'TEAM': teams, 'URL': team_urls, 'PREFIX_2': prefix_2, 'PREFIX_1': prefix_1, 'LEAGUE': league_, 'DIVISION': division_}
	teams = pd.DataFrame(dic, index=teams)
	teams.index.name = 'TEAM_ID'
	
	if debug:
		teams.to_csv('\\GameResults\\Outputs\\teams.csv', encoding='utf-8', sep='|')
	return teams

def majorScraper(df,league,years):
	'''
	THIS FUNCTION WILL ITERATES THROUGH THE INPUT DATAFRAME AND THE 
	'years' LIST TO PULL GAME RESULTS, DATES, SEASON, EVENT TYPE, AND
	EVENT NUMBER BY TYPE.
	'''
	mlb1_url = 'http://espn.com/mlb/team/schedule/_/name/{0}/year/{1}/seasontype/1'
	mlb2_url = 'http://espn.com/mlb/team/schedule/_/name/{0}/year/{1}/seasontype/2/half/1'
	mlb3_url = 'http://espn.com/mlb/team/schedule/_/name/{0}/year/{1}/seasontype/2/half/2'
	mlb4_url = 'http://espn.com/mlb/team/schedule/_/name/{0}/year/{1}/seasontype/3'
	nba1_url = 'http://www.espn.com/nba/team/schedule/_/name/{0}/year/{1}/seasontype/1'
	nba2_url = 'http://www.espn.com/nba/team/schedule/_/name/{0}/year/{1}/seasontype/2'
	nba3_url = 'http://www.espn.com/nba/team/schedule/_/name/{0}/year/{1}/seasontype/3'
	nhl_url = 'http://www.espn.com/nhl/team/schedule/_/name/{0}/year/{1}'
	wnba_url = 'http://www.espn.com/wnba/team/schedule/_/name/{0}/year/{1}'
	nfl_url = 'http://www.espn.com/nfl/team/schedule/_/name/{0}/year/{1}'


	if league == 'mlb':
		urls = [mlb1_url, mlb2_url, mlb3_url, mlb4_url]
	elif league == 'nba':
		urls = [nba1_url, nba2_url, nba3_url]
	elif league == 'nhl':
		urls = [nhl_url]
	elif league == 'wnba':
		urls = [wnba_url]
	elif league == 'nfl':
		urls = [nfl_url]

	match_id = []
	dates = []
	home_team = []
	home_team_score = []
	away_team = []
	away_team_score = []
	type_ = []
	league_ = []
	season_ = []

	print league.upper(), 'Program Start'

	for year in years:
		for index, record in df.iterrows():
			_team, _url = record['PREFIX_2'], record['URL']
			for url in urls:
				r = requests.get(url.format(record['PREFIX_1'], year))
				try:
					table = BeautifulSoup(r.text, 'html.parser').table
					for row in table.find_all('tr', class_=lambda x: x != 'colhead'):
						if row['class'][0] == 'stathead':
							if league in ['nba','nfl','nhl','wnba']:
								_type = row.text.split()[1] 
							else:
								_type = row.text.split()[0]
						else:
							columns = row.find_all('td')
							if league != 'nfl':
								try:
									if columns[2].span.text in ['W','L','D','T']:
										try:
											_home = True if columns[1].ul.li.contents[0] == 'vs' else False
											_other_team = columns[1].find_all('a')[1]['href'].split('/')[-1]
											_score = columns[2].a.text.split(' ')[0].split('-')
											_won = True if columns[2].span.text == 'W' else False
											_league = league.upper()
											
											if league in ['mlb','nfl']: 
												_season = year
											else:
												_season = year-1
											
											if league in ['nhl']:
												match_id.append(columns[2].a['href'].split('id=')[1])
											elif columns[2].a['href'].split('.')[1][:4] != 'espn':
												match_id.append('no_match_id')
											elif league not in ['wnba']:
												match_id.append(columns[2].a['href'].split('id/')[1])
											else:
												match_id.append(columns[2].a['href'].split('gameId/')[1])
											
											home_team.append(_team if _home else _other_team)
											away_team.append(_team if not _home else _other_team)
											if league in ['mlb','wnba']:
												d = datetime.strptime("{0},{1}".format(columns[0].text.split(', ')[1],year), '%b %d,%Y')
											else:
												if columns[0].text.split()[1] in ['Jan','Feb','Mar','Apr','May','Jun','Jul']:
													d = datetime.strptime("{0},{1}".format(columns[0].text.split(', ')[1],year), '%b %d,%Y')
												else:
													d = datetime.strptime("{0},{1}".format(columns[0].text.split(', ')[1],(year-1)), '%b %d,%Y')
											

											dates.append(date(d.year, d.month, d.day))
											type_.append(_type)
											league_.append(_league)
											season_.append(_season)
											
											if _home:
												if _won:
													home_team_score.append(_score[0].strip())
													away_team_score.append(_score[1].strip())
												else:
													home_team_score.append(_score[1].strip())
													away_team_score.append(_score[0].strip())
											else:
												if _won:
													home_team_score.append(_score[1].strip())
													away_team_score.append(_score[0].strip())
												else:
													home_team_score.append(_score[0].strip())
													away_team_score.append(_score[1].strip())

											if debug:
												print len(match_id), len(dates), len(home_team), len(home_team_score), len(away_team) \
														, len(away_team_score), len(type_), len(league_), len(season_)

										except:
											pass 
									else:
										pass
								except:
									pass
							else:
								if columns[1].text != 'BYE WEEK':
									try:
										_home = True if columns[2].ul.li.contents[0] == 'vs' else False
										_other_team = columns[2].find_all('a')[1]['href'].split('/')[-1]
										_score = columns[3].a.text.split(' ')[0].split('-')
										_won = True if columns[3].span.text == 'W' else False
										_league = league.upper()
										_season = year

										if columns[3].a['href'].split('.')[1][:4] != 'espn':
											match_id.append('no_match_id')
										else:
											match_id.append(columns[3].a['href'].split('gameId/')[1])
										
										home_team.append(_team if _home else _other_team)
										away_team.append(_team if not _home else _other_team)
										
										if columns[1].text.split()[1] in ['Jan','Feb','Mar','Apr','May','Jun','Jul']:
											d = datetime.strptime("{0},{1}".format(columns[1].text.split(', ')[1],(year+1)), '%b %d,%Y')
										else:
											d = datetime.strptime("{0},{1}".format(columns[1].text.split(', ')[1],(year)), '%b %d,%Y')
										
										dates.append(date(d.year, d.month, d.day))
										type_.append(_type)
										league_.append(_league)
										season_.append(_season)

										if _home:
											if _won:
												home_team_score.append(_score[0].strip())
												away_team_score.append(_score[1].strip())
											else:
												home_team_score.append(_score[1].strip())
												away_team_score.append(_score[0].strip())
										else:
											if _won:
												home_team_score.append(_score[1].strip())
												away_team_score.append(_score[0].strip())
											else:
												home_team_score.append(_score[0].strip())
												away_team_score.append(_score[1].strip())

										if debug:
												print len(match_id), len(dates), len(home_team), len(home_team_score), len(away_team) \
														, len(away_team_score), len(type_), len(league_), len(season_)

									except:
										pass

				except:
					pass

	if debug:
		print 'match_id', len(match_id)
		print 'dates', len(dates)
		print 'home_team', len(home_team)
		print 'home_team_score', len(home_team_score)
		print 'away_team', len(away_team)
		print 'away_team_score', len(away_team_score)
		print 'type_', len(type_)
		print 'league_', len(league_)
		print 'season_', len(season_)

	dic = {'ID': match_id, 'EVENT_DATE': dates, 'HOME_TEAM': home_team, 'AWAY_TEAM': away_team,
			'HOME_TEAM_SCORE': home_team_score, 'AWAY_TEAM_SCORE': away_team_score, 
			'LEAGUE': league_ , 'PRE/REG/POST': type_ , 'SEASON': season_}

	gameResults = pd.DataFrame(dic).drop_duplicates(subset='ID').set_index('ID')
	gameResults['HOME_TEAM_SCORE'] = gameResults['HOME_TEAM_SCORE'].apply(pd.to_numeric, args=('coerce',))
	gameResults['AWAY_TEAM_SCORE'] = gameResults['AWAY_TEAM_SCORE'].apply(pd.to_numeric, args=('coerce',))
	gameResults = replaceTeamName(gameResults,league)
	gameResults['EVENT_TYPE'] = gameResults.apply(addEventType,axis=1)	
	gameResults = addEventNumber(gameResults)
	gameResults['RESULT'] = gameResults.apply(addResult,axis=1)
	cols = ['EVENT_DATE','SEASON','EVENT_TYPE','HOME_TEAM','HOME_TEAM_SCORE','AWAY_TEAM','AWAY_TEAM_SCORE','RESULT','EVENT_NUMBER_BY_TYPE']
	gameResults[cols].to_csv('\\GameResults\\Outputs\\{0}_results.csv'.format(league), encoding='utf-8')
	print '{0} Program Complete'.format(league.upper())
	#return gameResults[cols]

def collegeScraper(df,league,years):
	'''
	THIS FUNCTION WILL ITERATES THROUGH THE INPUT DATAFRAME AND THE 
	'years' LIST TO PULL GAME RESULTS, DATES, SEASON, EVENT TYPE, AND
	EVENT NUMBER BY TYPE.
	'''
	ncaaf_url = 'http://www.espn.com/college-football/team/schedule/_/id/{0}/year/{1}'
	ncaam_url = 'http://www.espn.com/mens-college-basketball/team/schedule/_/id/{0}/year/{1}'
	

	if league == 'college-football':
		urls = [ncaaf_url]
	elif league == 'mens-college-basketball':
		urls = [ncaam_url]
	

	match_id = []
	dates = []
	home_team = []
	home_team_score = []
	away_team = []
	away_team_score = []
	type_ = []
	league_ = []
	season_ = []

	print league.replace('mens-','').replace('-',' ').title(), 'Program Start'

	for year in years:
		for index, record in df.iterrows():
			_team, _url = record['PREFIX_2'], record['URL']
			for url in urls:
				r = requests.get(url.format(record['PREFIX_1'], year))
				if debug:
					print r.status_code
				try:
					_type = 'Regular'
					table = BeautifulSoup(r.text, 'html.parser').table
					for row in table.find_all('tr', class_=lambda x: x not in ['colhead','stathead']):
						if debug:
							print 'step 1'
						if row['class'][-1] in ['evenrow','oddrow']:
							if league in ['mens-college-basketball']:
								_type = 'NCAA Tournament'
							elif league in ['college-football']:
								_type = 'Bowl'
						try:
							columns = row.find_all('td')
							if debug:
								print 'step 2'
							if columns[2].contents[0].text[:1] in ['W','L','D','T']: #columns[2].span.contents[0] in ['W','L','D','T']:
								if debug:
									print 'W/L/D'
								try:
									
									_home = True if columns[1].ul.li.contents[0] == 'vs' else False
									
									if columns[1].a['href'].split('.')[1][:4] == 'espn':
										_other_team = columns[1].find_all('a')[1]['href'].split('/')[-1]
									else:
										_other_team = columns[1].find_all('li', class_='team-name')[0].text
									if debug:
										print 'team assigned'
									_score = columns[2].a.text.split(' ')[0].split('-')
									_won = True if columns[2].span.text == 'W' else False
									_league = league.replace('mens-','').replace('-',' ').title()
									
									if league == 'college-football':
										_season = year
									elif league == 'mens-college-basketball':
										_season = year-1
									
									if debug:
										print 'season assigned'
									try:
										# if columns[2].a['href'].split('.')[1][:4] != 'espn':
										# 	match_id.append('no_match_id')
										# elif league not in ['mens-college-basketball']:
										if league not in ['mens-college-basketball']:
											match_id.append(columns[2].a['href'].split('id/')[1])
										else:
											match_id.append(columns[2].a['href'].split('gameId/')[1])
									except:
										match_id.append('no_match_id')

									home_team.append(_team if _home else _other_team)
									away_team.append(_team if not _home else _other_team)
									
									if league in ['college-football']:
										if columns[0].text.split()[1] in ['Jan','Feb','Mar','Apr','May','Jun','Jul']:
											d = datetime.strptime("{0},{1}".format(columns[0].text.split(', ')[1],(year+1)), '%b %d,%Y')
										elif columns[0].text.split()[1] in ['Sept']:
											d = datetime.strptime("{0} {1},{2}".format('Sep',columns[0].text.split(' ')[2],(year)), '%b %d,%Y')
										else:
											d = datetime.strptime("{0},{1}".format(columns[0].text.split(', ')[1],(year)), '%b %d,%Y')
									else:
										if columns[0].text.split()[1] in ['Jan','Feb','Mar','Apr','May','Jun','Jul']:
											d = datetime.strptime("{0},{1}".format(columns[0].text.split(', ')[1],year), '%b %d,%Y')
										else:
											d = datetime.strptime("{0},{1}".format(columns[0].text.split(', ')[1],(year-1)), '%b %d,%Y')
									

									dates.append(date(d.year, d.month, d.day))
									type_.append(_type)
									league_.append(_league)
									season_.append(_season)
									
									if _home:
										if _won:
											home_team_score.append(_score[0].strip())
											away_team_score.append(_score[1].strip())
										else:
											home_team_score.append(_score[1].strip())
											away_team_score.append(_score[0].strip())
									else:
										if _won:
											home_team_score.append(_score[1].strip())
											away_team_score.append(_score[0].strip())
										else:
											home_team_score.append(_score[0].strip())
											away_team_score.append(_score[1].strip())

									if debug:
										print (len(match_id), len(dates), len(home_team), len(home_team_score), len(away_team) \
												, len(away_team_score), len(type_), len(league_), len(season_))

								except:
									pass 
							else:
								pass
						except:
							pass

				except:
					pass

	if debug:
		print 'match_id', len(match_id)
		print 'event date', len(dates)
		print 'home_team', len(home_team)
		print 'home_team_score', len(home_team_score)
		print 'away_team', len(away_team)
		print 'away_team_score', len(away_team_score)
		print 'type_', len(type_)
		print 'league_', len(league_)
		print 'season_', len(season_)

	dic = {'ID': match_id, 'EVENT_DATE': dates, 'HOME_TEAM': home_team, 'AWAY_TEAM': away_team,
			'HOME_TEAM_SCORE': home_team_score, 'AWAY_TEAM_SCORE': away_team_score, 
			'LEAGUE': league_ , 'PRE/REG/POST': type_ , 'SEASON': season_}

	gameResults = pd.DataFrame(dic).drop_duplicates(subset='ID').set_index('ID')
	gameResults['HOME_TEAM_SCORE'] = gameResults['HOME_TEAM_SCORE'].apply(pd.to_numeric, args=('coerce',))
	gameResults['AWAY_TEAM_SCORE'] = gameResults['AWAY_TEAM_SCORE'].apply(pd.to_numeric, args=('coerce',))
	gameResults = replaceTeamName(gameResults,league)
	gameResults['EVENT_TYPE'] = gameResults.apply(addEventType,axis=1)	
	gameResults = addEventNumber(gameResults)
	gameResults['RESULT'] = gameResults.apply(addResult,axis=1)
	cols = ['EVENT_DATE','SEASON','EVENT_TYPE','HOME_TEAM','HOME_TEAM_SCORE','AWAY_TEAM','AWAY_TEAM_SCORE','RESULT','EVENT_NUMBER_BY_TYPE']
	if league == 'mens-college-basketball':
		gameResults[cols].to_csv('\\GameResults\\Outputs\\{0}_results.csv'.format('ncaam'), encoding='utf-8')
	elif league == 'college-football':
		gameResults[cols].to_csv('\\GameResults\\Outputs\\{0}_results.csv'.format('ncaaf'), encoding='utf-8')
	print '{0} Program Complete'.format(league.replace('mens-','').replace('-',' ').title())
	#return gameResults[cols]

def replaceTeamName(df, league):
	team_map = pd.read_csv(team_map_file, sep='|', encoding='utf-8')
	df['HOME_TEAM'] = df['HOME_TEAM'].map(team_map[team_map['LEAGUE']==league].set_index('PREFIX_2')['TEAM_NAME'])
	df['AWAY_TEAM'] = df['AWAY_TEAM'].map(team_map[team_map['LEAGUE']==league].set_index('PREFIX_2')['TEAM_NAME'])
	return df

def addResult(row):
	if int(row['HOME_TEAM_SCORE']) > int(row['AWAY_TEAM_SCORE']):
		return 'W'
	elif int(row['HOME_TEAM_SCORE']) < int(row['AWAY_TEAM_SCORE']):
		return 'L'
	elif int(row['HOME_TEAM_SCORE']) == int(row['AWAY_TEAM_SCORE']):
		return 'D'
	else:
		return ''

def addEventType(row):
	if row['LEAGUE'].split()[0] != 'College':
		if row['PRE/REG/POST'] == 'Regular':
			return '{0} Game'.format(row['LEAGUE'])
		elif row['PRE/REG/POST'] in ['Spring','Preseason']:
			return '{0} Game - Pre-Season'.format(row['LEAGUE'])
		elif row['PRE/REG/POST'] == 'Postseason':
			return '{0} Game - Playoffs'.format(row['LEAGUE'])
	elif row['LEAGUE'].split()[0] == 'College':
		if row['PRE/REG/POST'] == 'Regular':
			return '{0}'.format(row['LEAGUE'])
		elif row['LEAGUE'].split()[1] == 'Football':
			return '{0} - {1}'.format(row['LEAGUE'],'Bowl')
		elif row['LEAGUE'].split()[1] == 'Basketball':
			return '{0} - {1}'.format(row['LEAGUE'],'NCAA Tournament')
			


def addEventNumber(df):
	df.sort_values(['HOME_TEAM','SEASON','EVENT_TYPE','EVENT_DATE'], axis=0, inplace=True)
	df['EVENT_NUMBER_BY_TYPE'] = df.groupby(['HOME_TEAM','SEASON','EVENT_TYPE']).cumcount()+1
	return df

def archiveAndExport(files):
	cols = ['EVENT_DATE','SEASON','EVENT_TYPE','HOME_TEAM','HOME_TEAM_SCORE','AWAY_TEAM','AWAY_TEAM_SCORE','RESULT','EVENT_NUMBER_BY_TYPE']
	output = pd.DataFrame()
	for file in files:
		df = pd.read_csv(file)
		output = pd.concat([output,df])
		shutil.move(file,archive_path+datetime.now().strftime("%Y%m%d_%H%M%S_")+(file.split('\\')[-1]))
	output.drop(['ID'], axis=1, inplace=True)
	output[cols].to_csv('\\GameResults\\GAME_RESULTS.csv', sep='|', encoding='utf-8', index=False)
	

		

if __name__ == '__main__':
	teams = getTeams(leagues)
	# p1 = Process(target=majorScraper, args=(teams[teams['LEAGUE']=='mlb'],'mlb',years))	
	# p2 = Process(target=majorScraper, args=(teams[teams['LEAGUE']=='nba'],'nba',years))	
	# p3 = Process(target=majorScraper, args=(teams[teams['LEAGUE']=='nhl'],'nhl',years))	
	# p4 = Process(target=majorScraper, args=(teams[teams['LEAGUE']=='nfl'],'nfl',years))	
	# p5 = Process(target=majorScraper, args=(teams[teams['LEAGUE']=='wnba'],'wnba',years))
	# p6 = Process(target=collegeScraper, args=(teams[teams['LEAGUE']=='mens-college-basketball'],'mens-college-basketball',years))
	# p7 = Process(target=collegeScraper, args=(teams[teams['LEAGUE']=='college-football'],'college-football',years))

	# p1.start()
	# p2.start()
	# p3.start()
	# p4.start()
	# p5.start()
	# p6.start()
	# p7.start()
	
	# p1.join()
	# p2.join()
	# p3.join()
	# p4.join()
	# p5.join()
	# p6.join()
	# p7.join()	

	majorScraper(teams[teams['LEAGUE']=='mlb'],'mlb',years)	
	majorScraper(teams[teams['LEAGUE']=='nba'],'nba',years)
	majorScraper(teams[teams['LEAGUE']=='nhl'],'nhl',years)
	majorScraper(teams[teams['LEAGUE']=='nfl'],'nfl',years)
	majorScraper(teams[teams['LEAGUE']=='wnba'],'wnba',years)
	collegeScraper(teams[teams['LEAGUE']=='mens-college-basketball'],'mens-college-basketball',years)
	collegeScraper(teams[teams['LEAGUE']=='college-football'],'college-football',years)

	archiveAndExport(result_files)

	