import click
import cleaning_lib2
import lib
from operator import itemgetter
import multiprocessing, json, pandas, os, re, sys, math, time



def zerod_processor(file_path,tgp):  
	work_dir = os.environ[f'ZWORKDIR_{tgp}'] 
	df = pandas.read_csv(file_path)
	df = lib.zero_dist_trip(df)
	# df['imputed_list'] = df.apply(lambda row: lib.routing(row['lat_o'], row['lon_o'], row['lat_d'], row['lon_d']), axis =1)  ## makes the request for mapcrider2 routing
	error = None
	name = file_path.split('.')[0].split('trips')[1]
	linesCollection = dict(type="FeatureCollection",features=[])
	for i in range(len(df)):

		propertDict = dict(
			accuracy=df['accuracy'].tolist()[i],
			device_id=df['device_id'].tolist()[i],
			propulsion_type=df['propulsion_type'].tolist()[i],
			provider_id=df['provider_id'].tolist()[i],
			provider_name=df['provider_name'].tolist()[i],
			trip_distance=df['trip_distance'].tolist()[i],
			trip_duration=df['trip_duration'].tolist()[i],
			trip_id=df['trip_id'].tolist()[i],
			vehicle_id=df['vehicle_id'].tolist()[i],
			vehicle_type=df['vehicle_type'].tolist()[i],
			lat_o = df['lat_o'].tolist()[i],
			lon_o = df['lon_o'].tolist()[i],
			lat_d = df['lat_d'].tolist()[i],
			lon_d = df['lon_d'].tolist()[i],
			matching_error = error,
			start_time = str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime((df['start_time'].tolist()[i])))),
			end_time = str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime((df['end_time'].tolist()[i]))))
			)
		linegeo = dict(type="LineString",coordinates=[])
		for f in [(df['lon_o'].tolist()[i],df['lat_o'].tolist()[i]),(df['lon_d'].tolist()[i],df['lat_d'].tolist()[i])]:
			# print(f)
			linegeo['coordinates'].append(f)
		linefeature = dict(type="feature",geometry=linegeo,properties=propertDict)
		linesCollection['features'].append(linefeature)
		

	json_temp = json.dumps(linesCollection)
	# f = open(f"/home/bita/lime_bike/routes/zero_trips/zero_trips{name}.json","w")
	f = open(f"{work_dir}zero_trips{name}.json","w")
	f.write(json_temp)
	f.close()

	# delete if exitsts
	if os.path.exists(f"{work_dir}{tgp}DATA.csv"):
		os.remove(f"{work_dir}{tgp}DATA.csv")
	print(f'success {name}')



def multi_worker(all_good_trip_lst, tgp, prefix=1):

    print('Loading %d' % len(all_good_trip_lst))

    linesCollection = dict(type="FeatureCollection",features=[])
    work_dir = os.environ[f'WORKDIR_{tgp}'] 
    DATA = pandas.read_csv(f"{work_dir}{tgp}DATA.csv")
    click.echo(DATA)
    for trip in all_good_trip_lst:

        
    	
        mapped_points, df2, cal_error = lib.get_mapped_route(trip, tgp, data = DATA, work_dir=work_dir)
        linefeature = lib.add_mapped_points(df2, mapped_points, cal_error)
        linesCollection['features'].append(linefeature)
        json_temp = json.dumps(linesCollection)

    f = open(f"{work_dir}geojson_files/{tgp}/%s.json" % prefix,"w")  
    f.write(json_temp)
    f.close()
    print(f'{prefix} done')




def process_multi_segment_trip(df_speed, dfj, tgp, trip_id):
	work_dir = os.environ[f'WORKDIR_{tgp}'] 
	ver = os.environ['NCR_VER'] 
	df_t = df_speed[df_speed['trip_id']==trip_id]
	dfj_t = dfj[dfj['trip_id']==trip_id]
	st_lst = lib.remove_noise(df_t)
	j_lst = list(set([i[0]for i in st_lst]+[i[0]+1 for i in st_lst])) ## list of all rows indices that need to be dropped
	df_t = df_t.reset_index(drop = True).drop(j_lst)  ## 
	df_dict = df_t.reset_index(drop = True).to_dict('index')
	df_t = df_t.reset_index(drop=True)
	dist_thresh = min(max(df_t['step_dist'].quantile(0.75), 200), 350)
	index_jump_list = [k for k, v in df_dict.items() if v['step_dist']>dist_thresh]
	breaks = [(x,y) for x, y in zip(index_jump_list[:-1], index_jump_list[1:])]
	if breaks:
		breaks = breaks + [(0,index_jump_list[0]) , (index_jump_list[-1],len(df_t))]
		breaks = sorted(breaks,key=itemgetter(1))
	else:
		breaks = [(0, len(df_t))]

	linegeo = dict(type="MultiLineString",coordinates=[])
	propertDict= lib.prop_dict(dfj_t)
	for num, break_ in enumerate(breaks,0):
		table = {k:[] for k in ['timestamp','lat','lon']}
		if num ==0: ## for the first table, add the time1,.. to account for the first point
			table['timestamp'].append(df_t.iloc[0]['time1'])
			table['lat'].append(df_t.iloc[0]['lat1'])
			table['lon'].append(df_t.iloc[0]['lon1'])
		range_ = range(break_[0], break_[1])
		if len(range_)>=2:  ### WE DONT WANT SEGMENTS WITH A SINGLE POINT
			for i in range(break_[0], break_[1]):
				table['timestamp'].append(df_t.iloc[i]['time2'])
				table['lat'].append(df_t.iloc[i]['lat2'])
				table['lon'].append(df_t.iloc[i]['lon2'])
			df_split = pandas.DataFrame(table)
			seg_name = f'{trip_id}_{num}'
			lib.csv_to_gpx(df_split, tgp, seg_name, work_dir)
			matched_points, cal_error = lib.mapmatch(tgp, seg_name,work_dir)
			# print(seg_name, cal_error)
        ###### 	IF THERE IS A MAX THRESH ON THE ACCURACY LEVEL, SHOULD BE APPLIED HERE
			if cal_error <= 70:

				linegeo['coordinates'].append(matched_points)
				propertDict['matching_error'].append(cal_error)
	linefeature = dict(type="feature", geometry=linegeo, properties=propertDict)
	return linefeature





###########################################################################


def n_multi_worker(all_good_trip_lst, tgp, prefix=1):

	print('Loading %d' % len(all_good_trip_lst))

	linesCollection = dict(type="FeatureCollection",features=[])
	work_dir = os.environ[f'WORKDIR_{tgp}'] 
	ver = os.environ['NCR_VER'] 
	df_speed = pandas.read_csv(f"{work_dir}speed_files{ver}/speed__{tgp}.csv")
	dfj = pandas.read_csv(f'{work_dir}lime_trips_{tgp}.csv')
	for trip in all_good_trip_lst:
		try:
			linefeature = process_multi_segment_trip(df_speed, dfj, tgp, trip)
			linesCollection['features'].append(linefeature)
			json_temp = json.dumps(linesCollection)
			
		except Exception as e:
			with open(f'{work_dir}NCR_geojson_files/{tgp}/NCR_errors{ver}.json', 'a+') as outfile:
				error = {'error':str(e), 'trip': trip}
				outfile.write('%s\n' % json.dumps(error))
			continue
		except e:
			print(e)
			raise

	f = open(f"{work_dir}NCR_geojson_files/{tgp}/%s.json" % prefix,"w")
	# f = open(f"/mnt/c/Users/bitas/folders/MAPC/NCR_geojson_files/{tgp}/5.json" ,"w")
	# f = open(f"/mnt/c/Users/bitas/folders/MAPC/NCR_geojson_files/{tgp}/{trip}.json","w")  
	f.write(json_temp)
	f.close()
	print(f'{prefix} done')








@click.group()
@click.option('-tg', '--tgp', default='401_420', type=str, help='default is 401_420')
@click.option('-wd', '--work_dir', default='/home/arminakvn/Downloads/aa/', type=str, help='default is /home/arminakvn/Downloads/aa/')
@click.option('-nprfx', '--name_prefix', default='lime_trips_', type=str, help='defalut is lime_trips_')
# @click.option('-f', '--file_path',default='/home/arminakvn/Downloads/aa/lime_trips_401_420.csv',type=click.Path(exists=True))
@click.option('-v', '--ver', default=3, type=int,help='the version of the process that goes in the folder names like all_good3 the 3 is the ver')
@click.option('-t', '--numthreads',default=2, type=int)
@click.option('-l', '--numlines',default=50, type=int)
@click.pass_context
def cli(ctx,tgp,work_dir,name_prefix,ver,numthreads,numlines):
	"""module for the lime data parsing process"""
	ctx.ensure_object(dict)
	ctx.obj['tgp'] = tgp
	ctx.obj['work_dir'] = work_dir
	ctx.obj['name_prefix'] = name_prefix
	ctx.obj['file_path'] = f'{work_dir}{name_prefix}{tgp}.csv'
	ctx.obj['ver'] = ver
	ctx.obj['numthreads'] = numthreads
	ctx.obj['numlines'] = numlines
    # pass

@cli.command()
@click.pass_context
def clean(ctx):

## Read lime_bike CSVs:
	file_path = ctx.obj['file_path']
	filepath = click.format_filename(file_path)
	ver = ctx.obj['ver']
	numthreads = ctx.obj['numthreads']
	numlines = ctx.obj['numlines'] 

	click.echo(filepath)
	file_name = filepath.split('/')[-1]	
	work_dir = filepath.replace(file_name, '')
	click.echo(work_dir)
	df = pandas.read_csv(file_path)
	print(len(df))
	
	name = file_path.split('.')[0].split('trips')[1]
	## Distance and duration thresholds on the trip 

	df = df.loc[(df['trip_duration']>59) & (df['trip_distance']>99) & (df['trip_duration']<18000) & (df['trip_distance']<20000)]

	## Calculating nodes' speeds:

	trips = list(df['trip_id'])
	print(len(trips))
	df_speed = pandas.DataFrame()
	for i in trips:
	    df_speed = df_speed.append(cleaning_lib2.nodes_speed(df,i))
	    # print(len(df_speed))
	df_speed = df_speed.dropna()
	print(len(df_speed))
	## Adjusting speed:
	df_speed = df_speed.groupby('trip_id').apply(cleaning_lib2.adjust_speed)
	print(len(df_speed))
	## saving speeds and trips that need no imputing:

	df_speed = df_speed.dropna()
	speed_file_folder = f"speed_files{ver}"

	if os.path.exists(f"{work_dir}{speed_file_folder}"):
		pass
	else:
		click.echo("folder dont exist making it")
		os.mkdir(f'{work_dir}{speed_file_folder}')

	# df_speed.to_csv(f'/home/bita/lime_bike/data/speed/speed_{name}.csv')
	df_speed.to_csv(f'{work_dir}{speed_file_folder}/speed_{name}.csv')

	all_good_trips_folder = f'all_good_trips{ver}'

	if os.path.exists(f"{work_dir}{all_good_trips_folder}"):
		pass
	else:
		click.echo("folder dont exist making it")
		os.mkdir(f'{work_dir}{all_good_trips_folder}')


	cleaningNeeding_trips = list(set(df_speed.loc[(df_speed['step_dist']>201) | (df_speed['adjusted_speed']>12)]['trip_id'])) ## Trips with jupms or high speed
	all_good_ids = list(set(df[~df['trip_id'].isin(cleaningNeeding_trips)]['trip_id'])) ## Trips that need no imputing
	# with open(f"/home/bita/lime_bike/data/all_good_trips/all_good_ids_{name}.txt", "w") as f:
	with open(f"{work_dir}{all_good_trips_folder}/all_good_ids_{name}.txt", "w") as f:
	    for s in all_good_ids:
	        f.write(str(s) +"\n") 




@cli.command()
@click.pass_context
def  match(ctx):
	file_path = ctx.obj['file_path']
	filepath = click.format_filename(file_path)
	ver = ctx.obj['ver']
	numthreads = ctx.obj['numthreads']
	numlines = ctx.obj['numlines'] 
	click.echo(numthreads)
	click.echo(numlines)
	filepath = click.format_filename(file_path)
	click.echo(filepath)
	file_name = filepath.split('/')[-1]	
	work_dir = filepath.replace(file_name, '')

	click.echo(work_dir)
	speed_file_folder = f"speed_files{ver}"
	all_good_trips_folder = f'all_good_trips{ver}'
	tgp_01 = file_name.replace('lime_trips_','').replace('.csv','')
	os.environ[f'WORKDIR_{tgp_01}'] = work_dir 
	click.echo(tgp_01)
	# NUMTHREADS = 2
	# numlines = 50

	# DATA_PATH = '/mnt/c/Users/bitas/folders/MAPC/all_good_trips2/'
	# fps = [os.path.join(DATA_PATH, f) for f in os.listdir(DATA_PATH)]
	fps = [f'{work_dir}{all_good_trips_folder}/all_good_ids__{tgp_01}.txt']
	print(fps)
	for fp in fps:

		tgp = re.search('all_good_ids__(.*).txt', fp).group(1)
		print(tgp)
		click.echo(tgp)
		click.echo(tgp_01)

		# os.mkdir(f'/mnt/c/Users/bitas/folders/MAPC/geojson_files/{tgp}')

		all_good = []

		with open(fp,'r') as f:
			for line in f:
				all_good.append((line.strip()))


		
		df = pandas.read_csv(f'{work_dir}lime_trips_{tgp}.csv')  
		DATA = df.loc[df['trip_id'].isin(all_good)]
		# click.echo(DATA)

		# delete if exitsts
		if os.path.exists(f"{work_dir}{tgp}DATA.csv"):
			os.remove(f"{work_dir}{tgp}DATA.csv")

		DATA.to_csv(f"{work_dir}{tgp}DATA.csv")
		N = len(all_good)
		print(N)

		if os.path.exists(f"{work_dir}geojson_files"):
			if os.path.exists(f"{work_dir}geojson_files/{tgp}"):
			
				pass
			else:
				os.mkdir(f'{work_dir}geojson_files/{tgp}')
		else:
			click.echo("folder dont exist making it")
			os.mkdir(f'{work_dir}geojson_files')
			os.mkdir(f'{work_dir}geojson_files/{tgp}')

		if os.path.exists(f"{work_dir}gpx_files"):
			if os.path.exists(f"{work_dir}gpx_files/{tgp}"):
			
				pass
			else:
				os.mkdir(f'{work_dir}gpx_files/{tgp}')
		else:
			click.echo("folder dont exist making it")
			os.mkdir(f'{work_dir}gpx_files')
			os.mkdir(f'{work_dir}gpx_files/{tgp}')


		# os.mkdir(f'/mnt/c/Users/bitas/folders/MAPC/geojson_files/{tgp}')
		# os.mkdir(f'/mnt/c/Users/bitas/folders/MAPC/gpx_files/{tgp}')
		pool = multiprocessing.Pool(processes=numthreads)
		pool.starmap(
			multi_worker, 
			[

	        (all_good[line:line + numlines], tgp, line)
	        for line in range(0, N, numlines)
	        ]
	        )
		pool.close()
		pool.join()
		if os.path.exists(f"{work_dir}{tgp}DATA.csv"):
			os.remove(f"{work_dir}{tgp}DATA.csv")


# def multi_worker(file_paths):
#     print('Loading %s' % file_paths)
#     for fp in file_paths:
        
#         processer(fp)



@cli.command()
@click.pass_context
def nmatch(ctx):
	# NUMTHREADS = 2
	# NUMLINES = 50
	# tgp = '381_384'
	file_path = ctx.obj['file_path']
	tgp = ctx.obj['tgp']
	filepath = click.format_filename(file_path)
	ver = ctx.obj['ver']
	numthreads = ctx.obj['numthreads']
	numlines = ctx.obj['numlines'] 

	# filepath = click.format_filename(file_path)
	# click.echo(filepath)
	file_name = filepath.split('/')[-1]	
	work_dir = filepath.replace(file_name, '')
	# work_dir = filepath.replace(file_name, '')
	os.environ[f'WORKDIR_{tgp}'] = work_dir 
	os.environ['NCR_VER'] = f'{ver}'

	dfj = pandas.read_csv(f'{work_dir}lime_trips_{tgp}.csv')
	df_speed = pandas.read_csv(f"{work_dir}speed_files{ver}/speed__{tgp}.csv")
	cleaningNeeding_trips = list(set(df_speed.loc[(df_speed['step_dist']>201) | (df_speed['adjusted_speed']>12)]['trip_id']))
	# cleaningNeeding_trips = cleaningNeeding_trips[:50]
	# cleaningNeeding_trips = ['7a6ad51e-8332-4b7b-b27b-7e55e5c67fe8']
	N = len(cleaningNeeding_trips)
	print(N)


	if os.path.exists(f"{work_dir}NCR_geojson_files"):
			if os.path.exists(f"{work_dir}NCR_geojson_files/{tgp}"):
			
				pass
			else:
				os.mkdir(f'{work_dir}NCR_geojson_files/{tgp}')
	else:
		click.echo("folder dont exist making it")
		os.mkdir(f'{work_dir}NCR_geojson_files')
		os.mkdir(f'{work_dir}NCR_geojson_files/{tgp}')

	if os.path.exists(f"{work_dir}NCR_gpx_files"):
		if os.path.exists(f"{work_dir}NCR_gpx_files/{tgp}"):
		
			pass
		else:
			os.mkdir(f'{work_dir}NCR_gpx_files/{tgp}')
	else:
		click.echo("folder dont exist making it")
		os.mkdir(f'{work_dir}NCR_gpx_files')
		os.mkdir(f'{work_dir}NCR_gpx_files/{tgp}')



	
	# os.mkdir(f'/mnt/c/Users/bitas/folders/MAPC/NCR_geojson_files/{tgp}')
	# os.mkdir(f'/mnt/c/Users/bitas/folders/MAPC/NCR_gpx_files/{tgp}')
	pool = multiprocessing.Pool(processes=numthreads)
	pool.starmap(
		n_multi_worker, 
		[

        (cleaningNeeding_trips[line:line + numlines], tgp, line)
        for line in range(0, N, numlines)
        ]
        )

	pool.close()
	pool.join()
       


@cli.command()
@click.pass_context
def zerod(ctx):
	file_path = ctx.obj['file_path']
	tgp = ctx.obj['tgp']
	filepath = click.format_filename(file_path)

	ver = ctx.obj['ver']
	numthreads = ctx.obj['numthreads']
	numlines = ctx.obj['numlines'] 

	# filepath = click.format_filename(file_path)
	# click.echo(filepath)
	file_name = filepath.split('/')[-1]	
	work_dir = filepath.replace(file_name, '')
	# work_dir = filepath.replace(file_name, '')
	os.environ[f'ZWORKDIR_{tgp}'] = work_dir 
	os.environ['Z_VER'] = f'{ver}'

	zerod_processor(filepath,tgp)




if __name__ == '__main__':
	cli()



# NUMTHREADS = 8
# NUMLINES = 1

# if __name__ == '__main__':

#     # Get staypoint data filepaths
#     fps = [os.path.join(DATA_PATH, f) for f in os.listdir(DATA_PATH)]
#     print(fps)

#     N = len(fps)

#     pool = multiprocessing.Pool(processes=NUMTHREADS)
#     pool.map(multi_worker, 
#         (fps[line:line + NUMLINES] for line in range(0, N, NUMLINES)))
#     pool.close()
#     pool.join()
