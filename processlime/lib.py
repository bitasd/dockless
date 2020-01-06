import pandas, time, re, requests, json, math
from xml.etree.ElementTree import Comment, tostring
from xml.etree.ElementTree import ElementTree
from xml.etree.ElementTree import Element
from xml.etree.ElementTree import SubElement
import xml.etree.ElementTree as ET

from xml.dom import minidom
from geopy.distance import great_circle


# import multiprocessing, os

# 02



def prettify(elem):
    """Return a pretty-printed XML string for the Element.
    """
    rough_string = tostring(elem, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    return reparsed.toprettyxml(indent="  ")




def json_to_csv(df, trip_id): ## reads the json file requested from lime url and retrives route points of each trip in format of a dataframe

    df2 = df[df['trip_id']==trip_id]
    route = json.loads(df2['route'].item().replace("\'", "\""))['features']
    route_points={k:[] for k in ['lon','lat','timestamp']}

    for p in route:

        route_points['lon'].append(p['geometry']['coordinates'][0])
        route_points['lat'].append(p['geometry']['coordinates'][1])
        route_points['timestamp'].append(p['properties']['timestamp'])
    route_df = pandas.DataFrame(route_points)
    # route_df.to_csv(f'/mnt/c/Users/bitas/folders/MAPC/csv_files/{trip_id}.csv')
    return route_df, df2


def csv_to_gpx(df, tgp, trip_id,work_dir): ## Gets the dataframe created above and converts it into a gpx format

    root = Element('gpx')
    root.set('version', '1.1')
    root.set('xmlns',"http://www.topografix.com/GPX/1/1")
    root.set('xmlns:xsi',"http://www.w3.org/2001/XMLSchema-instance")
    root.set('creator',"data from lime bike analysis by mapc")
    root.set('xmlns:gh',"https://graphhopper.com/public/schema/gpx/1.1")
    trk = SubElement(root,'trk')
    _name = SubElement(trk, 'name')
    _name.text = "Track with Python"

	# df = pandas.read_csv(file_path)
    df['date_time'] = df["timestamp"].apply(lambda x:  pandas.to_datetime(x,unit='s'))
    df["datetime"] = df["date_time"].apply(lambda x : re.sub(r'\s','T', f"{x}"))
    df["datetime"] = df["datetime"].apply(lambda x : f"{x}Z")
    df = df.reset_index()
    trkseg = SubElement(trk, 'trkseg')
    for i in range(len(df)):
        iindex = df.index[i]
        this_lat = df.loc[iindex,["lat"]].values[0]
        this_lon = df.loc[iindex,["lon"]].values[0]
        this_record = df.loc[iindex,["datetime"]].values[0]
        trkpt = SubElement(trkseg, 'trkpt', {'lat': f"{this_lat}", 'lon':f"{this_lon}"})
        _ele = SubElement(trkpt, 'ele')
        _ele.text = "16.39"
        _time = SubElement(trkpt,'time')
        _time.text = f"{this_record}"
    tree = ElementTree(root)
    tree.write(open(f"{work_dir}gpx_files/{tgp}/{trip_id}.gpx", 'wb'), encoding='UTF-8')
    # tree.write(open(f"/mnt/c/Users/bitas/folders/MAPC/NCR_gpx_files/{tgp}/{trip_id}.gpx", 'wb'), encoding='UTF-8')
    # tree.write(open(f"/mnt/c/Users/bitas/folders/MAPC/NCR_c_gpx_files/{tgp}/{trip_id}.gpx", 'wb'), encoding='UTF-8')

def cal_error(dist, original_dist):
    try:
        cal_error = (100 * abs(1 -  dist /original_dist))
        cal_error = math.floor(cal_error * 100) / 100.0
        return cal_error
    except ZeroDivisionError:
        return 100

def mapmatch(tgp, trip_id, work_dir):  ## Feeds the GPX formatted files to graphhopper and yields the snapped points in format of a list


    headers = {
            'Content-Type': 'application/gpx+xml'
    }

    ###### test multiple gps accuracies and pick the best one

    gps_accuracy =20
    resp_20 = requests.post(
        'http://localhost:8989/match?locale=en&gps_accuracy=20&max_visited_nodes=2000&type=json&vehicle=bfoot&points_encoded=false&weighting=shortest&ch.disable=true', 
        data = open(f"{work_dir}gpx_files/{tgp}/{trip_id}.gpx",'rb'), 
        # data = open(f"/mnt/c/Users/bitas/folders/MAPC/NCR_c_gpx_files/{tgp}/{trip_id}.gpx",'rb'),
        headers = headers
    )
    response_20 = resp_20.json()

    map_match_20 = response_20['paths'][0]['points']['coordinates']

    ## calculating the difference between original and matched routes' distances 
    dist_20 = response_20['map_matching']['distance']

    original_dist_20 = response_20['map_matching']['original_distance']
    
    diff_20 = cal_error(dist_20, original_dist_20)


    gps_accuracy =40
    resp_40 = requests.post(
        'http://localhost:8989/match?locale=en&gps_accuracy=40&max_visited_nodes=2000&type=json&vehicle=bfoot&points_encoded=false&weighting=shortest&ch.disable=true', 
        data = open(f"{work_dir}gpx_files/{tgp}/{trip_id}.gpx",'rb'), 
        # data = open(f"/mnt/c/Users/bitas/folders/MAPC/NCR_c_gpx_files/{tgp}/{trip_id}.gpx",'rb'),
        headers = headers
    )
    response_40 = resp_40.json()
    # click.echo(response_40)

    map_match_40 = response_40['paths'][0]['points']['coordinates']

    dist_40 = response_40['map_matching']['distance']
    original_dist_40 = response_40['map_matching']['original_distance']
    diff_40 = cal_error(dist_40, original_dist_40)


    if diff_20<diff_40 or abs(diff_20-diff_40)<6:

        diff = diff_20
        map_match = map_match_20 

    else:
        diff = diff_40
        map_match = map_match_40
    # print(diff, trip_id)
    return map_match, diff



def get_mapped_route(trip_id, tgp, data, work_dir): ## combines functions for map-matching and returns a nested dictionary including needed fields in the shapefile for each  trip 


	df, df_trip = json_to_csv(data,trip_id)
	csv_to_gpx(df, tgp, trip_id, work_dir)
	matched_points, cal_error = mapmatch(tgp, trip_id, work_dir)

	return matched_points, df_trip, cal_error  ## returns error as a relative difference between the original path and the matched one


def add_mapped_points(df, matched_points, cal_error):  ## adds the snapped path to the df retrieved from lime url


    propertDict = dict(
        accuracy=df['accuracy'].item(),
        device_id=df['device_id'].item(),

        propulsion_type=df['propulsion_type'].item(),
        provider_id=df['provider_id'].item(),
        provider_name=df['provider_name'].item(),

        trip_distance=df['trip_distance'].item(),
        trip_duration=df['trip_duration'].item(),
        trip_id=df['trip_id'].item(),
        vehicle_id=df['vehicle_id'].item(),
        vehicle_type=df['vehicle_type'].item(),
        lat_o = json.loads(df['route'].item().replace("\'", "\""))['features'][0]['geometry']['coordinates'][1],
        lon_o = json.loads(df['route'].item().replace("\'", "\""))['features'][0]['geometry']['coordinates'][0],
        lat_d = json.loads(df['route'].item().replace("\'", "\""))['features'][-1]['geometry']['coordinates'][1],
    	lon_d = json.loads(df['route'].item().replace("\'", "\""))['features'][-1]['geometry']['coordinates'][0],
        matching_error = cal_error,
        start_time = str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime((df['start_time'].item())))),
        end_time = str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime((df['end_time'].item()))))
    )
    linegeo = dict(type="LineString",coordinates=[])
    
    for f in matched_points:  

        linegeo['coordinates'].append(f)
    linefeature = dict(type="feature",geometry=linegeo,properties=propertDict)
    return linefeature


############# LIBRARIES FOR ZERO-DISTANCE TRIPS

def od_extractor(route): ## Returns origin/destination coordinates

    route = json.loads(route.replace("\'", "\""))['features']
    lon_o,lat_o = route[0]['geometry']['coordinates']
    lon_d, lat_d  = route[-1]['geometry']['coordinates']
    return lon_o, lat_o, lon_d, lat_d



# def routing(lat_o, lon_o, lat_d, lon_d):   ## Requests for getting route using mapcrider2
    
#     resj = json.loads(requests.get(

#         f'http://10.10.10.249:8000/route/?point={lat_o}%2C{lon_o}&point={lat_d}%2C{lon_d}&locale=en-US&points_encoded=false&vehicle=mapcrider2&weighting=fastest&elevation=true&use_miles=false&layer=Mapbox%20Tile'
#         ).content)
  
#     try:
#         result = resj['paths'][0]['points']['coordinates']
#         return result
#     except:
#         print(resj)
#         return []



def zero_dist_trip(df):  ## Getting the TRIPS that are zero-distance and longer than one minute, shoter than 5 hours and (origin and destination are further than 100 meters apart) maybe change this condition as origin and destination might end up at the same place:

    df_0 = df[(df['trip_distance']==0) & (df['trip_duration']<18000) & (df['trip_duration']>59)]
    result = df_0.apply(lambda row: od_extractor(row['route']), axis=1 )
    df_0['lon_o'], df_0['lat_o'], df_0['lon_d'], df_0['lat_d'] = zip(*result)
    df_0['trip_dist_cal'] = df_0.apply(lambda row: great_circle((row['lat_o'], row['lon_o']), (row['lat_d'], row['lon_d'])).meters, axis=1)
    df_0 = df_0.loc[(df_0['trip_dist_cal']>99) & (df_0['trip_dist_cal']<20000)]
    print(len(df_0))
    return df_0





############# LIBRARIES FOR JUMPY/NOISY-DISTANCE TRIPS


def remove_noise(df1): ## returns timestamp of noisy jumps if speeds of consective steps are high and distances are similar
    df1 = df1.reset_index(drop =True)
    df_dict = df1.to_dict('index')
    index_high_list = [k for k, v in df_dict.items() if v['adjusted_speed']>12]
    cons = [(x,df_dict[y]['time1']) for x, y in zip(index_high_list[:-1], index_high_list[1:]) if y-x==1 and cal_error(df_dict[y]['step_dist'],df_dict[x]['step_dist'])<2]  
    return cons

def prop_dict(df):  ## adds the snapped path to the df retrieved from lime url


    propertDict = dict(
        accuracy=df['accuracy'].item(),
        device_id=df['device_id'].item(),

        propulsion_type=df['propulsion_type'].item(),
        provider_id=df['provider_id'].item(),
        provider_name=df['provider_name'].item(),

        trip_distance=df['trip_distance'].item(),
        trip_duration=df['trip_duration'].item(),
        trip_id=df['trip_id'].item(),
        vehicle_id=df['vehicle_id'].item(),
        vehicle_type=df['vehicle_type'].item(),
        lat_o = json.loads(df['route'].item().replace("\'", "\""))['features'][0]['geometry']['coordinates'][1],
        lon_o = json.loads(df['route'].item().replace("\'", "\""))['features'][0]['geometry']['coordinates'][0],
        lat_d = json.loads(df['route'].item().replace("\'", "\""))['features'][-1]['geometry']['coordinates'][1],
        lon_d = json.loads(df['route'].item().replace("\'", "\""))['features'][-1]['geometry']['coordinates'][0],
        matching_error = [],
        start_time = str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime((df['start_time'].item())))),
        end_time = str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime((df['end_time'].item()))))
    )

    return propertDict


