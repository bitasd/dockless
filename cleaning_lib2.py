import pandas as pd, numpy as np, json, time
from geopy.distance import great_circle



def speed(lat1, lon1, lat2, lon2,timestamp1,timestamp2):
    try:
        return great_circle((lat1, lon1),(lat2, lon2)).meters/(timestamp2-timestamp1)
    except:
        pass


def dist_step(lat1, lon1, lat2, lon2):
    try:
        return great_circle((lat1, lon1),(lat2, lon2)).meters
    except:
        pass

def json_to_csv(df, trip_id): ## reads the json file requested from lime url and retrives route points of each trip in format of a dataframe

    df2 = df[df['trip_id']==trip_id]
    route = json.loads(df2['route'].item().replace("\'", "\""))['features']
    route_points={k:[] for k in ['lon','lat','timestamp']}

    for p in route:

        route_points['lon'].append(p['geometry']['coordinates'][0])
        route_points['lat'].append(p['geometry']['coordinates'][1])
        route_points['timestamp'].append(p['properties']['timestamp'])
    route_df = pd.DataFrame(route_points)
    
    return route_df



## calculate speed between each pair of GPS nodes and time and distance of each step
def nodes_speed(df, trip_id):
    
    v = json_to_csv(df,trip_id)
    route_df = pd.concat([v['lat'].shift(), v['lon'].shift(), v['timestamp'].shift(), v.loc[1:, 'lat'], v.loc[1:, 'lon'], v.loc[1:, 'timestamp']], axis=1, ignore_index=True)
    route_df.rename(columns={0: 'lat1',1:'lon1',2:'time1',3:'lat2',4:'lon2',5:'time2'}, inplace=True)
    route_df['speed'] = route_df.apply(lambda row: speed(row['lat1'], row['lon1'], row['lat2'], row['lon2'], row['time1'], row['time2'] ), axis=1)
    route_df['step_time'] = route_df.apply(lambda row: (row['time2'] - row['time1']), axis =1)
    route_df['step_dist'] = route_df.apply(lambda row: dist_step(row['lat1'], row['lon1'], row['lat2'], row['lon2'] ), axis=1)
    route_df['trip_id'] = trip_id
    return route_df


## This function adjusts the speed for steps that are preceeded by zero-speed points

def adjust_speed(df): 
    
    df_dict = df.to_dict('index')
    index_0_list = [k for k, v in df_dict.items() if v['speed']==0]

    non_zero_idx_list = [k for k, v in df_dict.items() if v['speed']> 0]

    for idx in non_zero_idx_list:  ## non_zero speed rows
        count =0
        l = [idx_0 for idx_0 in index_0_list if idx_0<idx]
        l.sort(reverse=True)
        temp_idx = idx
        for idx_0 in l:
            
            if temp_idx- idx_0 ==1:
                count+=1
                temp_idx = idx_0
            else:
                continue
        try:
            df_dict[idx]['adjusted_speed'] = (df_dict[idx]['step_dist'])/((count+1)*(df_dict[idx]['step_time']))
        except ZeroDivisionError:
            df_dict[idx]['adjusted_speed'] = None
    return pd.DataFrame.from_dict(df_dict, orient='index')

