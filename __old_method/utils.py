import numpy as np
from datetime import datetime, timedelta
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pymongo, subprocess

def find_common_prefix(strings):
    if not strings:
        return ""
    
    prefix = strings[0]
    for string in strings[1:]:
        while string[:len(prefix)] != prefix:
            prefix = prefix[:-1]
            if not prefix:
                return ""
    
    return prefix


def points_to_hist(size, bins_joint):
    x, y = [], []
    for i, b in enumerate(bins_joint):
        if i == 0 or i == len(bins_joint) - 1:
            x.append(b)    
        else:
            x.append(b)
            x.append(b)
    
        if i < len(bins_joint) - 1:
            y.append(size[i])
            y.append(size[i])
    return x, y

def bins_merger(counts, mask, bins_joint):
    counts_joint = []
    for mi in range(len(mask)):
        if mask[mi] == None:
            counts_joint.append(0.0)
        else:
            # print(mask[mi])
            counts_joint.append(counts[mask[mi]])
    return np.array(counts_joint)

# THIS ONLY WORKS TO GET THE OBSERVATION RECORDS NOT THE VARIABLES ITSELF
# USE TO GET DATATAKING/CALIBRATION PERIODS OR RUN INFORMATION
def get_records(mongo_client, database_name, collection_name, query):
    collection = mongo_client[database_name][collection_name]
    response   = collection.find(query)
    records = tuple(doc for doc in response)    
    return records



def extract_common_prefix(strings):
    common_prefix = find_common_prefix(strings)
    if not common_prefix:
        return [""] * len(strings)
    
    extracted_prefixes = []
    for string in strings:
        if string.startswith(common_prefix):
            extracted_prefixes.append(string[len(common_prefix):])
        else:
            extracted_prefixes.append(string)
    
    return extracted_prefixes

def get_time_and_values(out_caco):
    times      = np.array(out_caco["time"])
    values     = np.array(out_caco["value"])
    timestamps = np.array([t.timestamp() for t in times])

    # Sorting in time order, because for some reason entries are not read in order
    sorted_indices_general = np.argsort(times)
    times  = times[sorted_indices_general]
    values = values[sorted_indices_general]    
    timestamps = timestamps[sorted_indices_general]
    
    return times, values, timestamps


def get_caco_entries(client, variable, dict_caco_names, tstart, tstop):
    
    if variable != None:
        # We search for the variable inside all caco collections
        for coll in dict_caco_names.keys():
            for name in dict_caco_names[coll]:
                if variable == name:
                    var_collection = coll
                    break

        # Then we open this collection with the specified query
        query_caco = {"date" : {"$gte" : tstart, "$lte" : tstop}, "name" : variable}
        collection = client.CACO[var_collection].find(query_caco)

        # Getting the values and the dates
        dates, values = [], []
        for entrie in collection:

            tbase = entrie["date"]
            for second in list(entrie["values"]):
                date  = tbase + timedelta(seconds=int(second))
                value = entrie["values"][second]
                dates.append(date)
                values.append(value)
            
            
    else:
        var_collection = None
        dates, values = [], []
                
    
    # Store in a dictionary as output
    dict_out = {
        "name" : variable,
        "collection" : var_collection,
        "time" : dates,
        "value" : values,
    }
    
    return dict_out


def format_time_ticks_axes(ax, lim_m, lim_M, timespan):
    n_seconds = timespan.total_seconds()
    n_mins    = n_seconds / 60
    n_5mins   = n_seconds / 60 * 5
    n_10mins  = n_seconds / 60 * 10
    n_15mins  = n_seconds / 60 * 15
    n_30mins  = n_seconds / 60 * 30
    n_hours   = n_seconds / 3600
    n_days    = n_seconds / 3600 / 24
    n_months  = n_seconds / 3600 / 24 / 30.4
    n_years   = n_seconds / 3600 / 24 / 30.4 / 365

    # Days as axis
    if n_days > 20:
        pass
    elif n_hours > 20:
        ax.xaxis.set_major_locator(mdates.DayLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%b\n%d'))
    elif n_30mins > 20:
        ax.xaxis.set_major_locator(mdates.HourLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    elif n_15mins > 20:
        ax.xaxis.set_major_locator(mdates.MinuteLocator(interval=30))
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    elif n_10mins > 20:
        ax.xaxis.set_major_locator(mdates.MinuteLocator(interval=15))
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    elif n_5mins > 20:
        ax.xaxis.set_major_locator(mdates.MinuteLocator(interval=10))
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    elif n_mins > 20:
        ax.xaxis.set_major_locator(mdates.MinuteLocator(interval=5))
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    elif n_seconds > 20:
        ax.xaxis.set_major_locator(mdates.MinuteLocator(interval=1))
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    else:
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
        
        
        
def get_TCU_properties():
    return subprocess.run("python lst1_mongodb_tcu.py --list-available", shell=True)

def get_CaCo_collections(client_caco):
    all_databases = np.sort(client_caco.list_database_names())
    print(f"--- {'':^40s} ---")
    print(f"--- {'Available CaCo databases and collections':^40s} ---")
    print(f"--- {'':^40s} ---")
    for database in all_databases:
        print(f"\n--- {f'Database - {database}':^40s} ---")
        all_collections = np.sort(client_caco[database].list_collection_names())
        for i, collection in enumerate(all_collections):
            print(f"{i:4d} : {collection}")

def get_CaCo_properties(client_caco):
    caco_db = client_caco.CACO
    dict_caco_names = {}
    all_collections = np.sort(caco_db.list_collection_names())
    for coll_name in all_collections:
        coll = caco_db[coll_name]
        if coll not in ["STATE", "RUN_INFORMATION"] and "week" in coll_name:
        
            print(f"\n--- {f'Collection - {coll_name}':^40s} ---")
            names = coll.distinct("name")
            
            dict_caco_names[coll_name.replace("week", "min")] = names
            for i,n in enumerate(names):
                print(f"{i:4.0f} : {n}")

