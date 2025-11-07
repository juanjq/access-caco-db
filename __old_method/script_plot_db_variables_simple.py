import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
from datetime import datetime, timedelta
import pymongo, sys

# Other external scripts
import utils

def main():

    print("\n" + "--" * 20)
    is_in_it_cluster = str(input("Are you running the script @ IT cluster? (\"No\"/\"Yes\"):"))
    if is_in_it_cluster == "No":
    	client_caco = pymongo.MongoClient("localhost:27018")
    elif is_in_it_cluster == "Yes":
    	client_caco = pymongo.MongoClient("lst101-int:27018") #("localhost:27018")
    else:
    	print(f"Answer should be \"No\" or \"Yes\", your input: {is_in_it_cluster}")
    	sys.exit()
    
    variable_caco = str(input("\nEnter variable name for CaCo: "))
    t1 = str(input("Enter start time in format (yyyy-mm-dd hh:mm:ss): "))
    t2 = str(input("Enter stop time in format (yyyy-mm-dd hh:mm:ss): "))
    print("\n\nQuerying dbs...\n")
    # Time for the query
    tstart = datetime.fromisoformat(t1)
    tstop  = datetime.fromisoformat(t2)    

    
    caco_db = client_caco.CACO
    dict_caco_names = {}
    all_collections = np.sort(caco_db.list_collection_names())
    for coll_name in all_collections:
        coll = caco_db[coll_name]
        if coll not in ["STATE", "RUN_INFORMATION"] and "week" in coll_name:

            names = coll.distinct("name")
            if len(names) > 1:
#                 print(f"\n--- {f'Collection - {coll_name}':^40s} ---")


                dict_caco_names[coll_name.replace("week", "min")] = names   
    
  
    t_extension  = timedelta(days=0)
    tstart_query = tstart - t_extension
    tstop_query  = tstop + t_extension

    if variable_caco != "":
        out_caco = utils.get_caco_entries(client_caco, variable_caco, dict_caco_names, tstart=tstart_query, tstop=tstop_query)
        date_caco, value_caco = out_caco["time"], out_caco["value"]
    else:
        date_caco, value_caco = [], []
    if len(date_caco) > 0:
        date_caco, value_caco = zip(*sorted(zip(date_caco, value_caco)))

    # Ceck if something is empty
    for var, vals, name in zip(
        [variable_caco], 
        [value_caco], 
        ["CaCo"]
    ):
        if var != None:
            print(f"For {name} {var} : {len(vals)} entries found")
 
    fig, ax = plt.subplots(1, 1, figsize=(16, 3.4))
    # Plotting in usual axis
    if len(date_caco) > 0:
        ax.plot(date_caco, value_caco, ds="steps-post", c="darkblue", ls="-", lw=1.5,
                label=f"CaCo: {out_caco['name']}")

    # Setting y-label
    if variable_caco != None:
        ax.set_ylabel(variable_caco)

    ax.legend(loc=6, prop={'size': 7})
    ax.grid(color="lightgray")
    ax.set_xlim(tstart, tstop)
    ax.set_title(f"{tstart} to {tstop}")
    # ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))

    plt.savefig(f"plot_simple_variables.png", dpi=300, bbox_inches='tight')
    plt.show()    
    print("\n\nFinished plotting. Check \"plot_simple_variables.png\"\n")
    
    

if __name__ == "__main__":
    main()
