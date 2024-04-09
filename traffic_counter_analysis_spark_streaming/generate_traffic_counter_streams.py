import pandas as pd
import time

input_file = "per-vehicle-records-2021-01-15.csv"
output_file_prefix = "counterdata"

def read_vehicle_data():
	print("Reading data")
	reader = pd.read_csv(input_file, sep=',', chunksize=10, iterator=True)
	for i, j in enumerate(reader):
	    df = next(reader)
	    output_file = output_file_prefix + str(i) + ".csv"
	    df.to_csv(output_file, index=False)
	    time.sleep(5) 
	    print("Finished chunk")


if __name__ == "__main__":
	read_vehicle_data()
	
