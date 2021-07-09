import database
import time
#get the time of when the program started running
start_time = time.time()
test = database.Database("nsf_roadtraffic_friction_v2")
table = "friction_measurement_uml_avar"
attributes = ["id","unix_time", "FL_friction_noisy"]
print("--- %s seconds ---" % (time.time() - start_time))
data = test.select( table, attributes,limit=5)
print("--- %s seconds ---" % (time.time() - start_time))
print(data[0][0])
# for x in data[0]:
#     test.insert_execute_values_iterator("friction_measurement_uml_insertion_test",["id", "unix_time", "FL_friction_noisy"], x )
test.close_DB_connection()