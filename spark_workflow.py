# Step-1: Create the list of values for slicing the data (let's say, df corresponds to one SO)
product_codes = list(set(df.Product_Code))
agent_codes = list(set(df.Agent_Code))

# Step-2: Create a broadcast variable for our data
dfBroadcast = sc.broadcast(df)

# Step-3: Create a list of tasks to distribute
model_tasks = [] # Model tasks
for product_code in product_codes:
   model_tasks = model_tasks + [product_code]
   
agg_tasks = [] # Aggregation tasks
for product_code in product_codes:
   for agent_code in agent_codes:
      agg_tasks = agg_tasks + [(product_code, agent_code)]
      
# Step-4: Define method to run on one slice of the data
def modelOneSlice(product_code): # Model task
   dfLocal = dfBroadcast[dfBroadcast.Product_Code==product_code].copy()
   # Do your stuff here on dfLocal (feature engineering, modelling)
   # Dump transformed dataframe and trained model
   # Return nothing
   
def aggOneSlice(product_code, agent_code): # Aggregation task
   # Read from dumped transformed dataframe and trained model, corresponding to product_code
   # Filter data corresponding to agent_code
   # Do your stuff here (hyper-parameter tuning, aggregating)
   # Dump output data
   # Return nothing

# Step-5: Run model tasks
model_tasks_RDD = sc.parallelize(model_tasks, numSlices=len(model_tasks))
model_tasks_RDD.map(lambda product: modelOneSlice(product)).count()

# Step-6: Run aggregation tasks
agg_tasks_RDD = sc.parallelize(agg_tasks, numSlices=len(agg_tasks))
agg_tasks_RDD.map(lambda product_agent: aggOneSlice(product_agent[0], product_agent[1])).count()

# Step-7: Clean up the broadcast variables
dfBroadcast.unpersist()

# Step-8: Report
# Read from the dumped output files and prepare reports

