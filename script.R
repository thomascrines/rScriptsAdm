# Load functions
source("functions.R")

# Assign variables (provided by ADM team)
server <- ""
database <- ""
schema <- ""

# Assign dummy data for examples, can assign desired values as needed
table_name <- "ExampleUpload" # The name the table will have in the database 
dataframe <- iris             # The dataframe to upload

# Upload dataframe to database
adm_write_table_to_db(database = database, 
                      server = server, 
                      schema = schema, 
                      table_name = table_name, 
                      dataframe = dataframe)

# List tables currently in database
adm_db_tables(database = database, 
              server = server)

# View metadata of table in database
adm_db_table_metadata(database = database, 
                      server = server, 
                      schema = schema, 
                      table_name = table_name)

# Import entire table from database
table_from_db <- adm_read_table_from_db(database = database, 
                                        server = server, 
                                        schema = schema, 
                                        table_name = table_name)

# Import selected columns from table in database
columns <- c('Sepal.Length', 'Sepal.Width') # Populate with existing column names
table_from_db <- adm_read_table_from_db(database = database, 
                                        server = server, 
                                        schema = schema, 
                                        table_name = table_name, 
                                        columns = columns)

# Delete table completely from database
adm_drop_table_from_db(database = database, 
                       server = server, 
                       schema = schema, 
                       table_name = table_name)
