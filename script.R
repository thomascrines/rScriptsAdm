# Populate with values provided by ADM team
server <- ""
database <- ""
schema <- ""

# Name you want table to have
table_name <- ""

# List tables currently in database
adm_list_tables(database, server)

# View metadata of table in database
metadata <- adm_metadata_columns(database, server, schema, table_name)

# Import entire table from database
table_from_db <- adm_import_table(database, server, schema, table_name)

# Import selected columns from table in database
columns <- c('ColumnName1', 'ColumnName2') # Populate with existing column names
table_from_db <- adm_import_table(database, server, schema, table_name, columns)

# Import selected columns from table in database between selected rows
columns <- c('ColumnName1', 'ColumnName2') # Populate with existing column names
start_row <- 1 # If NULL will start from beginning of table
end_row <- 3 # If NULL will end at end of table
table_from_db <- adm_import_table(database, server, schema, table_name, columns, start_row, end_row)

# Example loop to bring back selected columns from selected tables
columns <- c('ColumnName1', 'ColumnName2') # Populate with existing column names
tables <- c('TableName1', 'TableName2') # Populate with existing table names

for (table in tables) {
  
  table_name <- table
  table_from_db <- adm_import_table(database, server, schema, table_name, columns)
  assign(table_name, table_from_db)
}

# Upload dataframe to database
table_name <- "ExampleUpload"
dataframe <- iris
adm_upload_dataframe(database, server, schema, table_name, dataframe)
