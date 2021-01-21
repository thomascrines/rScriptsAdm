server <- "s0196a\\ADM"
# database <- "EducationSchoolLeavers"
database <- "WorkspaceU442427"
schema <- "schoolleaverpublicationdata"

table_name <- "Leavers_followup_200910"

# List tables currently in database
adm_list_tables(database, server)

# View metadata of table in database
metadata <- adm_metadata_columns(database, server, schema, table_name)

# Import table from database
table_from_db <- adm_import_table(database, server, schema, table_name)

# Import selected columns from table in database
columns <- c('foll_org', 'dest_org', 'urbrur6j', 'urbrur8j')
table_from_db <- adm_import_table(database, server, schema, table_name, columns)

# Example loop to bring back selected columns from selected tables
columns <- c('foll_org', 'dest_org', 'urbrur6j', 'urbrur8j')
tables <- c('Leavers_followup_200910', 'Leavers_followup_201011')

for (table in tables) {
  
  table_name <- table
  table_from_db <- adm_import_table(database, server, schema, table_name, columns)
  assign(table_name, table_from_db)
}

# Upload dataframe to database
table_name <- "ExampleUpload"
dataframe <- iris
adm_upload_dataframe(database, server, schema, table_name, dataframe)
