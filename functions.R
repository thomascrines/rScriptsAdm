# The functions below will be updated as the package is refactored
# Functions with _i_ in name will be internal functions and shouldn't be run directly.

adm_i_create_db_connection <- function(database, server) {
  
  tryCatch({
    
    odbc::dbConnect(odbc::odbc(),
                    Driver = "SQL Server",
                    Trusted_Connection = "True",
                    DATABASE = database,
                    SERVER = server)},
    
    error = function(cond) {
      
      stop(paste0("Failed to create connection to database: '", database, "' on server: '", server, "'\nOriginal error message: '", cond, "'"))
      
    })
}

adm_i_execute_sql <- function(database, server, sql, output = FALSE) {
  
  output_data <- NULL
  
  connection <- adm_i_create_db_connection(database = database, server = server)
  
  if (output == TRUE) {
    
    output_data <- DBI::dbGetQuery(connection, sql)
    
  } else {
    
    DBI::dbGetQuery(connection, sql)
  }
  
  DBI::dbDisconnect(connection)
  
  output_data
}

adm_db_tables <- function(database, server) {
  
  sql <- "SELECT SCHEMA_NAME(t.schema_id) AS 'Schema',
  t.name AS 'Name'
  FROM sys.tables t
  WHERE t.temporal_type != 1"
  
  data <- adm_i_execute_sql(database = database, server = server, sql = sql, output = TRUE)
  
  data
}

adm_db_table_metadata <- function(database, server, schema, table_name) {
  
  sql <- paste0("SET NOCOUNT ON;
                DECLARE	@table_catalog nvarchar(128) = '", database, "',
                @table_schema nvarchar(128) = '", schema, "',
                @table_name nvarchar(128) = '", table_name, "';
                DECLARE @sql_statement nvarchar(2000),
                @param_definition nvarchar(500),
                @column_name nvarchar(128),
                @data_type nvarchar(128),
                @null_count int,
                @distinct_values int,
                @minimum_value nvarchar(225),
                @maximum_value nvarchar(225);
                DECLARE @T1 AS TABLE	(ColumnName nvarchar(128),
                DataType nvarchar(128),
                NullCount int,
                DistinctValues int,
                MinimumValue nvarchar(255),
                MaximumValue nvarchar(255));
                INSERT INTO @T1 (ColumnName, DataType)
                SELECT	COLUMN_NAME,
                REPLACE(CONCAT(DATA_TYPE, '(', CHARACTER_MAXIMUM_LENGTH, ')'), '()', '')
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE	TABLE_CATALOG = @table_catalog
                AND TABLE_SCHEMA = @table_schema
                AND TABLE_NAME = @table_name;
                DECLARE column_cursor CURSOR
                FOR SELECT ColumnName, DataType FROM @T1;
                OPEN column_cursor;
                FETCH NEXT FROM column_cursor
                INTO @column_name, @data_type;
                WHILE @@FETCH_STATUS = 0
                BEGIN
                SET @sql_statement =
                CONCAT(N'SET @null_countOUT =
                (SELECT COUNT(*)
                FROM [', @table_catalog, '].[', @table_schema, '].[', @table_name, ']
                WHERE [', @column_name, '] IS NULL)
                SET @distinct_valuesOUT =
                (SELECT COUNT(DISTINCT([', @column_name, ']))
                FROM [', @table_catalog, '].[', @table_schema, '].[', @table_name, ']
                WHERE [', @column_name, '] IS NOT NULL) ')
                IF (@data_type != 'bit')
                BEGIN
                SET @sql_statement =
                CONCAT(@sql_statement,
                'SET @minimum_valueOUT =
                CAST((SELECT MIN([', @column_name, '])
                FROM [', @table_catalog, '].[', @table_schema, '].[', @table_name, ']
                WHERE [', @column_name, '] IS NOT NULL)
                AS nvarchar(225))
                SET @maximum_valueOUT =
                CAST((SELECT MAX([', @column_name, '])
                FROM [', @table_catalog, '].[', @table_schema, '].[', @table_name, ']
                WHERE [', @column_name, '] IS NOT NULL)
                AS nvarchar(225))')
                END
                ELSE
                BEGIN
                SET @sql_statement =
                CONCAT(@sql_statement,
                'SET @minimum_valueOUT = NULL
                SET @maximum_valueOUT = NULL');
                END
                SET @param_definition = N'@null_countOUT int OUTPUT,
                @distinct_valuesOUT int OUTPUT,
                @minimum_valueOUT nvarchar(255) OUTPUT,
                @maximum_valueOUT nvarchar(255) OUTPUT';
                print(@sql_statement)
                EXECUTE sp_executesql	@sql_statement,
                @param_definition,
                @null_countOUT = @null_count OUTPUT,
                @distinct_valuesOUT = @distinct_values OUTPUT,
                @minimum_valueOUT = @minimum_value OUTPUT,
                @maximum_valueOUT = @maximum_value OUTPUT;
                UPDATE @T1
                SET NullCount = @null_count,
                DistinctValues = @distinct_values,
                MinimumValue = @minimum_value,
                MaximumValue = @maximum_value
                WHERE ColumnName = @column_name;
                FETCH NEXT FROM column_cursor
                INTO @column_name, @data_type;
                END
                CLOSE column_cursor;
                DEALLOCATE column_cursor;
                SELECT * FROM @T1;")
  
  data <- adm_i_execute_sql(database = database, server = server, sql = sql, output = TRUE)
  
  data
}

adm_i_r_to_sql_data_type <- function(r_data_type) {
  
  sql_data_type <- switch(r_data_type,
                          "numeric" = "float",
                          "logical" = "bit",
                          "character" = "varchar(255)",
                          "factor" = "varchar(255)",
                          "POSIXct" = "datetime2(3)",
                          "POSIXlt" = "datetime2(3)",
                          "integer" = "int")
  
  sql_data_type
}

adm_i_create_staging_table <- function(database, server, schema, table, dataframe) {
  
  columns <- sapply(dataframe, class)
  
  sql <- paste0("CREATE TABLE [", schema, "].[", table, "_staging_] (", table, "ID INT NOT NULL IDENTITY PRIMARY KEY,")
  
  for (column_index in seq_len(ncol(dataframe))) {
    
    column_name <- names(columns[column_index])
    data_type <- adm_i_r_to_sql_data_type(columns[[column_index]][1])
    
    sql <- paste0(sql, " [", column_name, "] ", data_type, ", ")
  }
  
  sql <- paste0(substr(sql, 1, nchar(sql) - 2), ");")
  
  adm_i_execute_sql(database = database, server = server, sql = sql, output = FALSE)
}

adm_i_create_table <- function(database, server, schema, table) {
  
  metadata <- adm_db_table_metadata(database = database, server = server, schema = schema, table = paste0(table, "_staging_"))
  
  sql <- paste0("CREATE TABLE [", schema, "].[", table, "] (", table, "ID INT NOT NULL IDENTITY PRIMARY KEY,")
  
  for (row in seq_len(nrow(metadata))) {
    
    column_name <- metadata[row, "ColumnName"]
    data_type <- metadata[row, "DataType"]
    
    sql <- paste0(sql, " [", column_name, "] ", data_type, ", ")
  }
  
  sql <- paste0(substr(sql, 1, nchar(sql) - 2), ");")
  
  adm_i_execute_sql(database = database, server = server, sql = sql, output = FALSE)
}

adm_i_create_versioned_table <- function(database, server, schema, table) {
  
  metadata <- adm_db_table_metadata(database = database, server = server, schema = schema, table = paste0(table, "_staging_"))
  
  sql <- paste0("CREATE TABLE [", schema, "].[", table, "] (", table, "ID INT NOT NULL IDENTITY PRIMARY KEY,")
  
  for (row in seq_len(nrow(metadata))) {
    
    column_name <- metadata[row, "ColumnName"]
    data_type <- metadata[row, "DataType"]
    
    sql <- paste0(sql, " [", column_name, "] ", data_type, ", ")
  }
  
  sql <- paste0(sql,
                "SysStartTime DATETIME2 GENERATED ALWAYS AS ROW START NOT NULL, ",
                "SysEndTime DATETIME2 GENERATED ALWAYS AS ROW END NOT NULL, ",
                "PERIOD FOR SYSTEM_TIME (SysStartTime, SysEndTime)) ",
                "WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = [", schema, "].[", table, "History]));")
  
  connection <- adm_i_create_db_connection(database = database, server = server)
  
  DBI::dbGetQuery(connection, sql)
  
  DBI::dbDisconnect(connection)
}

adm_i_populate_staging_table <- function(database, server, schema, table, dataframe, overwrite = TRUE, append = FALSE) {
  
  connection <- adm_i_create_db_connection(database = database, server = server)
  
  tables <- adm_db_tables(database = database, server = server)
  
  if (nrow(tables[tables$Schema == schema & tables$Name == paste0(table, "_staging_"), ]) == 0) {
    
    adm_i_create_staging_table(database = database, server = server, schema = schema, table = table, dataframe = dataframe)
    
    overwrite <- TRUE
  }
  
  tryCatch({
    
    DBI::dbWriteTable(connection, name = DBI::Id(schema = schema, table = paste0(table, "_staging_")), value = dataframe, overwrite = overwrite, append = append)
    
    message(paste0("Staging successfully written to database"))
    
  }, error = function(cond) {
    
    stop(paste0("Failed to write staging data to database.\nOriginal error message: ", cond))
  })
  
  DBI::dbDisconnect(connection)
}

adm_i_populate_table_from_staging <- function(database, server, schema, table) {
  
  metadata <- adm_db_table_metadata(database = database, server = server, schema = schema, table = paste0(table, "_staging_"))
  
  column_string <- ""
  
  for (row in seq_len(nrow(metadata))) {
    
    column_name <- metadata[row, 1]
    
    column_string <- paste0(column_string, " [", column_name, "], ")
  }
  
  column_string <- substr(column_string, 1, nchar(column_string) - 2)
  
  sql <- paste0("DELETE FROM [", schema, "].[", table, "];
                
                INSERT INTO [", schema, "].[", table, "] (", column_string, ") select ", column_string, " from [", schema, "].[", table, "_staging_];")
  
  adm_i_execute_sql(database = database, server = server, sql = sql, output = FALSE)
}

adm_i_delete_staging_table <- function(database, server, schema, table) {
  
  connection <- adm_i_create_db_connection(database = database, server = server)
  
  tryCatch({
    
    odbc::dbRemoveTable(conn = connection, DBI::Id(schema = schema, table = paste0(table, "_staging_")))
    
  }, error = function(cond) {
    
    stop(paste0("Failed to delete staging table: '", table, "' from database: '", database, "' on server: '", server, "'\nOriginal error message: ", cond))
  })
  
  DBI::dbDisconnect(connection)
  
  message("Staging table: '", table, "' successfully deleted from database: '", database, "' on server '", server, "'")
}

adm_write_table_to_db <- function(database, server, schema, table_name, dataframe) {
  
  adm_i_populate_staging_table(database = database, server = server, schema = schema, table = table_name, dataframe = dataframe)
  
  connection <- adm_i_create_db_connection(database = database, server = server)
  
  tables <- adm_db_tables(database = database, server = server)
  
  if (nrow(tables[tables$Schema == schema & tables$Name == table_name, ]) == 0) {
    
    adm_i_create_table(database = database, server = server, schema = schema, table = table_name)
  }
  
  tryCatch({
    
    adm_i_populate_table_from_staging(database = database, server = server, schema = schema, table = table_name)
    
    message(paste0("Dataframe successfully written to: '", table_name, "'"))
    
  }, error = function(cond) {
    
    stop(paste0("Failed to write dataframe to database: '", database, "'\nOriginal error message: ", cond))
  })
  
  adm_i_delete_staging_table(database = database, server = server, schema = schema, table = table_name)
  
  DBI::dbDisconnect(connection)
}

adm_i_select_list <- function(columns) {
  
  select_list <- ""
  
  if (is.null(columns)) {
    
    select_list <- "*"
    
  } else {
    
    for (column in columns) {
      
      select_list <- paste0(select_list, "[", column, "], ")
    }
    select_list <- substr(select_list, 1, nchar(select_list) - 2)
  }
  
  select_list
}

adm_i_where_clause <- function(id_column, start_row, end_row) {
  
  dplyr::case_when(
    is.null(start_row) & is.null(end_row) ~ "",
    is.null(start_row) & !is.null(end_row) ~ paste0(" WHERE [", id_column, "] <= ", end_row),
    !is.null(start_row) & is.null(end_row) ~ paste0(" WHERE [", id_column, "] >= ", start_row),
    !is.null(start_row) & !is.null(end_row) ~ paste0(" WHERE [", id_column, "] BETWEEN ", start_row, " AND ", end_row)
  )
}

adm_read_table_from_db <- function(database, server, schema, table_name, columns = NULL) {
  
  id_column <- paste0(table_name, "ID") 
  select_list <- adm_i_select_list(columns)
  
  sql <- paste0("SELECT ", select_list,
                " FROM [", schema, "].[", table_name, "];")
  
  adm_i_execute_sql(database = database, server = server, sql = sql, output = TRUE) 
}

adm_drop_table_from_db <- function(database, server, schema, table_name) {
  
  sql <- paste0("DROP TABLE [", schema, "].[", table_name, "]")
  
  adm_i_execute_sql(database = database, server = server, sql = sql, output = TRUE)
  
  message("Table: '", table_name, "' successfully deleted from database: '", database, "' on server '", server, "'")
}
