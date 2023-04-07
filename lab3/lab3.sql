create or replace PROCEDURE MOVE_TABLE (
    dev_schema_name IN VARCHAR2,
    prod_schema_name IN VARCHAR2,
    table_name IN VARCHAR2
) IS 
  table_exists NUMBER;
BEGIN
    SELECT COUNT(*) INTO table_exists FROM all_tables WHERE table_name = move_table.table_name AND owner = prod_schema_name;
    IF table_exists > 0  THEN
        EXECUTE IMMEDIATE 'DROP TABLE ' || prod_schema_name || '.' || table_name;
        DBMS_OUTPUT.PUT_LINE('INFO: drop table ' ||  table_name || ' from schema: ' ||  prod_schema_name);
    END IF;  
    EXECUTE IMMEDIATE 'CREATE TABLE ' || prod_schema_name ||  '.' || table_name || ' AS SELECT * FROM '  || dev_schema_name ||  '.' ||  table_name;
    DBMS_OUTPUT.PUT_LINE('INFO: create table '  || table_name || ' in schema: ' || prod_schema_name);
END MOVE_TABLE;


create or replace PROCEDURE MOVE_PROCEDURE (
  p_source_schema VARCHAR2,
  p_target_schema VARCHAR2,
  p_procedure_name VARCHAR2
) AS
  query_string VARCHAR2(300);
BEGIN
    FOR src IN (SELECT line, text FROM ALL_SOURCE WHERE OWNER = p_source_schema AND NAME = p_procedure_name) LOOP
    IF src.line = 1 THEN
    query_string := 'CREATE OR REPLACE ' || REPLACE(src.text, p_procedure_name, p_target_schema || '.'  || p_procedure_name);
    
    ELSE
    
    query_string := query_string || src.text;
    END IF;
    END LOOP;
    EXECUTE IMMEDIATE query_string;

END;

create or replace PROCEDURE MOVE_PACKAGE(
    dev_schema_name IN VARCHAR2,
    prod_schema_name IN VARCHAR2,
    package_name IN VARCHAR2
) IS
    package_exists NUMBER;
    package_spec CLOB;
    query_string VARCHAR2(500);
BEGIN
    FOR src IN (SELECT line, text FROM ALL_SOURCE WHERE OWNER = dev_schema_name AND NAME = package_name) LOOP
    IF src.line = 1 THEN
    query_string := 'CREATE OR REPLACE ' ||  REPLACE(src.text, package_name, prod_schema_name || '.' ||  package_name);
    
    ELSE
    
    query_string := query_string || src.text;
    END IF;
    END LOOP;
    EXECUTE IMMEDIATE query_string;

END MOVE_PACKAGE;

create or replace PROCEDURE MOVE_INDEX (
    dev_schema_name IN VARCHAR2,
    prod_schema_name IN VARCHAR2,
    index_name IN VARCHAR2
) IS 
  index_exists NUMBER;
  index_table_name VARCHAR2(255);
  idx_cols VARCHAR2(4000);
BEGIN
    -- Check if the index exists in the target schema
    SELECT COUNT(*) INTO index_exists FROM all_indexes WHERE index_name = MOVE_INDEX.index_name AND owner = prod_schema_name;
    IF index_exists > 0 THEN
        EXECUTE IMMEDIATE 'DROP INDEX ' || prod_schema_name || '.' || index_name;
        DBMS_OUTPUT.PUT_LINE('INFO: drop index ' || index_name || ' from schema: ' || prod_schema_name);
    END IF;
    
    -- Get the name of the table that the index is on in the source schema
    SELECT table_name INTO index_table_name FROM all_indexes WHERE index_name = MOVE_INDEX.index_name AND owner = dev_schema_name AND ROWNUM = 1;
    
    -- Get the list of columns in the index
    SELECT LISTAGG(column_name, ',') WITHIN GROUP (ORDER BY column_position)
    INTO idx_cols
    FROM all_ind_columns
    WHERE index_owner = dev_schema_name AND index_name = MOVE_INDEX.index_name AND column_position > 0 AND column_name IS NOT NULL;
    -- Create the index on the table in the target schema
    EXECUTE IMMEDIATE 'CREATE INDEX ' || prod_schema_name || '.' || index_name || ' ON ' || prod_schema_name || '.' || index_table_name || '(' || idx_cols || ')';
    DBMS_OUTPUT.PUT_LINE('INFO: create index ' || index_name || ' in schema: ' || prod_schema_name);
END MOVE_INDEX;


create or replace PROCEDURE MOVE_FUNC (
  p_source_schema VARCHAR2,
  p_target_schema VARCHAR2,
  p_procedure_name VARCHAR2
) AS
  query_string VARCHAR2(300);
BEGIN
    FOR src IN (SELECT line, text FROM ALL_SOURCE WHERE OWNER = p_source_schema AND NAME = p_procedure_name) LOOP
    IF src.line = 1 THEN
    query_string := 'CREATE OR REPLACE ' || REPLACE(src.text, p_procedure_name, p_target_schema || '.' ||  p_procedure_name);
    
    ELSE
    
    query_string := query_string || src.text;
    END IF;
    END LOOP;
    EXECUTE IMMEDIATE query_string;

END;


create or replace FUNCTION check_circular_reference(p_schema_name IN VARCHAR2, p_table_name IN VARCHAR2)
RETURN BOOLEAN
IS
    v_sql VARCHAR2(1000);
    v_found BOOLEAN := FALSE;
BEGIN
    v_sql := 'SELECT COUNT(*) FROM (
                SELECT level, owner, table_name, constraint_name, r_owner, r_constraint_name
                FROM all_constraints
                WHERE constraint_type = ''R''
                START WITH table_name = ''' ||  p_table_name || ''' AND owner = ''' || p_schema_name || '''
                CONNECT BY NOCYCLE PRIOR r_owner = owner AND PRIOR r_constraint_name = constraint_name
            )';
    EXECUTE IMMEDIATE v_sql INTO v_found;
    RETURN v_found;
END;

create or replace FUNCTION compare_functions_procedures(p_schema1 IN VARCHAR2, p_func_name1 IN VARCHAR2, p_schema2 IN VARCHAR2, p_func_name2 IN VARCHAR2)
RETURN BOOLEAN
IS
  func_body1 VARCHAR2(10000);
  func_body2 VARCHAR2(10000);
BEGIN
    SELECT text INTO func_body1 FROM all_source WHERE owner = p_schema1 AND name = p_func_name1 AND type in ('FUNCTION', 'PROCEDURE') AND ROWNUM = 1 ORDER BY line;
    SELECT text INTO func_body2 FROM all_source WHERE owner = p_schema2 AND name = p_func_name2 AND type in ('FUNCTION', 'PROCEDURE') AND ROWNUM = 1 ORDER BY line;

    RETURN func_body1 = func_body2;
END;


create or replace FUNCTION compare_indexes(
  schema1 IN VARCHAR2,
  index_name1 IN VARCHAR2,
  schema2 IN VARCHAR2,
  index_name2 IN VARCHAR2
) RETURN BOOLEAN
IS
  idx1_cols VARCHAR2(4000);
  idx2_cols VARCHAR2(4000);
BEGIN
  -- Get the visible columns for index1
  SELECT LISTAGG(column_name, ',') WITHIN GROUP (ORDER BY column_position)
  INTO idx1_cols
  FROM all_ind_columns
  WHERE index_owner = schema1
    AND index_name = index_name1
    AND column_position > 0
    AND column_name IS NOT NULL;

  -- Get the visible columns for index2
  SELECT LISTAGG(column_name, ',') WITHIN GROUP (ORDER BY column_position)
  INTO idx2_cols
  FROM all_ind_columns
  WHERE index_owner = schema2
    AND index_name = index_name2
    AND column_position > 0
    AND column_name IS NOT NULL;

  -- Compare the visible columns only
  RETURN idx1_cols = idx2_cols;
END;


create or replace FUNCTION compare_packages(p_schema1 IN VARCHAR2, p_func_name1 IN VARCHAR2, p_schema2 IN VARCHAR2, p_func_name2 IN VARCHAR2)
RETURN BOOLEAN
IS
  func_body1 VARCHAR2(10000);
  func_body2 VARCHAR2(10000);
BEGIN
    SELECT text INTO func_body1 FROM all_source WHERE owner = p_schema1 AND name = p_func_name1 AND type in ('PACKAGE') AND ROWNUM = 1 ORDER BY line;
    SELECT text INTO func_body2 FROM all_source WHERE owner = p_schema2 AND name = p_func_name2 AND type in ('PACKAGE') AND ROWNUM = 1 ORDER BY line;

    RETURN func_body1 = func_body2;
END;



create or replace NONEDITIONABLE PROCEDURE compare_objects (
    dev_schema_name IN VARCHAR2,
    prod_schema_name IN VARCHAR2
)
IS
    sql_stmt VARCHAR2(4000);
    TYPE object_list IS TABLE OF VARCHAR2(255);
    dev_objects object_list;
    temp_count NUMBER;
    v_has_circular_reference BOOLEAN;
    func_proc_equal BOOLEAN;
    indexes_equal BOOLEAN;
BEGIN
    -- Truncate temp table
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TEMP_TABLE';


    -- Insert missing tables into temp table
    FOR rec IN (SELECT table_name FROM ALL_TABLES WHERE OWNER=dev_schema_name AND table_name NOT IN (SELECT table_name FROM ALL_TABLES WHERE OWNER=prod_schema_name)) LOOP
        sql_stmt := 'INSERT INTO TEMP_TABLE (object_name) VALUES (:1)';
        EXECUTE IMMEDIATE sql_stmt USING rec.table_name;
        MOVE_TABLE(dev_schema_name, prod_schema_name, rec.table_name);
    END LOOP;
    
    -- Drop tables from prod which are not from dev
    FOR rec IN (SELECT table_name FROM ALL_TABLES WHERE OWNER=prod_schema_name AND table_name NOT IN (SELECT table_name FROM ALL_TABLES WHERE OWNER=dev_schema_name)) LOOP
        sql_stmt := 'DROP TABLE ' || prod_schema_name || '.' ||  rec.table_name;
        EXECUTE IMMEDIATE sql_stmt;
    END LOOP;


    -- Check for tables with different structure
    FOR rec IN (
        SELECT DISTINCT a.table_name
        FROM all_tab_columns a, all_tab_columns b
        WHERE a.table_name = b.table_name
        AND a.owner = dev_schema_name
        AND b.owner = prod_schema_name
        AND (a.column_name != b.column_name
        OR a.data_type != b.data_type
        OR a.data_length != b.data_length)
    ) LOOP
        -- Insert table into temp table
        sql_stmt := 'INSERT INTO TEMP_TABLE(object_name) VALUES (:1)';
        EXECUTE IMMEDIATE sql_stmt USING rec.table_name;
        MOVE_TABLE(dev_schema_name, prod_schema_name, rec.table_name);
    END LOOP;

    -- Check for foreign key constraints
    FOR rec IN (SELECT table_name FROM ALL_TABLES WHERE OWNER=dev_schema_name) LOOP
        v_has_circular_reference := check_circular_reference(dev_schema_name, rec.table_name);
        IF v_has_circular_reference THEN
            DBMS_OUTPUT.PUT_LINE('Circular reference detected in table: ' || rec.table_name || ' of schema: ' || dev_schema_name);
        ELSE
            DBMS_OUTPUT.PUT_LINE('No circular reference detected in table: ' || rec.table_name || ' of schema: ' || dev_schema_name);
        END IF;
    END LOOP;
    
    
    FOR rec IN (SELECT index_name FROM all_indexes WHERE OWNER=prod_schema_name MINUS SELECT index_name FROM all_indexes WHERE OWNER=dev_schema_name) LOOP
        EXECUTE IMMEDIATE 'DROP INDEX ' || prod_schema_name || '.' ||  rec.index_name;
    END LOOP;
    

    -- Indexes
    FOR rec IN (SELECT index_name FROM all_indexes WHERE OWNER=dev_schema_name MINUS SELECT index_name FROM all_indexes WHERE OWNER=prod_schema_name) LOOP
        sql_stmt := 'INSERT INTO TEMP_TABLE (object_name) VALUES (:1)';
        EXECUTE IMMEDIATE sql_stmt USING rec.index_name;
        MOVE_INDEX(dev_schema_name, prod_schema_name, rec.index_name);
    END LOOP;
    
        
    FOR rec IN (SELECT index_name FROM all_indexes WHERE OWNER=dev_schema_name AND index_name IN (SELECT index_name FROM all_indexes WHERE OWNER=prod_schema_name)) LOOP
    indexes_equal  := compare_indexes(dev_schema_name, rec.index_name, prod_schema_name, rec.index_name);
    IF indexes_equal THEN
        dbms_output.put_line(' ');
    ELSE
        sql_stmt := 'INSERT INTO TEMP_TABLE (object_name) VALUES (:1)';
        EXECUTE IMMEDIATE sql_stmt USING rec.index_name;
        MOVE_INDEX(dev_schema_name, prod_schema_name, rec.index_name);
    END IF;
    END LOOP;
    FOR rec IN (SELECT object_name FROM all_objects WHERE OWNER=prod_schema_name AND object_type = 'PACKAGE' MINUS SELECT object_name FROM all_objects WHERE OWNER=dev_schema_name and object_type = 'PACKAGE') LOOP
        EXECUTE IMMEDIATE 'DROP PACKAGE ' ||  prod_schema_name || '.'  || rec.object_name;
    END LOOP;
    
    -- Packages
    FOR rec IN (SELECT object_name FROM all_objects WHERE OWNER=dev_schema_name AND object_type = 'PACKAGE' MINUS SELECT object_name FROM all_objects WHERE OWNER=prod_schema_name and object_type = 'PACKAGE') LOOP
        sql_stmt := 'INSERT INTO TEMP_TABLE (object_name) VALUES (:1)';
        EXECUTE IMMEDIATE sql_stmt USING rec.object_name;
        MOVE_PACKAGE(dev_schema_name, prod_schema_name, rec.object_name);
    END LOOP;
    
    FOR rec IN (SELECT object_name FROM all_objects WHERE OWNER=dev_schema_name AND object_type in ('PACKAGE') AND object_name in (SELECT object_name FROM all_objects WHERE OWNER=prod_schema_name and object_type in ('PACKAGE'))) LOOP
    func_proc_equal  := compare_packages(dev_schema_name, rec.object_name, prod_schema_name, rec.object_name);
    IF func_proc_equal THEN
        dbms_output.put_line(' ');
    ELSE
        sql_stmt := 'INSERT INTO TEMP_TABLE (object_name) VALUES (:1)';
        EXECUTE IMMEDIATE sql_stmt USING rec.object_name;
        MOVE_PACKAGE(dev_schema_name, prod_schema_name, rec.object_name);
    END IF;
    END LOOP;
    
    FOR rec IN (SELECT object_name FROM all_objects WHERE OWNER=prod_schema_name AND object_type in ('PROCEDURE') MINUS SELECT object_name FROM all_objects WHERE OWNER=dev_schema_name and object_type in ('PROCEDURE')) LOOP
        EXECUTE IMMEDIATE 'DROP PROCEDURE ' ||  prod_schema_name || '.'  || rec.object_name;
    END LOOP;
    
    FOR rec IN (SELECT object_name FROM all_objects WHERE OWNER=prod_schema_name AND object_type in ('FUNCTION') MINUS SELECT object_name FROM all_objects WHERE OWNER=dev_schema_name and object_type in ('FUNCTION')) LOOP
        EXECUTE IMMEDIATE 'DROP FUNCTION ' || prod_schema_name || '.' ||  rec.object_name;
    END LOOP;
    -- Procedures and functions
    FOR rec IN (SELECT object_name FROM all_objects WHERE OWNER=dev_schema_name AND object_type in ('PROCEDURE') MINUS SELECT object_name FROM all_objects WHERE OWNER=prod_schema_name and object_type in ('PROCEDURE')) LOOP
        sql_stmt := 'INSERT INTO TEMP_TABLE (object_name) VALUES (:1)';
        EXECUTE IMMEDIATE sql_stmt USING rec.object_name;
        MOVE_PROCEDURE(dev_schema_name, prod_schema_name, rec.object_name);
    END LOOP;
    FOR rec IN (SELECT object_name FROM all_objects WHERE OWNER=dev_schema_name AND object_type in ('FUNCTION') MINUS SELECT object_name FROM all_objects WHERE OWNER=prod_schema_name and object_type in ('FUNCTION')) LOOP
        sql_stmt := 'INSERT INTO TEMP_TABLE (object_name) VALUES (:1)';
        EXECUTE IMMEDIATE sql_stmt USING rec.object_name;
        MOVE_FUNC(dev_schema_name, prod_schema_name, rec.object_name);
    END LOOP;
    
    FOR rec IN (SELECT object_name FROM all_objects WHERE OWNER=dev_schema_name AND object_type in ('PROCEDURE') AND object_name in (SELECT object_name FROM all_objects WHERE OWNER=prod_schema_name and object_type in ('PROCEDURE'))) LOOP
    func_proc_equal  := compare_functions_procedures(dev_schema_name, rec.object_name, prod_schema_name, rec.object_name);
    IF func_proc_equal THEN
        dbms_output.put_line(' ');
    ELSE
        sql_stmt := 'INSERT INTO TEMP_TABLE (object_name) VALUES (:1)';
        EXECUTE IMMEDIATE sql_stmt USING rec.object_name;
        MOVE_PROCEDURE(dev_schema_name, prod_schema_name, rec.object_name);
    END IF;
    END LOOP;
    
    
    FOR rec IN (SELECT object_name FROM all_objects WHERE OWNER=dev_schema_name AND object_type in ('FUNCTION') AND object_name in (SELECT object_name FROM all_objects WHERE OWNER=prod_schema_name and object_type in ('FUNCTION'))) LOOP
    func_proc_equal  := compare_functions_procedures(dev_schema_name, rec.object_name, prod_schema_name, rec.object_name);
    IF func_proc_equal THEN
        dbms_output.put_line(' ');
    ELSE
        sql_stmt := 'INSERT INTO TEMP_TABLE (object_name) VALUES (:1)';
        EXECUTE IMMEDIATE sql_stmt USING rec.object_name;
        MOVE_FUNC(dev_schema_name, prod_schema_name, rec.object_name);
    END IF;
    END LOOP;


--     Remove duplicates and order by dependency
    EXECUTE IMMEDIATE 'SELECT DISTINCT object_name FROM TEMP_TABLE ORDER BY object_name' BULK COLLECT INTO dev_objects;

    -- Output result
    IF dev_objects.COUNT > 0 THEN
        dbms_output.put_line('Missing objects:');
        FOR i IN 1..dev_objects.COUNT LOOP
            DBMS_OUTPUT.PUT_LINE(dev_objects(i));
        END LOOP;
    ELSE
        DBMS_OUTPUT.PUT_LINE('No missing objects found.');
    END IF;


    -- Truncate temp table
--    EXECUTE IMMEDIATE 'TRUNCATE TABLE TEMP_TABLE2';
END;