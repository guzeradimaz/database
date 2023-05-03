SET SQLBLANKLINES ON

DECLARE
  out_File  UTL_FILE.FILE_TYPE;
  lc_dir_name VARCHAR2(6) := 'RTVMS2';
  lc_file_name VARCHAR(8) := 'test.txt';
  lc_text_input NCLOB := '{''script'':[
	{''select'': {
		''columns'': [''id'', ''name'', ''surname''],
		''tables'': [''students'', ''groups''],
        ''conditions'': {
                ''WHERE'': {
                    ''and'':[
                        {''id'': {
                            ''operator'': ''>'',
                            ''value'': 2
                        }},
                        {''name'': {
                             ''has_not'': ''NOT'',
                            ''operator'':''like'',
                            ''condition'': ''T%''
                        }},
                        {''group_id'': {
                            ''has_not'': ''NOT'',
                            ''operator'':''in'',
                            ''condition'': [2, 4]
                        }}
                    ],
                ''or'': [
                    {''group_id'': {
                        ''operator'':''BETWEEN'',
                        ''condition'': [1, 3]
                    }}
                ]
                },
                ''LEFT JOIN'': {
                    ''from_table'': ''groups'',
                    ''from_column'': ''id'',
                    ''to_table'': ''students'',
                    ''to_column'': ''id''
                }
		}
	}},	
    {''select'': {
		''columns'': [''id'', ''name'', ''surname'', ''groups.id''],
		''tables'': [''students'', ''groups''],
        ''conditions'': {
                ''WHERE'': {
                    ''not exists'': {
                            ''select'': {
                                ''columns'': [''id'', ''name'', ''surname''],
                                ''tables'': [''students'']
                                }
                    }
                }
        }
    }},
    {''select'': {
		''columns'': [''id'', ''name'', ''surname'', ''groups.id''],
		''tables'': [''students''],
        ''conditions'': {
                ''WHERE'': {
                ''not in'': [
                    {''group_id'': {
                        ''value'': {   
                                ''select'': {
                                    ''columns'': [''id'', ''name''],
                                    ''tables'': [''groups'']
                                }
                            }
                    }}
                ]
                }
            }
    }},
	{''create'': {		
		''groups'': {
			''columns'': {
				''id'': {
                    ''type'': ''number'',
                    ''allow_null'': ''NOT NULL''
                },
				''name'': {
					''type'': ''varchar2'',
					''size'': 50,
                    ''allow_null'': ''NOT NULL''
				}
			},
			''constraints'': {
				''group_pk'': {
                    ''PRIMARY KEY'': [''id'']
                }
			}	
        },
        ''students'': {
			''columns'': {
				''id'': {
                    ''type'': ''number'',
                    ''allow_null'': ''NOT NULL''
                },
				''name'': {
					''type'': ''varchar2'',
					''size'': 50,
                    ''allow_null'': ''NOT NULL''
				},
                ''surname'': {
					''type'': ''varchar2'',
					''size'': 50,
                    ''allow_null'': ''NOT NULL''
				},
                ''group_id'': {
                    ''type'': ''number'',
                    ''allow_null'': ''NOT NULL''
                }
			},
			''constraints'': {
				''students_pk'': {
                    ''PRIMARY KEY'': [''id'',''name'']
                },
                ''student_group_fk'': {
                    ''FOREIGN KEY'': {
                        ''columns'': [''group_id'', ''group_name''],
                        ''table_name'': ''groups'',
                        ''columns_from'': [''id'', ''name'']
                    }
                }
			}	
        }
	}},
    {''insert into'': {
		''table'': ''students'',
		''columns'': [''id'', ''name'', ''surname'', ''group_id''],
		''values'': [1, ''Tanusha'', ''Shurko'', ''053501'']
	}},
    {''insert into'': {
		''table'': ''students'',
		''columns'': [''id'', ''name'', ''surname'', ''group_id''],
		 ''select'': {
                ''columns'': [''*''],
                ''tables'': [''students'']
            }
	}},
    {''update'': {
		''table'': ''students'',
		''values'': [{''id'': 1},{''name'':''Tanusha''}],
         ''WHERE'': {
                    ''and'':[
                        {''id'': {
                            ''operator'': ''>'',
                            ''value'': 2
                        }}
                    ]}
	}},
    {''update'': {
		''table'': ''students'',
		''columns'': [''name'', ''surname'', ''group_id''],
        ''select'': {
                ''columns'': [''name'', ''surname'', ''group_id''],
                ''tables'': [''students'']
            },
        ''WHERE'': {
                    ''exists'': {
                            ''select'': {
                                ''columns'': [''id'', ''name'', ''surname''],
                                ''tables'': [''students'']
                                }
                    }
                }
	}},
    {''delete'': {
		''table'': ''students''
	}},
    {''delete'': {
		''table'': ''students'',
        ''WHERE'': {
                ''in'': [
                    {''group_id'': {
                        ''value'': {   
                                ''select'': {
                                    ''columns'': [''id'', ''name''],
                                    ''tables'': [''groups''],
                                     ''conditions'':{''WHERE'': {
                                             ''and'':[
                        {''id'': {
                            ''operator'': ''>'',
                            ''value'': 2
                        }}
                    ]}
                                }
                            }
                            }
                    }}
                ]
       }
	}},
    {''drop'': {
        ''tables'': [''students'', ''groups'']
    }}
]}';
BEGIN
  out_File := UTL_FILE.FOPEN(lc_dir_name, lc_file_name , 'W');

  UTL_FILE.PUT_LINE(out_file , lc_text_input);
  UTL_FILE.FCLOSE(out_file);
END;

create or replace function parse_json_drop_object(js_obj JSON_OBJECT_T)
RETURN NCLOB
IS
lv_result_query NCLOB;

lv_ja JSON_ARRAY_T;
lv_ja_tables JSON_ARRAY_T;
lv_keys JSON_KEY_LIST;
BEGIN
    lv_ja := new JSON_ARRAY_T;
    
    lv_keys := js_obj.get_keys;
    FOR i IN 1..lv_keys.COUNT LOOP
        lv_ja_tables := js_obj.get_array(lv_keys(i));
        FOR j IN 0..lv_ja_tables.get_size - 1 LOOP
            lv_result_query := CONCAT(lv_result_query, 
             utl_lms.format_message('DROP TABLE %s;', lv_ja_tables.get(j).to_string) || chr(10));
        END LOOP;
    END LOOP;
    lv_result_query := replace(lv_result_query, '"', '');
    RETURN lv_result_query;
END;



create or replace function parse_json_create_object(js_obj JSON_OBJECT_T)
RETURN NCLOB
IS
lv_result_data NCLOB;
lv_result_constr NCLOB;
lv_ja JSON_ARRAY_T;
lv_keys_tab_names JSON_KEY_LIST;
lv_keys_tab_param JSON_KEY_LIST;
lv_keys_tab_columns JSON_KEY_LIST;
lv_keys_tab_constr JSON_KEY_LIST;
lv_keys_temp JSON_KEY_LIST;
lv_jo_table JSON_OBJECT_T;
lv_jo_param JSON_OBJECT_T;
lv_jo_temp JSON_OBJECT_T;

lv_table NCLOB;
lv_column NCLOB;

BEGIN
    lv_ja := new JSON_ARRAY_T;
    
    lv_keys_tab_names := js_obj.get_keys; 
    --table_names
    FOR i IN 1..lv_keys_tab_names.COUNT LOOP
        lv_jo_table := js_obj.get_object(lv_keys_tab_names(i));
        lv_keys_tab_param := lv_jo_table.get_keys;
        lv_result_data := CONCAT(lv_result_data, 
             utl_lms.format_message('CREATE TABLE %s (', lv_keys_tab_names(i)));
       --columns & constraints
        FOR j IN 1..lv_keys_tab_param.COUNT LOOP
            lv_jo_param := lv_jo_table.get_object(lv_keys_tab_param(j));
            lv_keys_tab_columns := lv_jo_param.get_keys;
            
            IF upper(lv_keys_tab_param(j)) = upper('columns') THEN  
                FOR k IN 1..lv_keys_tab_columns.COUNT LOOP
                    lv_result_data := CONCAT(lv_result_data, chr(10) || lv_keys_tab_columns(k));
               
                    lv_jo_temp := lv_jo_param.get_object(lv_keys_tab_columns(k)); -- jo {type, size}
                    lv_keys_temp := lv_jo_temp.get_keys;
                    FOR n in 1..lv_keys_temp.COUNT LOOP   
                        IF lv_keys_temp(n) = 'size' THEN
                            lv_result_data := CONCAT(lv_result_data,  utl_lms.format_message('(%s)',lv_jo_temp.get(lv_keys_temp(n)).to_string));
                        ELSE
                             lv_result_data := CONCAT(lv_result_data, ' ' || lv_jo_temp.get(lv_keys_temp(n)).to_string);
                        END IF;
                    END LOOP;
                    lv_result_data := CONCAT(lv_result_data, ',');   
                END LOOP;          
            ELSIF upper(lv_keys_tab_param(j)) = upper('constraints') THEN
                    lv_result_constr := '';
                    lv_keys_tab_constr := lv_jo_param.get_keys;
                    FOR n in 1..lv_keys_tab_constr.COUNT LOOP
                        lv_jo_temp := lv_jo_param.get_object(lv_keys_tab_constr(n));
                        lv_result_constr := CONCAT(lv_result_constr, 'CONSTRAINT ' || lv_keys_tab_constr(n)); --pk_id
                        lv_keys_temp := lv_jo_temp.get_keys;
                        lv_result_constr := CONCAT(lv_result_constr, utl_lms.format_message(' %s (', lv_keys_temp(1))); --PRIMARY KEY
                        IF upper(lv_keys_temp(1)) = 'PRIMARY KEY' THEN
                            lv_ja := lv_jo_temp.get_array(lv_keys_temp(1));
                            for k in 0..lv_ja.get_size - 1 LOOP
                                DBMS_OUTPUT.put_line(utl_lms.format_message('CREATE SEQUENCE %s_%s START WITH 1 INCREMENT BY 1 CACHE 100;', lv_keys_tab_names(i), REPLACE(lv_ja.get(k).to_string, '"', '')));   
                                DBMS_OUTPUT.put_line(utl_lms.format_message('CREATE OR REPLACE TRIGGER %s_%s
                                BEFORE INSERT ON %s
                                FOR EACH ROW
                                BEGIN
                                :new.id := %s_%s.nextval;
                                END;',
                                lv_keys_tab_names(i),
                                 REPLACE(lv_ja.get(k).to_string, '"',''),
                                 lv_keys_tab_names(i),
                                 lv_keys_tab_names(i),
                                REPLACE(lv_ja.get(k).to_string, '"', '')));
                                lv_result_constr := CONCAT(lv_result_constr, lv_ja.get(k).to_string || ', ');
                                IF k = lv_ja.get_size - 1 THEN
                                    lv_result_constr := SUBSTR(lv_result_constr, 1, length(lv_result_constr) - 2);
                                END IF;
                            END LOOP;  
                            lv_result_constr := CONCAT(lv_result_constr, '),' || chr(10));
                        ELSIF upper(lv_keys_temp(1)) = 'FOREIGN KEY' THEN
                            lv_jo_temp := lv_jo_temp.get_object(lv_keys_temp(1));
                            lv_keys_temp := lv_jo_temp.get_keys; -- col, tab_nam, col_from
                        lv_ja := lv_jo_temp.get_array(lv_keys_temp(1));
                        for k in 0..lv_ja.get_size - 1 LOOP
                            lv_result_constr := CONCAT(lv_result_constr, lv_ja.get(k).to_string || ', ');
                            IF k = lv_ja.get_size - 1 THEN
                                lv_result_constr := SUBSTR(lv_result_constr, 1, length(lv_result_constr) - 2);
                            END IF;
                        END LOOP;  
                        lv_result_constr := CONCAT(lv_result_constr, ') REFERENCES ' || lv_jo_temp.get(lv_keys_temp(2)).to_string || '( ');
                        lv_ja := lv_jo_temp.get_array(lv_keys_temp(3));
                        for k in 0..lv_ja.get_size - 1 LOOP
                            lv_result_constr := CONCAT(lv_result_constr, lv_ja.get(k).to_string || ', ');
                             IF k = lv_ja.get_size - 1 THEN
                                lv_result_constr := SUBSTR(lv_result_constr, 1, length(lv_result_constr) - 2);
                            END IF;
                        END LOOP;  
                        lv_result_constr := CONCAT(lv_result_constr, ' ) ');
                        IF lv_keys_temp.COUNT >= 4 THEN 
                            lv_result_constr := CONCAT(lv_result_constr, 'ON DELETE ' || lv_jo_temp.get(lv_keys_temp(4)).to_string || ', ');
                        ELSE 
                         lv_result_constr := CONCAT(lv_result_constr, 'ON DELETE SET NULL, ');
                        END IF;
                    END IF;
                    END LOOP;
                    
            END IF;
        END LOOP;
        lv_result_data := CONCAT(lv_result_data, chr(10) || lv_result_constr);
        lv_result_data := SUBSTR(lv_result_data, 1, LENGTH(lv_result_data)-2);
        lv_result_data := CONCAT(lv_result_data, ');' || chr(10));
    END LOOP;
    lv_result_data := replace(lv_result_data, '"', '');
    RETURN lv_result_data;
END;



create or replace function parse_json_select_object(ja_obj JSON_OBJECT_T)
RETURN NCLOB
IS
lv_result_query NCLOB;
lv_field NCLOB;
is_like BOOLEAN := false;
is_condition_in BOOLEAN :=false;
is_condition_between BOOLEAN :=false;
lv_ja JSON_ARRAY_T;
lv_ja_param JSON_ARRAY_T;
lv_keys JSON_KEY_LIST;
lv_jk_temp JSON_KEY_LIST;
lv_jk_temp_inner JSON_KEY_LIST;
lv_jk_field_temp JSON_KEY_LIST;
lv_jo_temp JSON_OBJECT_T;
lv_jo_temp_inner JSON_OBJECT_T;
lv_jo_field_temp JSON_OBJECT_T;
BEGIN
    lv_keys := ja_obj.get_keys;
    lv_result_query := CONCAT(lv_result_query, 'SELECT ');
    for i in 1..lv_keys.COUNT LOOP
        IF upper(lv_keys(i)) = 'COLUMNS' THEN
            lv_ja := ja_obj.get_array(lv_keys(i));
            FOR j in 0..lv_ja.get_size - 1 LOOP
                lv_result_query := CONCAT(lv_result_query, REPLACE(lv_ja.get(j).to_string, '"', '') || ', ');
            END LOOP;
        ELSIF upper(lv_keys(i)) = 'TABLES' THEN
            lv_result_query := SUBSTR(lv_result_query, 1, length(lv_result_query) - 2);
            lv_result_query := CONCAT(lv_result_query, ' FROM ');
            lv_ja := ja_obj.get_array(lv_keys(i));
            
             FOR j in 0..lv_ja.get_size - 1 LOOP
                 lv_result_query := CONCAT(lv_result_query, REPLACE(lv_ja.get(j).to_string, '"', '') || ', ');
             END LOOP;
            lv_result_query := SUBSTR(lv_result_query, 1, length(lv_result_query) - 2);
        ELSIF upper(lv_keys(i)) = 'CONDITIONS' THEN
            lv_jo_temp := ja_obj.get_object(lv_keys(i)); --{''where''}
            
            lv_jk_temp := lv_jo_temp.get_keys;
            FOR p in 1..lv_jk_temp.COUNT LOOP
                IF lv_jk_temp(p) = 'WHERE' THEN
                    lv_result_query := CONCAT(lv_result_query, ' ' || REPLACE(lv_jk_temp(p), '"', ''));
                    lv_jo_temp_inner := lv_jo_temp.get_object(lv_jk_temp(p)); --and & or
                    lv_jk_temp_inner := lv_jo_temp_inner.get_keys; -- and / or
                     for j in 1..lv_jk_temp_inner.COUNT LOOP
                IF upper(lv_jk_temp_inner(j)) = 'OR' or upper(lv_jk_temp_inner(j)) = 'AND' or INSTR(upper(lv_jk_temp_inner(j)), 'IN') > 0 THEN
                        IF j != 1 THEN
                            lv_result_query := CONCAT(lv_result_query || ' ', lv_jk_temp_inner(j) || ' ');
                        END IF;
                             lv_ja := lv_jo_temp_inner.get_array(lv_jk_temp_inner(j)); --array of fields
                for k in 0..lv_ja.get_size - 1 LOOP
                    lv_jo_field_temp := JSON_OBJECT_T.parse(lv_ja.get(k).to_string);
                    
                    lv_jk_field_temp := lv_jo_field_temp.get_keys;
                    lv_field := REPLACE(lv_jk_field_temp(1), '"', ''); --id
                    lv_jo_field_temp := lv_jo_field_temp.get_object(lv_jk_field_temp(1));  --{has_not, operator_value}
                    
                    lv_jk_field_temp := lv_jo_field_temp.get_keys;
                    For n in 1..lv_jk_field_temp.COUNT LOOP
                       IF upper(REPLACE(lv_jo_field_temp.get(lv_jk_field_temp(n)).to_string, '"', '')) = 'LIKE' THEN
                            lv_field := CONCAT(lv_field, ' ' || REPLACE(lv_jo_field_temp.get(lv_jk_field_temp(n)).to_string, '"', ''));
                            is_like := true;
                        ELSIF upper(REPLACE(lv_jo_field_temp.get(lv_jk_field_temp(n)).to_string, '"', '')) = 'IN' THEN
                            lv_field := CONCAT(lv_field, ' ' || REPLACE(lv_jo_field_temp.get(lv_jk_field_temp(n)).to_string, '"', ''));
                            is_condition_in := true;
                        ELSIF upper(REPLACE(lv_jo_field_temp.get(lv_jk_field_temp(n)).to_string, '"', '')) = 'BETWEEN' THEN
                            lv_field := CONCAT(lv_field, ' ' || REPLACE(lv_jo_field_temp.get(lv_jk_field_temp(n)).to_string, '"', ''));
                            is_condition_between := true;
                        ELSIF is_like THEN
                            lv_field := CONCAT(lv_field, ' ' || lv_jo_field_temp.get(lv_jk_field_temp(n)).to_string);
                            is_like := false;
                        ELSIF is_condition_between THEN
                            lv_ja_param := lv_jo_field_temp.get_array(lv_jk_field_temp(n));
                            lv_field := CONCAT(lv_field, ' ' || utl_lms.format_message('%s and %s', lv_ja_param.get(0).to_string, lv_ja_param.get(1).to_string));
                            is_condition_between := false;
                        ELSIF is_condition_in THEN
                            lv_ja_param := lv_jo_field_temp.get_array(lv_jk_field_temp(n));
                            lv_field := CONCAT(lv_field, '( ');
                            FOR m in 0..lv_ja_param.get_size - 1 LOOP
                                lv_field := CONCAT(lv_field, lv_ja_param.get(m).to_string || ', ');
                            END LOOP;
                            lv_field := SUBSTR(lv_field, 1, LENGTH(lv_field) - 2);
                            lv_field := CONCAT(lv_field, ')');
                            is_condition_in := false;
                        ELSE
                            IF INSTR(upper(lv_jk_temp_inner(j)), 'IN') > 0 THEN
                                lv_jo_field_temp := JSON_OBJECT_T.parse(lv_jo_field_temp.get(lv_jk_field_temp(n)).to_string); --{select}
                                lv_jo_field_temp := lv_jo_field_temp.get_object('select');
                                lv_field := CONCAT(lv_field, ' ' || upper(lv_jk_temp_inner(j)) || '( ' || parse_json_select_object(lv_jo_field_temp));
                                lv_field:=SUBSTR(lv_field, 1, LENGTH(lv_field)-1);
                                lv_field := CONCAT(lv_field, ')');     
                            ELSE
                                lv_field := CONCAT(lv_field, ' ' || REPLACE(lv_jo_field_temp.get(lv_jk_field_temp(n)).to_string, '"', ''));
                            END IF;
                        END IF;
                    END LOOP;
                        lv_result_query := CONCAT(lv_result_query || ' ', lv_field || ' ' || lv_jk_temp_inner(j));
                END LOOP;
                lv_result_query := SUBSTR(lv_result_query, 1, LENGTH(lv_result_query) - LENGTH(lv_jk_temp_inner(j)));
                ELSIF INSTR(upper(lv_jk_temp_inner(j)), 'EXISTS') > 0 THEN
                    lv_result_query := CONCAT(lv_result_query || ' ', upper(lv_jk_temp_inner(j)) || '(');
                    lv_jo_field_temp := lv_jo_temp.get_object(lv_jk_temp(p)); --{'exists'}
                    lv_jk_field_temp := lv_jo_field_temp.get_keys;
                    lv_jo_field_temp := lv_jo_field_temp.get_object(lv_jk_field_temp(1)); --{'select'} 
                    lv_jk_field_temp := lv_jo_field_temp.get_keys;
                    lv_jo_field_temp := lv_jo_field_temp.get_object(lv_jk_field_temp(1)); 
                    lv_field := parse_json_select_object(lv_jo_field_temp);
                    lv_result_query := CONCAT(lv_result_query || ' ', SUBSTR(lv_field, 1, length(lv_field)-1) || ')');
                END IF;
            END LOOP;
                ELSIF INSTR(upper(lv_jk_temp(p)), 'JOIN') > 0 THEN
                    lv_jo_temp_inner := lv_jo_temp.get_object(lv_jk_temp(p)); --join param
                    lv_jk_temp_inner := lv_jo_temp_inner.get_keys; -- from_table/to_table
                    
                    lv_result_query := CONCAT(lv_result_query, 
                        REPLACE(utl_lms.format_message(' %s %s ON %s.%s = %s.%s ',
                        upper(lv_jk_temp(p)),
                        lv_jo_temp_inner.get(lv_jk_temp_inner(1)).to_string,
                        lv_jo_temp_inner.get(lv_jk_temp_inner(1)).to_string,
                        lv_jo_temp_inner.get(lv_jk_temp_inner(2)).to_string,
                        lv_jo_temp_inner.get(lv_jk_temp_inner(3)).to_string,
                        lv_jo_temp_inner.get(lv_jk_temp_inner(4)).to_string
                        ), '"', ''));
                END IF;
            END LOOP;
        END IF;
    END LOOP;
      lv_result_query := CONCAT(lv_result_query, ';');
    RETURN lv_result_query;
END;




create or replace function parse_json_insert_object(js_obj JSON_OBJECT_T)
RETURN NCLOB
IS
lv_result_query NCLOB;
lv_field NCLOB;
lv_ja JSON_ARRAY_T;
lv_ja_param JSON_ARRAY_T;
lv_keys JSON_KEY_LIST;
lv_jk_temp JSON_KEY_LIST;
lv_jo_temp JSON_OBJECT_T;
BEGIN
    lv_result_query := 'INSERT INTO ';
    lv_keys := js_obj.get_keys;
    for i in 1..lv_keys.COUNT LOOP
        IF upper(lv_keys(i))='TABLE' THEN
            lv_result_query := CONCAT(lv_result_query, REPLACE(js_obj.get(lv_keys(i)).to_string, '"', '') || ' ');
        ELSIF upper(lv_keys(i))='COLUMNS' THEN
            lv_ja := js_obj.get_array(lv_keys(i));
            lv_result_query := CONCAT(lv_result_query, '( ');
            for j in 0..lv_ja.get_size - 1 LOOP
                lv_result_query := CONCAT(lv_result_query, REPLACE(lv_ja.get(j).to_string, '"','') || ', ');
            END LOOP;
            lv_result_query := SUBSTR(lv_result_query, 1, LENGTH(lv_result_query) - 2);
            lv_result_query := CONCAT(lv_result_query, ') ');
        ELSIF upper(lv_keys(i))='VALUES' THEN
                lv_result_query := CONCAT(lv_result_query, upper(lv_keys(i)));
                lv_ja := js_obj.get_array(lv_keys(i));
                lv_result_query := CONCAT(lv_result_query, '( ');
                for j in 0..lv_ja.get_size - 1 LOOP
                    lv_result_query := CONCAT(lv_result_query, lv_ja.get(j).to_string || ', ');
                END LOOP;
                lv_result_query := SUBSTR(lv_result_query, 1, LENGTH(lv_result_query) - 2);
                lv_result_query := CONCAT(lv_result_query, ');');
        ELSIF upper(lv_keys(i))='SELECT' THEN
            lv_jo_temp := js_obj.get_object(lv_keys(i));
             lv_field := upper('( ' || parse_json_select_object(lv_jo_temp));
             lv_field:=SUBSTR(lv_field, 1, LENGTH(lv_field)-1);
             lv_result_query := CONCAT(lv_result_query, lv_field || ');');
            END IF;
    END LOOP;
    return lv_result_query;
END;




create or replace function parse_json_update_object(js_obj JSON_OBJECT_T)
RETURN NCLOB
IS
lv_result_query NCLOB;
lv_field NCLOB;
lv_ja JSON_ARRAY_T;
lv_ja_param JSON_ARRAY_T;
lv_keys JSON_KEY_LIST;
lv_jk_temp JSON_KEY_LIST;
lv_jo_temp JSON_OBJECT_T;

is_like BOOLEAN := false;
is_condition_in BOOLEAN :=false;
is_condition_between BOOLEAN :=false;

lv_jk_temp_inner JSON_KEY_LIST;
lv_jk_field_temp JSON_KEY_LIST;

lv_jo_temp_inner JSON_OBJECT_T;
lv_jo_field_temp JSON_OBJECT_T;
BEGIN
    lv_result_query := 'UPDATE ';
    lv_keys := js_obj.get_keys;
    for i in 1..lv_keys.COUNT LOOP
        IF upper(lv_keys(i))='TABLE' THEN
            lv_result_query := CONCAT(lv_result_query, REPLACE(js_obj.get(lv_keys(i)).to_string, '"', '') || ' ');
        ELSIF upper(lv_keys(i))='VALUES' THEN
                lv_result_query := CONCAT(lv_result_query, 'SET '); 
                lv_ja := js_obj.get_array(lv_keys(i));
                for j in 0..lv_ja.get_size - 1 LOOP                
                    lv_jo_temp := JSON_OBJECT_T.parse(lv_ja.get(j).to_string);
                lv_jk_temp := lv_jo_temp.get_keys;
                    lv_result_query := CONCAT(lv_result_query, REPLACE(lv_jk_temp(1), '"', '') || '=' || lv_jo_temp.get(lv_jk_temp(1)).to_string || ', ');
                END LOOP;
                lv_result_query := SUBSTR(lv_result_query, 1, LENGTH(lv_result_query) - 2);
      ELSIF upper(lv_keys(i))='COLUMNS' THEN
            lv_result_query := CONCAT(lv_result_query, 'SET '); 
            lv_ja := js_obj.get_array(lv_keys(i));
            lv_result_query := CONCAT(lv_result_query, '( ');
            for j in 0..lv_ja.get_size - 1 LOOP
                lv_result_query := CONCAT(lv_result_query, REPLACE(lv_ja.get(j).to_string, '"','') || ', ');
            END LOOP;
            lv_result_query := SUBSTR(lv_result_query, 1, LENGTH(lv_result_query) - 2);
            lv_result_query := CONCAT(lv_result_query, ') ');
       ELSIF upper(lv_keys(i))='SELECT' THEN
             lv_jo_temp := js_obj.get_object(lv_keys(i));
             lv_field := upper(' = ( ' || parse_json_select_object(lv_jo_temp));
             lv_field:=SUBSTR(lv_field, 1, LENGTH(lv_field)-1);
             lv_result_query := CONCAT(lv_result_query, lv_field || ')');
       ELSIF upper(lv_keys(i)) = 'WHERE' THEN
               lv_result_query := CONCAT(lv_result_query, ' ' || REPLACE(lv_keys(i), '"', ''));
                    lv_jo_temp_inner := js_obj.get_object(lv_keys(i)); --and & or
                    lv_jk_temp_inner := lv_jo_temp_inner.get_keys; -- and / or
                     for j in 1..lv_jk_temp_inner.COUNT LOOP
                IF upper(lv_jk_temp_inner(j)) = 'OR' or upper(lv_jk_temp_inner(j)) = 'AND' or INSTR(upper(lv_jk_temp_inner(j)), 'IN') > 0 THEN
                        IF j != 1 THEN
                            lv_result_query := CONCAT(lv_result_query || ' ', lv_jk_temp_inner(j) || ' ');
                        END IF;
                             lv_ja := lv_jo_temp_inner.get_array(lv_jk_temp_inner(j)); --array of fields
                for k in 0..lv_ja.get_size - 1 LOOP
                    lv_jo_field_temp := JSON_OBJECT_T.parse(lv_ja.get(k).to_string);
                    
                    lv_jk_field_temp := lv_jo_field_temp.get_keys;
                    lv_field := REPLACE(lv_jk_field_temp(1), '"', ''); --id
                    lv_jo_field_temp := lv_jo_field_temp.get_object(lv_jk_field_temp(1));  --{has_not, operator_value}
                    
                    lv_jk_field_temp := lv_jo_field_temp.get_keys;
                    For n in 1..lv_jk_field_temp.COUNT LOOP
                       IF upper(REPLACE(lv_jo_field_temp.get(lv_jk_field_temp(n)).to_string, '"', '')) = 'LIKE' THEN
                            lv_field := CONCAT(lv_field, ' ' || REPLACE(lv_jo_field_temp.get(lv_jk_field_temp(n)).to_string, '"', ''));
                            is_like := true;
                        ELSIF upper(REPLACE(lv_jo_field_temp.get(lv_jk_field_temp(n)).to_string, '"', '')) = 'IN' THEN
                            lv_field := CONCAT(lv_field, ' ' || REPLACE(lv_jo_field_temp.get(lv_jk_field_temp(n)).to_string, '"', ''));
                            is_condition_in := true;
                        ELSIF upper(REPLACE(lv_jo_field_temp.get(lv_jk_field_temp(n)).to_string, '"', '')) = 'BETWEEN' THEN
                            lv_field := CONCAT(lv_field, ' ' || REPLACE(lv_jo_field_temp.get(lv_jk_field_temp(n)).to_string, '"', ''));
                            is_condition_between := true;
                        ELSIF is_like THEN
                            lv_field := CONCAT(lv_field, ' ' || lv_jo_field_temp.get(lv_jk_field_temp(n)).to_string);
                            is_like := false;
                        ELSIF is_condition_between THEN
                            lv_ja_param := lv_jo_field_temp.get_array(lv_jk_field_temp(n));
                            lv_field := CONCAT(lv_field, ' ' || utl_lms.format_message('%s and %s', lv_ja_param.get(0).to_string, lv_ja_param.get(1).to_string));
                            is_condition_between := false;
                        ELSIF is_condition_in THEN
                            lv_ja_param := lv_jo_field_temp.get_array(lv_jk_field_temp(n));
                            lv_field := CONCAT(lv_field, '( ');
                            FOR m in 0..lv_ja_param.get_size - 1 LOOP
                                lv_field := CONCAT(lv_field, lv_ja_param.get(m).to_string || ', ');
                            END LOOP;
                            lv_field := SUBSTR(lv_field, 1, LENGTH(lv_field) - 2);
                            lv_field := CONCAT(lv_field, ')');
                            is_condition_in := false;
                        ELSE
                            IF INSTR(upper(lv_jk_temp_inner(j)), 'IN') > 0 THEN
                                lv_jo_field_temp := JSON_OBJECT_T.parse(lv_jo_field_temp.get(lv_jk_field_temp(n)).to_string); --{select}
                                lv_jo_field_temp := lv_jo_field_temp.get_object('select');
                                lv_field := CONCAT(lv_field, ' ' || upper(lv_jk_temp_inner(j)) || '( ' || parse_json_select_object(lv_jo_field_temp));
                                lv_field:=SUBSTR(lv_field, 1, LENGTH(lv_field)-1);
                                lv_field := CONCAT(lv_field, ')');
                            ELSE
                                lv_field := CONCAT(lv_field, ' ' || REPLACE(lv_jo_field_temp.get(lv_jk_field_temp(n)).to_string, '"', ''));
                            END IF;
                        END IF;
                    END LOOP;
                    lv_result_query := CONCAT(lv_result_query || ' ', lv_field || ' ' || lv_jk_temp_inner(j));
                    
                END LOOP;
                lv_result_query := SUBSTR(lv_result_query, 1, LENGTH(lv_result_query) - LENGTH(lv_jk_temp_inner(j)));
                 ELSIF INSTR(upper(lv_jk_temp_inner(j)), 'EXISTS') > 0 THEN
                    lv_result_query := CONCAT(lv_result_query || ' ', upper(lv_jk_temp_inner(j)) || '(');
                    lv_jo_field_temp := lv_jo_temp_inner.get_object(lv_jk_temp_inner(j)); --{'select'}
                    lv_jk_field_temp := lv_jo_field_temp.get_keys;
                    lv_jo_field_temp := lv_jo_field_temp.get_object(lv_jk_field_temp(1)); 
                    lv_field := parse_json_select_object(lv_jo_field_temp);
                    lv_result_query := CONCAT(lv_result_query || ' ', SUBSTR(lv_field, 1, length(lv_field)-1) || ')');
               END IF; 
            END LOOP;
        END IF;
        END LOOP;
      lv_result_query := CONCAT(lv_result_query, ';');
    return lv_result_query;
END;





create or replace function parse_json_delete_object(js_obj JSON_OBJECT_T)
RETURN NCLOB
IS
lv_result_query NCLOB;
lv_field NCLOB;
lv_ja JSON_ARRAY_T;
lv_ja_param JSON_ARRAY_T;
lv_keys JSON_KEY_LIST;
lv_jk_temp JSON_KEY_LIST;
lv_jo_temp JSON_OBJECT_T;

is_like BOOLEAN := false;
is_condition_in BOOLEAN :=false;
is_condition_between BOOLEAN :=false;

lv_jk_temp_inner JSON_KEY_LIST;
lv_jk_field_temp JSON_KEY_LIST;

lv_jo_temp_inner JSON_OBJECT_T;
lv_jo_field_temp JSON_OBJECT_T;
BEGIN
    lv_result_query := 'DELETE FROM ';
    lv_keys := js_obj.get_keys;
    for i in 1..lv_keys.COUNT LOOP
        IF upper(lv_keys(i))='TABLE' THEN
            lv_result_query := CONCAT(lv_result_query, REPLACE(js_obj.get(lv_keys(i)).to_string, '"', '') || ' ');
       ELSIF upper(lv_keys(i)) = 'WHERE' THEN
               lv_result_query := CONCAT(lv_result_query, ' ' || REPLACE(lv_keys(i), '"', ''));
                    lv_jo_temp_inner := js_obj.get_object(lv_keys(i)); --and & or
                    lv_jk_temp_inner := lv_jo_temp_inner.get_keys; -- and / or
                     for j in 1..lv_jk_temp_inner.COUNT LOOP
                IF upper(lv_jk_temp_inner(j)) = 'OR' or upper(lv_jk_temp_inner(j)) = 'AND' or INSTR(upper(lv_jk_temp_inner(j)), 'IN') > 0 THEN
                        IF j != 1 THEN
                            lv_result_query := CONCAT(lv_result_query || ' ', lv_jk_temp_inner(j) || ' ');
                        END IF;
                             lv_ja := lv_jo_temp_inner.get_array(lv_jk_temp_inner(j)); --array of fields
                for k in 0..lv_ja.get_size - 1 LOOP
                    lv_jo_field_temp := JSON_OBJECT_T.parse(lv_ja.get(k).to_string);
                    
                    lv_jk_field_temp := lv_jo_field_temp.get_keys;
                    lv_field := REPLACE(lv_jk_field_temp(1), '"', ''); --id
                    lv_jo_field_temp := lv_jo_field_temp.get_object(lv_jk_field_temp(1));  --{has_not, operator_value}
                    
                    lv_jk_field_temp := lv_jo_field_temp.get_keys;
                    For n in 1..lv_jk_field_temp.COUNT LOOP
                       IF upper(REPLACE(lv_jo_field_temp.get(lv_jk_field_temp(n)).to_string, '"', '')) = 'LIKE' THEN
                            lv_field := CONCAT(lv_field, ' ' || REPLACE(lv_jo_field_temp.get(lv_jk_field_temp(n)).to_string, '"', ''));
                            is_like := true;
                        ELSIF upper(REPLACE(lv_jo_field_temp.get(lv_jk_field_temp(n)).to_string, '"', '')) = 'IN' THEN
                            lv_field := CONCAT(lv_field, ' ' || REPLACE(lv_jo_field_temp.get(lv_jk_field_temp(n)).to_string, '"', ''));
                            is_condition_in := true;
                        ELSIF upper(REPLACE(lv_jo_field_temp.get(lv_jk_field_temp(n)).to_string, '"', '')) = 'BETWEEN' THEN
                            lv_field := CONCAT(lv_field, ' ' || REPLACE(lv_jo_field_temp.get(lv_jk_field_temp(n)).to_string, '"', ''));
                            is_condition_between := true;
                        ELSIF is_like THEN
                            lv_field := CONCAT(lv_field, ' ' || lv_jo_field_temp.get(lv_jk_field_temp(n)).to_string);
                            is_like := false;
                        ELSIF is_condition_between THEN
                            lv_ja_param := lv_jo_field_temp.get_array(lv_jk_field_temp(n));
                            lv_field := CONCAT(lv_field, ' ' || utl_lms.format_message('%s and %s', lv_ja_param.get(0).to_string, lv_ja_param.get(1).to_string));
                            is_condition_between := false;
                        ELSIF is_condition_in THEN
                            lv_ja_param := lv_jo_field_temp.get_array(lv_jk_field_temp(n));
                            lv_field := CONCAT(lv_field, '( ');
                            FOR m in 0..lv_ja_param.get_size - 1 LOOP
                                lv_field := CONCAT(lv_field, lv_ja_param.get(m).to_string || ', ');
                            END LOOP;
                            lv_field := SUBSTR(lv_field, 1, LENGTH(lv_field) - 2);
                            lv_field := CONCAT(lv_field, ')');
                            is_condition_in := false;
                        ELSE
                            IF INSTR(upper(lv_jk_temp_inner(j)), 'IN') > 0 THEN
                                lv_jo_field_temp := JSON_OBJECT_T.parse(lv_jo_field_temp.get(lv_jk_field_temp(n)).to_string); --{select}
                                lv_jo_field_temp := lv_jo_field_temp.get_object('select');
                                lv_field := CONCAT(lv_field, ' ' || upper(lv_jk_temp_inner(j)) || '( ' || parse_json_select_object(lv_jo_field_temp));
                                lv_field:=SUBSTR(lv_field, 1, LENGTH(lv_field)-1);
                                lv_field := CONCAT(lv_field, ')');     
                            ELSE
                                lv_field := CONCAT(lv_field, ' ' || REPLACE(lv_jo_field_temp.get(lv_jk_field_temp(n)).to_string, '"', ''));
                            END IF;
                        END IF;
                    END LOOP;
                        lv_result_query := CONCAT(lv_result_query || ' ', lv_field || ' ' || lv_jk_temp_inner(j));
                END LOOP;
                lv_result_query := SUBSTR(lv_result_query, 1, LENGTH(lv_result_query) - LENGTH(lv_jk_temp_inner(j)));
                 ELSIF INSTR(upper(lv_jk_temp_inner(j)), 'EXISTS') > 0 THEN
                    lv_result_query := CONCAT(lv_result_query || ' ', upper(lv_jk_temp_inner(j)) || '(');
                    lv_jo_field_temp := lv_jo_temp_inner.get_object(lv_jk_temp_inner(j)); --{'select'}
                    lv_jk_field_temp := lv_jo_field_temp.get_keys;
                    lv_jo_field_temp := lv_jo_field_temp.get_object(lv_jk_field_temp(1)); 
                    lv_field := parse_json_select_object(lv_jo_field_temp);
                    lv_result_query := CONCAT(lv_result_query || ' ', SUBSTR(lv_field, 1, length(lv_field)-1) || ')');
               END IF; 
            END LOOP;
        END IF;
        END LOOP;
      lv_result_query := CONCAT(lv_result_query, ';');
    return lv_result_query;
END;









create or replace function parse_json(json_str NCLOB)
RETURN VARCHAR2
IS
lv_jo JSON_OBJECT_T;
lv_keys JSON_KEY_LIST;
lv_ja JSON_ARRAY_T;
lv_jo_temp JSON_OBJECT_T;
BEGIN
    lv_ja := new JSON_ARRAY_T;
    lv_jo := JSON_OBJECT_T.parse(json_str);
    lv_keys := lv_jo.get_keys;
     lv_ja := lv_jo.get_array(lv_keys(1)); -- array of obj
    FOR i IN 0..lv_ja.get_size - 1 LOOP
        lv_jo := JSON_OBJECT_T.parse(lv_ja.get(i).to_string); --{} obj
        lv_keys := lv_jo.get_keys;
        lv_jo_temp := lv_jo.get_object(lv_keys(1));
        CASE upper(lv_keys(1))
        WHEN upper('select') THEN DBMS_OUTPUT.put_line(parse_json_select_object(lv_jo_temp));
        WHEN upper('insert into') THEN DBMS_OUTPUT.put_line(parse_json_insert_object(lv_jo_temp));
        WHEN upper('update') THEN DBMS_OUTPUT.put_line(parse_json_update_object(lv_jo_temp));
        WHEN upper('delete') THEN DBMS_OUTPUT.put_line(parse_json_delete_object(lv_jo_temp));
        WHEN upper('create') THEN DBMS_OUTPUT.put_line(parse_json_create_object(lv_jo_temp));
        WHEN upper('drop') THEN DBMS_OUTPUT.put_line(parse_json_drop_object(lv_jo_temp));
        ELSE DBMS_OUTPUT.put_line('No operation availiable for ' || lv_jo_temp.to_string);
        END CASE;
        
    END LOOP;
    return 'Done';
END;





 