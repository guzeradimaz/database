insert into groups(name) values('053501');
insert into groups(name) values('053502');
insert into groups(name) values('053503');
insert into groups(name) values('053504');

insert into students(name,group_id) values('stud231d',4);


CREATE TABLE GROUPS
(ID number primary key not null, 
 NAME varchar2(100) not null, 
 C_VAL number default 0);

CREATE TABLE STUDENTS 
(ID number primary key, 
 NAME varchar2(100), 
 GROUP_ID number);
------------------------------
CREATE OR REPLACE TRIGGER check_unique_groupname
BEFORE INSERT OR UPDATE OF NAME ON GROUPS
FOR EACH ROW
DECLARE
    PRAGMA AUTONOMOUS_TRANSACTION;
    rowcount NUMBER;
    e_not_unique_groupname EXCEPTION;
    PRAGMA exception_init(e_not_unique_groupname, -20001);
BEGIN 
    SELECT COUNT(*) INTO rowcount FROM GROUPS WHERE NAME = :new.NAME;
    IF rowcount != 0  THEN
        RAISE e_not_unique_groupname;
    END IF;
END;
------------------------------
CREATE OR REPLACE TRIGGER check_unique_id_student
BEFORE INSERT OR UPDATE OF NAME ON STUDENTS
FOR EACH ROW
DECLARE
    PRAGMA AUTONOMOUS_TRANSACTION;
    rowcount NUMBER;
    e_not_unique_groupname EXCEPTION;
    PRAGMA exception_init(e_not_unique_groupname, -20001);
BEGIN 
    SELECT COUNT(*) INTO rowcount FROM STUDENTS WHERE ID = :new.ID;
    IF rowcount != 0  THEN
        RAISE e_not_unique_groupname;
    END IF;
END;
------------------------------
CREATE OR REPLACE TRIGGER check_unique_id_group
BEFORE INSERT OR UPDATE OF NAME ON GROUPS
FOR EACH ROW
DECLARE
    PRAGMA AUTONOMOUS_TRANSACTION;
    rowcount NUMBER;
    e_not_unique_groupname EXCEPTION;
    PRAGMA exception_init(e_not_unique_groupname, -20001);
BEGIN 
    SELECT COUNT(*) INTO rowcount FROM GROUPS WHERE ID = :new.ID;
    IF rowcount != 0  THEN
        RAISE e_not_unique_groupname;
    END IF;
END;
------------------------------
CREATE OR REPLACE TRIGGER generate_group_id
BEFORE INSERT ON GROUPS
FOR EACH ROW
FOLLOWS check_unique_groupname
BEGIN 
    SELECT groups_id_sequence.nextval INTO :new.ID FROM dual;
END;
------------------------------
CREATE OR REPLACE TRIGGER generate_student_id
BEFORE INSERT ON STUDENTS
FOR EACH ROW
BEGIN 
    SELECT students_id_sequence.nextval INTO :new.ID FROM dual;
END;
------------------------------
CREATE OR REPLACE TRIGGER group_del 
BEFORE DELETE ON GROUPS
FOR EACH ROW
DECLARE
    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
    DELETE FROM STUDENTS WHERE GROUP_ID = :old.ID;
    COMMIT;
END;
------------------------------
CREATE TABLE LOGS
(TIME TIMESTAMP NOT NULL, 
 STATEMENT varchar2(100) NOT NULL,
 ID_NEW number,
 ID_OLD number,
 NAME varchar2(100),
 GROUP_ID number
);
------------------------------
CREATE OR REPLACE TRIGGER logging 
AFTER INSERT OR DELETE OR UPDATE ON STUDENTS
FOR EACH ROW
BEGIN
    CASE
    WHEN INSERTING THEN
        INSERT INTO LOGS VALUES(SYSTIMESTAMP,'INSERT',:new.ID,null,null,null);
    WHEN UPDATING THEN
        INSERT INTO LOGS VALUES(SYSTIMESTAMP,'UPDATE',:new.ID,:old.ID,:old.NAME,:old.GROUP_ID);
    WHEN DELETING THEN
        INSERT INTO LOGS VALUES(SYSTIMESTAMP,'DELETE',null,:old.ID,:old.NAME,:old.GROUP_ID);
     END CASE;
END;
------------------------------
CREATE OR REPLACE PROCEDURE Restore(start_time IN TIMESTAMP)
IS 
    CURSOR selected_logs IS 
    SELECT * FROM LOGS 
    WHERE TIME>=start_time
    ORDER BY TIME DESC;
BEGIN
    FOR selected_log IN selected_logs
    LOOP
        CASE
            WHEN selected_log.STATEMENT='INSERT' THEN
                DELETE FROM STUDENTS WHERE ID=selected_log.ID_NEW;
            WHEN selected_log.STATEMENT='UPDATE' THEN
                UPDATE STUDENTS
                SET ID=selected_log.ID_OLD,NAME=selected_log.NAME,GROUP_ID =selected_log.GROUP_ID
                WHERE ID=selected_log.ID_NEW;
            WHEN selected_log.STATEMENT='DELETE' THEN
                INSERT INTO STUDENTS VALUES(selected_log.ID_OLD,selected_log.NAME,selected_log.GROUP_ID);
        END CASE;
        DELETE FROM LOGS WHERE TIME=selected_log.TIME;
    END LOOP;
END Restore;
------------------------------
CREATE OR REPLACE PROCEDURE Restore_by_interval(interval IN INTERVAL DAY TO SECOND)
IS 
BEGIN
    Restore(LOCALTIMESTAMP - interval);
END Restore_by_interval;
------------------------------
CREATE OR REPLACE TRIGGER students_change 
AFTER INSERT OR DELETE OR UPDATE ON STUDENTS
FOR EACH ROW
DECLARE
    rowcount NUMBER;
BEGIN
    CASE
    WHEN INSERTING THEN
        UPDATE GROUPS
        SET C_VAL=C_VAL+1
        WHERE ID = :new.GROUP_ID;
    WHEN UPDATING THEN
        UPDATE GROUPS
        SET C_VAL=C_VAL+1
        WHERE ID = :new.GROUP_ID;
        UPDATE GROUPS
        SET C_VAL=C_VAL-1
        WHERE ID = :old.GROUP_ID;
    WHEN DELETING THEN
        UPDATE GROUPS
        SET C_VAL=C_VAL-1
        WHERE ID = :old.GROUP_ID;
     END CASE;
END;
------------------------------



CREATE OR REPLACE PROCEDURE cascade_delete(old_id NUMBER) IS
BEGIN
  DELETE FROM Students WHERE group_id = old_id;
END;
------------------------------



CREATE OR REPLACE TRIGGER group_del
  AFTER DELETE ON Groups
  FOR EACH ROW
BEGIN
  cascade_delete(:old.id);
END;

------------------------------


CREATE OR REPLACE PROCEDURE group_decrement_cval(s_grid NUMBER) IS
  group_exists NUMBER;
BEGIN
  IF group_exists > 0 THEN
    UPDATE Groups SET c_val = c_val - 1 WHERE id = s_grid;
  END IF;
END;
------------------------------


CREATE OR REPLACE PROCEDURE group_increment_cval(s_grid NUMBER) IS
BEGIN
  UPDATE Groups SET c_val = c_val + 1 WHERE id = s_grid;
END;
------------------------------


CREATE OR REPLACE PROCEDURE group_swap_cval(s_grid_old NUMBER, s_grid_new NUMBER) IS
BEGIN
  UPDATE Groups SET c_val = c_val - 1 WHERE id = s_grid_old;
  UPDATE Groups SET c_val = c_val + 1 WHERE id = s_grid_new;
END;
------------------------------



CREATE OR REPLACE TRIGGER students_change
  AFTER INSERT OR UPDATE OR DELETE ON Students
  FOR EACH ROW
BEGIN
  IF INSERTING THEN
      group_increment_cval(:NEW.group_id);
  ELSIF UPDATING THEN
      group_swap_cval(:NEW.group_id, :OLD.group_id);
  ELSIF DELETING THEN
      group_decrement_cval(:OLD.group_id);
  END IF;
END;
