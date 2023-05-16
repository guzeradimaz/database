drop table films;
drop table actors;
drop table producers;



CREATE TABLE actors
(
    actor_id  NUMBER(10)
        CONSTRAINT PK_actors PRIMARY KEY,
    first_name   VARCHAR2(50),
    last_name    VARCHAR2(50),
    email        VARCHAR2(100) UNIQUE,
    phone_number VARCHAR2(50)
);

CREATE TABLE producers
(
    producer_id   NUMBER(10)
        CONSTRAINT PK_producers PRIMARY KEY,
    producer_name VARCHAR2(100),
    filmography   VARCHAR2(500),
    age         NUMBER
);

CREATE TABLE films
(
    film_id    NUMBER(10)
        CONSTRAINT PK_films PRIMARY KEY,
    film_date  DATE,
    actor_id NUMBER(10),
    producer_id NUMBER(10),
    box_office      NUMBER(10),
    CONSTRAINT fk_actor FOREIGN KEY (actor_id) REFERENCES actors (actor_id),
    CONSTRAINT fk_producer FOREIGN KEY (producer_id) REFERENCES producers (producer_id)
);

drop table actors_history;
drop table producers_history;
drop table films_history;

CREATE TABLE actors_history
(
    action_id    number,
    actor_id  NUMBER(10),
    first_name   VARCHAR2(50),
    last_name    VARCHAR2(50),
    email        VARCHAR2(100),
    phone_number VARCHAR2(50),
    change_date  DATE,
    change_type  VARCHAR2(10)
);

CREATE TABLE producers_history
(
    action_id     number,
    producer_id   NUMBER(10),
    producer_name VARCHAR2(100),
    filmography   VARCHAR2(500),
    age         NUMBER,
    change_date   DATE,
    change_type   VARCHAR2(10)
);

CREATE TABLE films_history
(
    action_id   number,
    film_id    NUMBER(10),
    film_date  DATE,
    actor_id NUMBER(10),
    producer_id NUMBER(10),
    box_office      NUMBER(10),
    change_date DATE,
    change_type VARCHAR2(10)
);

drop table reports_history;
create table reports_history
(
    id          number GENERATED ALWAYS AS IDENTITY,
    report_date timestamp,
    CONSTRAINT PK_reports PRIMARY KEY (id)
);

insert into reports_history(report_date)
values (to_timestamp('1000-04-23 18:25:00', 'YYYY-MM-DD HH24:MI:SS'));
select *
from reports_history;

drop sequence history_seq;
create sequence history_seq start with 1;



--main

delete
from films;
delete
from actors;
delete
from producers;

INSERT INTO actors (actor_id, first_name, last_name, email, phone_number)
VALUES (1, 'first_name1', 'last_name1', 'email1@gmail.com', 'phone_number1');

INSERT INTO actors (actor_id, first_name, last_name, email, phone_number)
VALUES (2, 'first_name2', 'last_name2', 'email2@gmail.com', 'phone_number2');

UPDATE actors
set phone_number = 'new phone'
where actor_id = 2;

INSERT INTO producers (producer_id, producer_name, filmography, age)
VALUES (1, 'producer_name1', 'filmography1', 45);

INSERT INTO producers (producer_id, producer_name, filmography, age)
VALUES (2, 'producer_name2', 'filmography2', 30);

INSERT INTO films (film_id, film_date, actor_id, producer_id, box_office)
VALUES (1, TO_DATE('2002-02-02', 'YYYY-MM-DD'), 1, 1, 1000);

INSERT INTO films (film_id, film_date, actor_id, producer_id, box_office)
VALUES (2, TO_DATE('2023-05-05', 'YYYY-MM-DD'), 2, 2, 22313);

delete
from films
where film_id = 2;

---------------

select *
from actors;
select *
from actors_history;

select *
from producers;
select *
from producers_history;

select *
from films;
select *
from films_history;

select *
from reports_history;

call rollback_by_date(to_timestamp('2023-04-29 10:00:00', 'YYYY-MM-DD HH24:MI:SS'));
call rollback_by_date(to_timestamp('2023-05-01 23:30:40', 'YYYY-MM-DD HH24:MI:SS'));


call FUNC_PACKAGE.ROLL_BACK(100000);
call FUNC_PACKAGE.ROLL_BACK(to_timestamp('2023-05-05 13:44:50', 'YYYY-MM-DD HH24:MI:SS'));
call FUNC_PACKAGE.REPORT();
call FUNC_PACKAGE.REPORT(to_timestamp('2023-04-29 10:00:00', 'YYYY-MM-DD HH24:MI:SS'),
                         to_timestamp('2024-05-02 23:30:40', 'YYYY-MM-DD HH24:MI:SS'));

--package

CREATE OR REPLACE PACKAGE func_package IS
    procedure roll_back(date_time timestamp);
    procedure roll_back(date_time number);
    procedure report(t_begin in timestamp, t_end in timestamp);
    procedure report;
END func_package;

CREATE OR REPLACE PACKAGE BODY func_package IS
    PROCEDURE roll_back(date_time timestamp) IS
    begin
        rollback_by_date(date_time);
    END roll_back;

    PROCEDURE roll_back(date_time number) IS
    BEGIN
        DECLARE
            current_time timestamp := systimestamp;
        BEGIN
            current_time := current_time - NUMTODSINTERVAL(date_time / 1000, 'SECOND');
            rollback_by_date(current_time);
        END;
    END roll_back;

    PROCEDURE report(t_begin in timestamp, t_end in timestamp) IS
        v_cur timestamp;
    begin

        SELECT CAST(SYSDATE AS TIMESTAMP) into v_cur FROM dual;

        if t_end > v_cur then
            create_report(t_begin, v_cur);
            insert into reports_history(report_date) values (v_cur);
        else
            create_report(t_begin, t_end);
            insert into reports_history(report_date) values (t_end);
        end if;
    END report;

    PROCEDURE report IS
        v_begin timestamp;
        v_cur   timestamp;
    begin

        SELECT CAST(SYSDATE AS TIMESTAMP) into v_cur FROM dual;

        select REPORT_DATE
        into v_begin
        from REPORTS_HISTORY
        where id = (select MAX(id) from REPORTS_HISTORY);

        create_report(v_begin, v_cur);

        insert into reports_history(report_date) values (v_cur);
    END report;

END func_package;


--trigger


CREATE OR REPLACE TRIGGER tr_actors_insert
    AFTER INSERT
    ON actors
    FOR EACH ROW
BEGIN
    INSERT INTO actors_history (action_id, actor_id, first_name, last_name, email, phone_number, change_date,
                                   change_type)
    VALUES (history_seq.nextval, :NEW.actor_id, :NEW.first_name, :NEW.last_name, :NEW.email, :NEW.phone_number,
            SYSDATE, 'INSERT');
END;

CREATE OR REPLACE TRIGGER tr_actors_update
    AFTER UPDATE
    ON actors
    FOR EACH ROW
DECLARE
    v_id number;
BEGIN
    INSERT INTO actors_history (action_id, actor_id, first_name, last_name, email, phone_number, change_date,
                                   change_type)
    VALUES (HISTORY_SEQ.nextval, :OLD.actor_id, :OLD.first_name, :OLD.last_name, :OLD.email, :OLD.phone_number,
            SYSDATE, 'DELETE');

    INSERT INTO actors_history (action_id, actor_id, first_name, last_name, email, phone_number, change_date,
                                   change_type)
    VALUES (HISTORY_SEQ.nextval, :OLD.actor_id, :OLD.first_name, :OLD.last_name, :OLD.email, :OLD.phone_number,
            SYSDATE, 'UPDATE');

    INSERT INTO actors_history (action_id, actor_id, first_name, last_name, email, phone_number, change_date,
                                   change_type)
    VALUES (HISTORY_SEQ.nextval, :NEW.actor_id, :NEW.first_name, :NEW.last_name, :NEW.email, :NEW.phone_number,
            SYSDATE, 'INSERT');
END;

CREATE OR REPLACE TRIGGER tr_actors_delete
    AFTER DELETE
    ON actors
    FOR EACH ROW
BEGIN
    INSERT INTO actors_history (action_id, actor_id, first_name, last_name, email, phone_number, change_date,
                                   change_type)
    VALUES (history_seq.nextval, :OLD.actor_id, :OLD.first_name, :OLD.last_name, :OLD.email, :OLD.phone_number,
            SYSDATE, 'DELETE');
END;

CREATE OR REPLACE TRIGGER tr_producers_insert
    AFTER INSERT
    ON producers
    FOR EACH ROW
BEGIN
    INSERT INTO producers_history (action_id, producer_id, producer_name, filmography, age, change_date, change_type)
    VALUES (history_seq.nextval, :new.producer_id, :new.producer_name, :new.filmography, :new.age, SYSDATE, 'INSERT');
END;

CREATE OR REPLACE TRIGGER tr_producers_update
    AFTER UPDATE
    ON producers
    FOR EACH ROW
DECLARE
    v_id number;
BEGIN
    v_id := HISTORY_SEQ.nextval;
    INSERT INTO producers_history (action_id, producer_id, producer_name, filmography, age, change_date, change_type)
    VALUES (v_id, :old.producer_id, :old.producer_name, :old.filmography, :old.age, SYSDATE, 'DELETE');

    INSERT INTO producers_history (action_id, producer_id, producer_name, filmography, age, change_date, change_type)
    VALUES (v_id, :old.producer_id, :old.producer_name, :old.filmography, :old.age, SYSDATE, 'UPDATE');

    INSERT INTO producers_history (action_id, producer_id, producer_name, filmography, age, change_date, change_type)
    VALUES (v_id, :new.producer_id, :new.producer_name, :new.filmography, :new.age, SYSDATE, 'INSERT');
END;

CREATE OR REPLACE TRIGGER tr_producers_delete
    AFTER DELETE
    ON producers
    FOR EACH ROW
BEGIN
    INSERT INTO producers_history (action_id, producer_id, producer_name, filmography, age, change_date, change_type)
    VALUES (history_seq.nextval, :old.producer_id, :old.producer_name, :old.filmography, :old.age, SYSDATE, 'DELETE');
END;

CREATE OR REPLACE TRIGGER tr_films_insert
    AFTER INSERT
    ON films
    FOR EACH ROW
DECLARE
BEGIN
    INSERT INTO films_history (action_id, film_id, film_date, actor_id, producer_id, box_office, change_date,
                                change_type)
    VALUES (history_seq.NEXTVAL, :NEW.film_id, :NEW.film_date, :NEW.actor_id, :NEW.producer_id, :NEW.box_office,
            SYSDATE, 'INSERT');
END;

CREATE OR REPLACE TRIGGER tr_films_update
    AFTER UPDATE
    ON films
    FOR EACH ROW
DECLARE
    v_id number;
BEGIN
    v_id := HISTORY_SEQ.nextval;
    INSERT INTO films_history (action_id, film_id, film_date, actor_id, producer_id, box_office, change_date,
                                change_type)
    VALUES (v_id, :OLD.film_id, :OLD.film_date, :OLD.actor_id, :OLD.producer_id, :OLD.box_office, SYSDATE, 'DELETE');

    INSERT INTO films_history (action_id, film_id, film_date, actor_id, producer_id, box_office, change_date,
                                change_type)
    VALUES (v_id, :OLD.film_id, :OLD.film_date, :OLD.actor_id, :OLD.producer_id, :OLD.box_office, SYSDATE, 'UPDATE');

    INSERT INTO films_history (action_id, film_id, film_date, actor_id, producer_id, box_office, change_date,
                                change_type)
    VALUES (v_id, :NEW.film_id, :NEW.film_date, :NEW.actor_id, :NEW.producer_id, :NEW.box_office, SYSDATE, 'INSERT');
END;

CREATE OR REPLACE TRIGGER tr_films_delete
    AFTER DELETE
    ON films
    FOR EACH ROW
DECLARE
BEGIN
    INSERT INTO films_history (action_id, film_id, film_date, actor_id, producer_id, box_office, change_date,
                                change_type)
    VALUES (history_seq.NEXTVAL, :OLD.film_id, :OLD.film_date, :OLD.actor_id, :OLD.producer_id, :OLD.box_office,
            SYSDATE, 'DELETE');
END;

--html

create or replace procedure rollback_by_date(date_time in timestamp)
as
begin
    disable_all_constraints('films');
    disable_all_constraints('actors');
    disable_all_constraints('producers');

    delete from films;
    delete from actors;
    delete from producers;

    for i in (select * from actors_history where CHANGE_DATE <= date_time order BY ACTION_ID)
        LOOP
            if i.CHANGE_TYPE = 'INSERT' then
                insert into actors values (i.actor_ID, i.FIRST_NAME, i.LAST_NAME, i.EMAIL, i.PHONE_NUMBER);
            elsif i.CHANGE_TYPE = 'DELETE' then
                delete from actors where actor_ID = i.actor_ID;
            end if;
        end loop;

    for i in (select * from producers_history where CHANGE_DATE <= date_time order BY ACTION_ID)
        LOOP
            if i.CHANGE_TYPE = 'INSERT' then
                insert into producers values (i.producer_ID, i.producer_NAME, i.filmography, i.age);
            elsif i.CHANGE_TYPE = 'DELETE' then
                delete from producers where producer_ID = i.producer_ID;
            end if;
        end loop;

    for i in (select * from films_history where CHANGE_DATE <= date_time order BY ACTION_ID)
        LOOP
            if i.CHANGE_TYPE = 'INSERT' then
                insert into films values (i.film_ID, i.film_DATE, i.actor_ID, i.producer_ID, i.box_office);
            elsif i.CHANGE_TYPE = 'DELETE' then
                delete from films where films.film_ID = i.film_ID;
            end if;
            commit;
        end loop;

    delete
    from actors_history
    where CHANGE_DATE > date_time;

    delete
    from producers_history
    where CHANGE_DATE > date_time;

    delete
    from films_history
    where CHANGE_DATE > date_time;

    enable_all_constraints('actors');
    enable_all_constraints('producers');
    enable_all_constraints('films');
end;

CREATE OR REPLACE PROCEDURE disable_all_constraints(p_table_name IN VARCHAR2) IS
BEGIN
    FOR c IN (SELECT constraint_name
              FROM user_constraints
              WHERE table_name = p_table_name)
        LOOP
            EXECUTE IMMEDIATE 'ALTER TABLE ' || p_table_name || ' DISABLE CONSTRAINT ' || c.constraint_name;
            DBMS_OUTPUT.PUT_LINE('ALTER TABLE ' || p_table_name || ' DISABLE CONSTRAINT ' || c.constraint_name);
        END LOOP;

    EXECUTE IMMEDIATE 'ALTER TABLE ' || p_table_name || ' DISABLE ALL TRIGGERS';
END;

CREATE OR REPLACE PROCEDURE enable_all_constraints(p_table_name IN VARCHAR2) IS
BEGIN
    FOR c IN (SELECT constraint_name
              FROM user_constraints
              WHERE table_name = p_table_name)
        LOOP
            EXECUTE IMMEDIATE 'ALTER TABLE ' || p_table_name || ' ENABLE CONSTRAINT ' || c.constraint_name;
            DBMS_OUTPUT.PUT_LINE('ALTER TABLE ' || p_table_name || ' ENABLE CONSTRAINT ' || c.constraint_name);
        END LOOP;

    EXECUTE IMMEDIATE 'ALTER TABLE ' || p_table_name || ' ENABLE ALL TRIGGERS';
END;


CREATE OR REPLACE DIRECTORY my_dir AS 'D:\';


create or replace procedure create_report(t_begin in timestamp, t_end in timestamp)
as
    v_result varchar2(4000);
    i_count  number;
    u_count  number;
    d_count  number;
    my_file  UTL_FILE.FILE_TYPE;
begin
    v_result :=
    '<!DOCTYPE html>
    <html>
    <head>
        <title>Database Changes</title>
        <style type="text/css">   
      .row {
        display: flex;
        width: 50%;
        margin: 20px auto;
        border-radius: 10px;
        box-shadow: 0 0 10px #cdcdcd;
        padding: 10px;
      }
      .row div{
        width: 300px;
        display: flex;
        flex-direction: column;
        align-items: center;
        text-align: center;
      }
      h1 {
        font-size: 24px;
        font-weight: bold;
        margin-top: 20px;
        margin-bottom: 10px;
        text-align: center;
        text-transform: uppercase;
        width: 250px;
      }
      h2 {
        font-size: 18px;
        margin-top: 5px;
        text-align: center;
        margin-bottom: 5px;
      }
      .magenta {
        color: gray;
      }
      .darkorange {
        color: black;
      }
        </style>
    </head>
    <body>';
    v_result := v_result || '<div class="row"><h1 class="darkorange">Table first:</h1><div>' || CHR(10);

    select count(*)
    into u_count
    from actors_history
    where CHANGE_DATE between t_begin and t_end
      and CHANGE_TYPE = 'UPDATE';

    select count(*)
    into i_count
    from actors_history
    where CHANGE_DATE between t_begin and t_end
      and CHANGE_TYPE = 'INSERT';

    select count(*)
    into d_count
    from actors_history
    where CHANGE_DATE between t_begin and t_end
      and CHANGE_TYPE = 'DELETE';

    i_count := i_count - u_count;
    d_count := d_count - u_count;

    v_result := v_result || '<h2 class="magenta">   Insert operations amount: ' || i_count || '</h2>' || CHR(10) ||
                '<h2 class="magenta">   Update operations amount: ' || u_count || '</h2>' || CHR(10) ||
                '<h2 class="magenta">   Delete operations amount: ' || d_count || '</h2>' || CHR(10);
    v_result := v_result || '</div></div>';

    select count(*)
    into u_count
    from producers_history
    where CHANGE_DATE between t_begin and t_end
      and CHANGE_TYPE = 'UPDATE';

    select count(*)
    into i_count
    from producers_history
    where CHANGE_DATE between t_begin and t_end
      and CHANGE_TYPE = 'INSERT';

    select count(*)
    into d_count
    from producers_history
    where CHANGE_DATE between t_begin and t_end
      and CHANGE_TYPE = 'DELETE';

    i_count := i_count - u_count;
    d_count := d_count - u_count;

    v_result := v_result || '<div class="row"><h1 class="darkorange">Table second:</h1><div>' || CHR(10) ||
                '<h2 class="magenta">   Insert operations amount: ' || i_count || '</h2>' || CHR(10) ||
                '<h2 class="magenta">  Update operations amount: ' || u_count || '</h2>' || CHR(10) ||
                '<h2 class="magenta">   Delete operations amount: ' || d_count || '</h2>' || CHR(10);

    v_result := v_result || '</div></div>';
    select count(*)
    into u_count
    from films_history
    where CHANGE_DATE between t_begin and t_end
      and CHANGE_TYPE = 'UPDATE';

    select count(*)
    into i_count
    from films_history
    where CHANGE_DATE between t_begin and t_end
      and CHANGE_TYPE = 'INSERT';

    select count(*)
    into d_count
    from films_history
    where CHANGE_DATE between t_begin and t_end
      and CHANGE_TYPE = 'DELETE';

    i_count := i_count - u_count;
    d_count := d_count - u_count;

    v_result := v_result || '<div class="row"><h1 class="darkorange">Table third:</h1><div>' || CHR(10) ||
                '<h2 class="magenta">  Insert operations amount: ' || i_count || '</h2>' || CHR(10) ||
                '<h2 class="magenta">   Update operations amount: ' || u_count || '</h2>' || CHR(10) ||
                '<h2 class="magenta">   Delete operations amount: ' || d_count || '</h2>' || CHR(10);
                
    v_result := v_result || '</div></div>';
    v_result := v_result || '</body></html>';
    
    DBMS_OUTPUT.PUT_LINE(v_result);
    my_file := UTL_FILE.FOPEN('MY_DIR', 'report.html', 'w');
    UTL_FILE.PUT_LINE(my_file, v_result);
    UTL_FILE.FCLOSE(my_file);

end;