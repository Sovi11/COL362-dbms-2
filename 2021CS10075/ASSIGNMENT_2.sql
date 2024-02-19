drop table if exists department cascade ;
drop table if exists valid_entry cascade;
drop table if exists professor cascade;
drop table if exists courses cascade;
drop table if exists student cascade;
drop table if exists course_offers cascade;
drop table if exists student_courses cascade;
drop table if exists student_dept_change cascade;

create table department(
    dept_id CHAR(3) PRIMARY KEY,
    dept_name VARCHAR(40)
);

create table valid_entry(
    dept_id CHAR(3) references department(dept_id) on update cascade on delete cascade,
    entry_year INTEGER,
    seq_number INTEGER,
    PRIMARY KEY(dept_id, entry_year)
);

create table professor(
    professor_id VARCHAR(10) PRIMARY KEY,
    professor_first_name VARCHAR(40),
    professor_last_name VARCHAR(40),
    office_number VARCHAR(20),
    contact_number CHAR(10),
    start_year INTEGER,
    resign_year INTEGER,
    dept_id CHAR(3) references department(dept_id) on update cascade
);

create table courses(
    course_id CHAR(6) PRIMARY KEY,
    course_name VARCHAR(20),
    course_desc TEXT,
    credits NUMERIC,
    dept_id CHAR(3) references department(dept_id) on update cascade on delete cascade
);

create table student(
    first_name VARCHAR(40),
    last_name VARCHAR(40),
    student_id CHAR(11) PRIMARY KEY,
    address VARCHAR(100),
    contact_number CHAR(10),
    email_id VARCHAR(50),
    tot_credits NUMERIC,
    dept_id CHAR(3) references department(dept_id) on update cascade
);

create table course_offers(
    course_id CHAR(6) references courses(course_id) on update cascade,
    session VARCHAR(9),
    semester INTEGER,
    professor_id VARCHAR(10) references professor(professor_id) on update cascade,
    capacity INTEGER,
    enrollments INTEGER,
    PRIMARY KEY(course_id, session, semester)
);

create table student_courses(
    student_id CHAR(11) references student(student_id) on update cascade on delete cascade,
    course_id CHAR(6) references courses(course_id) on update cascade,
    session VARCHAR(9),
    semester INTEGER,
    grade NUMERIC,
    PRIMARY KEY(student_id, course_id, session, semester)
);


alter table student
add constraint student_first_name_not_null check (first_name is not null);

alter table student
add constraint student_id_not_null check (student_id is not null);

alter table student
add constraint student_contact_number_not_null check (contact_number is not null);

alter table student
add constraint student_contact_number_unique UNIQUE (contact_number);

alter table student
add constraint student_email_id_unique UNIQUE (email_id);

alter table student
add constraint student_tot_credits_not_null check (tot_credits is not null);

alter table courses
add constraint courses_course_id_not_null check (course_id is not null);

alter table courses
add constraint courses_course_name_not_null check (course_name is not null);

alter table courses
add constraint courses_course_name_unique UNIQUE (course_name);

alter table courses
add constraint courses_credits_not_null check (credits is not null);

alter table student_courses
add constraint student_courses_grade_not_null check (grade is not null);

alter table course_offers 
add constraint course_offers_semester_not_null check (semester is not null);

alter table professor
add constraint professor_professor_first_name_not_null check (professor_first_name is not null);

alter table professor
add constraint professor_professor_last_name_not_null check (professor_last_name is not null);

alter table professor
add constraint professor_contact_number_not_null check (contact_number is not null);


alter table valid_entry
add constraint valid_entry_entry_year_not_null check (entry_year is not null);

alter table valid_entry
add constraint valid_entry_seq_number_not_null check (seq_number is not null);

alter table department
add constraint department_dept_name_not_null check (dept_name is not null);

alter table department
add constraint department_dept_name_unique UNIQUE (dept_name);


create or replace function validate_course_id_func(course_id CHAR(6))
returns boolean as $$
begin
    if substring(course_id, 1, 3) in (select dept_id from department) and substring(course_id, 4, 6) ~ '[0-9]+' then
        return true;
    else
        return false;
    end if;
end;
$$ language plpgsql;


alter table student
add constraint student_tot_credits_check check (tot_credits >= 0);

alter table courses
add constraint courses_credits_check check (credits > 0);

alter table student_courses
add constraint student_courses_grade_check check (grade between 0 and 10);

alter table professor
add constraint professor_start_resign_year_check check (start_year <= resign_year);

alter table course_offers
add constraint course_offers_semester_check check (semester between 1 and 2);

alter table student_courses
add constraint student_courses_semester_check check (semester between 1 and 2);

alter table courses
add constraint courses_course_id_check check (validate_course_id_func(course_id));




CREATE OR REPLACE FUNCTION validate_student_id()
RETURNS TRIGGER AS $$
BEGIN
    IF (NEW.student_id ~ '[0-9][0-9][0-9][0-9][A-Z][A-Z][A-Z][0-9][0-9][0-9]') THEN
        IF (EXISTS (SELECT 1 FROM valid_entry WHERE dept_id = SUBSTRING(NEW.student_id FROM 5 FOR 3) AND entry_year = CAST(SUBSTRING(NEW.student_id FROM 1 FOR 4) AS INTEGER) AND seq_number = CAST(SUBSTRING(NEW.student_id FROM 8 FOR 3) AS INTEGER))) THEN
            RETURN NEW;
        ELSE
            RAISE EXCEPTION 'Invalid ';
        END IF;
    ELSE
        RAISE EXCEPTION 'Invalid format';
    END IF;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE TRIGGER validate_student_id
BEFORE INSERT ON student
FOR EACH ROW
EXECUTE FUNCTION validate_student_id();



CREATE OR REPLACE FUNCTION update_seq_number()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE valid_entry
    SET seq_number = seq_number + 1
    WHERE dept_id = SUBSTRING(NEW.student_id FROM 5 FOR 3) AND entry_year = CAST(SUBSTRING(NEW.student_id FROM 1 FOR 4) AS INTEGER) ;
    RETURN NEW;
END; $$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER update_seq_number
AFTER INSERT ON student
FOR EACH ROW
EXECUTE FUNCTION update_seq_number();




CREATE OR REPLACE FUNCTION validate_email()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.email_id like '%@%.iitd.ac.in' AND SUBSTRING(NEW.email_id FROM 1 FOR 10) ~ NEW.student_id AND SUBSTRING(NEW.email_id FROM 12 FOR 3) ~ SUBSTRING(NEW.email_id FROM 5 FOR 3) THEN
        RETURN NEW;
    ELSE
        RAISE EXCEPTION 'Invalid email id';
    END IF;
END; $$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER validate_email
BEFORE INSERT ON student
FOR EACH ROW
EXECUTE FUNCTION validate_email();


create table if not exists student_dept_change(
    old_student_id CHAR(11),
    old_dept_id CHAR(3) references department(dept_id) on delete cascade,
    new_student_id CHAR(11) ,
    new_dept_id CHAR(3) references department(dept_id) on delete cascade,
    PRIMARY KEY(old_student_id, new_student_id)
);



CREATE OR REPLACE FUNCTION email_change_on_student_id()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE student
    SET email_id = NEW.student_id || '@' || NEW.dept_id || '.iitd.ac.in'    
    WHERE student_id = NEW.student_id;
    RETURN NEW;
END; $$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER email_change_on_student_id
AFTER UPDATE ON student
FOR EACH ROW
WHEN (OLD.student_id IS DISTINCT FROM NEW.student_id)
EXECUTE FUNCTION email_change_on_student_id();


CREATE OR REPLACE FUNCTION log_student_dept_change()
RETURNS TRIGGER AS $$
DECLARE
    avg_grade NUMERIC;
    num_courses INTEGER;
    new_sequence_num CHAR(3);
    new_stud_in_student_table CHAR(11);
    temp_dep_name VARCHAR(40);
BEGIN
    SELECT dept_name INTO temp_dep_name FROM department WHERE dept_id = NEW.dept_id;
    IF temp_dep_name = 'JAISHREERAM' THEN
        RETURN NEW ;
    END IF;
    IF (NEW.dept_id <> OLD.dept_id) THEN
        new_sequence_num := (SELECT LPAD(seq_number::CHAR(3) , 3,'0')  FROM valid_entry WHERE dept_id = NEW.dept_id AND entry_year = CAST(SUBSTRING(NEW.student_id FROM 1 FOR 4) AS INTEGER)) ;
        new_stud_in_student_table := CONCAT(SUBSTRING(NEW.student_id FROM 1 FOR 4), NEW.dept_id, new_sequence_num);
        IF EXISTS (SELECT 1 FROM student_dept_change WHERE new_student_id = OLD.student_id) THEN
            RAISE EXCEPTION 'Department can be changed only once';
        END IF;
        IF CAST(SUBSTRING(NEW.student_id FROM 1 FOR 4) AS INTEGER) < 2022 THEN
            RAISE EXCEPTION 'Entry year must be >= 2022';
        END IF;
        SELECT AVG(grade) INTO avg_grade FROM student_courses WHERE student_id = OLD.student_id;
        SELECT COUNT(*) INTO num_courses FROM student_courses WHERE student_id = OLD.student_id;
        IF num_courses = 0 THEN
        RAISE EXCEPTION '0 courses done';
        END IF;
        IF avg_grade <= 8.5 THEN
            RAISE EXCEPTION 'Low Grade';
        END IF;
        UPDATE student SET student_id = new_stud_in_student_table WHERE student_id = OLD.student_id;
        UPDATE valid_entry SET seq_number = seq_number + 1 WHERE dept_id = SUBSTRING(new_stud_in_student_table FROM 5 FOR 3) AND entry_year = CAST(SUBSTRING(new_stud_in_student_table FROM 1 FOR 4) AS INTEGER);
        UPDATE student SET email_id = new_stud_in_student_table || '@' || NEW.dept_id || '.iitd.ac.in' WHERE student_id = new_stud_in_student_table;
        INSERT INTO student_dept_change VALUES (OLD.student_id, OLD.dept_id, new_stud_in_student_table, NEW.dept_id);
    END IF;
    RETURN NEW;
END; $$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER log_student_dept_change
AFTER UPDATE ON student
FOR EACH ROW
EXECUTE FUNCTION log_student_dept_change();

CREATE OR REPLACE FUNCTION update_course_eval()
RETURNS TRIGGER AS $$
BEGIN
    REFRESH MATERIALIZED VIEW course_eval;
    RETURN NEW;
END; $$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER update_course_eval
AFTER INSERT OR UPDATE ON student_courses
FOR EACH ROW
EXECUTE FUNCTION update_course_eval();

DROP MATERIALIZED VIEW IF EXISTS course_eval;

CREATE MATERIALIZED VIEW course_eval AS
SELECT course_id, session, semester, COUNT(student_id) AS number_of_students, AVG(grade) AS average_grade, MAX(grade) AS max_grade, MIN(grade) AS min_grade
FROM student_courses
GROUP BY course_id, session, semester;


CREATE OR REPLACE FUNCTION update_tot_credits_func()
RETURNS TRIGGER AS $$
DECLARE
    new_credits NUMERIC;
BEGIN
    SELECT credits INTO new_credits 
    FROM courses
    WHERE course_id = NEW.course_id;
    
    UPDATE student
    SET tot_credits = tot_credits + new_credits
    WHERE student_id = NEW.student_id;
    
    RETURN NEW;

END; $$ LANGUAGE plpgsql;   

CREATE OR REPLACE TRIGGER update_tot_credits
AFTER INSERT ON student_courses
FOR EACH ROW
EXECUTE FUNCTION update_tot_credits_func();

CREATE OR REPLACE FUNCTION check_course_limit()
RETURNS TRIGGER AS $$
DECLARE
    num_courses INTEGER;
    new_credits NUMERIC;
    tot_credits_temp NUMERIC;
    tot_credits_temp_sum NUMERIC;
BEGIN
    SELECT COUNT(*) INTO num_courses
    FROM student_courses
    WHERE student_id = NEW.student_id AND session = NEW.session AND semester = NEW.semester;
    
    IF num_courses >= 5 THEN
        RAISE EXCEPTION 'Invalid';
    END IF;

    SELECT credits INTO new_credits 
    FROM courses
    WHERE course_id = NEW.course_id;
    
    SELECT tot_credits INTO tot_credits_temp
    FROM student
    WHERE student_id = NEW.student_id;
    
    IF tot_credits_temp + new_credits > 60 THEN
        RAISE EXCEPTION 'Invalid';
    END IF;

    tot_credits_temp_sum := (SELECT sum(credits) FROM courses c 
    join student_courses sc on c.course_id = sc.course_id
    where (student_id = NEW.student_id) and (session = NEW.session) and (semester = NEW.semester));
    
    IF tot_credits_temp_sum + credits_sum > 26 THEN
        RAISE EXCEPTION 'Invalid: Maximum credit criteria exceeded';
    END IF;
    
    RETURN NEW;

END; $$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION validate_student_five_cred_courses_func()
RETURNS TRIGGER AS $$
DECLARE
    first_year INTEGER;
BEGIN
    first_year := SUBSTRING(NEW.student_id FROM 1 FOR 4)::INTEGER;
    
    IF (SELECT credits FROM courses WHERE course_id = NEW.course_id) = 5 AND first_year != SUBSTRING(NEW.session FROM 1 FOR 4)::INTEGER THEN
        RAISE EXCEPTION 'Invalid: 5-credit course can be taken only in the first year';
    END IF;
    
    RETURN NEW;
END; $$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER validate_student_five_cred_courses_trigger
BEFORE INSERT ON student_courses
FOR EACH ROW
EXECUTE FUNCTION validate_student_five_cred_courses_func();


CREATE MATERIALIZED VIEW student_semester_summary AS
SELECT 
    sc.student_id,
    sc.session,
    sc.semester,
    SUM(CASE WHEN sc.grade >= 5.0 THEN sc.grade * c.credits ELSE 0 END) / SUM(CASE WHEN sc.grade >= 5.0 THEN c.credits ELSE 0 END) AS sgpa,
    SUM(CASE WHEN sc.grade >= 5.0 THEN c.credits ELSE 0 END) AS credits
FROM 
    student_courses sc
JOIN 
    courses c ON sc.course_id = c.course_id
GROUP BY 
    sc.student_id, sc.session, sc.semester;




CREATE OR REPLACE FUNCTION refresh_student_semester_summary()
RETURNS TRIGGER AS $$
BEGIN
    REFRESH MATERIALIZED VIEW student_semester_summary;
    RETURN NEW;
END; $$ LANGUAGE plpgsql;




CREATE OR REPLACE TRIGGER refresh_student_semester_summary
AFTER INSERT OR UPDATE OR DELETE ON student_courses
FOR EACH ROW
EXECUTE FUNCTION refresh_student_semester_summary();



CREATE OR REPLACE FUNCTION check_full_course_func()
RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM course_offers WHERE course_id = NEW.course_id AND session = NEW.session AND semester = NEW.semester AND enrollments = capacity) THEN
        RAISE EXCEPTION 'Course is full';
    ELSE
        UPDATE course_offers
        SET enrollments = enrollments + 1
        WHERE course_id = NEW.course_id AND session = NEW.session AND semester = NEW.semester;
    END IF;
    
    RETURN NEW;
END; $$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER check_full_course
BEFORE INSERT ON student_courses
FOR EACH ROW
EXECUTE FUNCTION check_full_course_func();




CREATE OR REPLACE FUNCTION update_student_courses()
RETURNS TRIGGER AS $$
DECLARE
    tot_credits_temp NUMERIC;
BEGIN
    IF TG_OP = 'DELETE' THEN
        
        SELECT tot_credits INTO tot_credits_temp
        FROM student
        WHERE student_id IN (SELECT student_id FROM student_courses WHERE course_id = OLD.course_id AND session = OLD.session AND semester = OLD.semester);
        
        UPDATE student
        SET tot_credits = tot_credits_temp - (SELECT credits FROM courses WHERE course_id = OLD.course_id)
        WHERE student_id IN (SELECT student_id FROM student_courses WHERE course_id = OLD.course_id AND session = OLD.session AND semester = OLD.semester);

        DELETE FROM student_courses
        WHERE course_id = OLD.course_id AND session = OLD.session AND semester = OLD.semester;
        
    ELSIF TG_OP = 'INSERT' THEN
        IF NOT EXISTS (SELECT 1 FROM courses WHERE course_id = NEW.course_id) THEN
            RAISE EXCEPTION 'Invalid course id';
        END IF;
        
        IF NOT EXISTS (SELECT 1 FROM professor WHERE professor_id = NEW.professor_id) THEN
            RAISE EXCEPTION 'Invalid professor id';
        END IF;
    END IF;
    
    RETURN NEW;
END; $$ LANGUAGE plpgsql;

CREATE TRIGGER update_student_courses_delete
AFTER DELETE ON course_offers
FOR EACH ROW
EXECUTE FUNCTION update_student_courses();

CREATE TRIGGER update_student_courses_insert
AFTER INSERT ON course_offers
FOR EACH ROW
EXECUTE FUNCTION update_student_courses();




CREATE OR REPLACE FUNCTION validate_course_offers()
RETURNS TRIGGER AS $$
DECLARE
    num_courses INTEGER;
    resign_year_temp INTEGER;
BEGIN
    SELECT COUNT(*) INTO num_courses
    FROM course_offers
    WHERE professor_id = NEW.professor_id AND session = NEW.session;
    
    IF num_courses >= 4 THEN
        RAISE EXCEPTION 'Invalid: Professor is teaching more than 4 courses in a session';
    END IF;
    
    SELECT resign_year INTO resign_year_temp
    FROM professor
    WHERE professor_id = NEW.professor_id;
    
    IF resign_year_temp IS NOT NULL AND resign_year_temp < SUBSTRING(NEW.session from 1 for 4)::INTEGER THEN 
        RAISE EXCEPTION 'Invalid: Professor has resigned';
    END IF;
    
    RETURN NEW;
END; $$ LANGUAGE plpgsql;

CREATE TRIGGER validate_course_offers
BEFORE INSERT ON course_offers
FOR EACH ROW
EXECUTE FUNCTION validate_course_offers();



CREATE OR REPLACE FUNCTION update_dept_function()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO department 
    VALUES (NEW.dept_id, 'JAISHREERAM');

    UPDATE professor
    SET dept_id = NEW.dept_id
    WHERE dept_id = OLD.dept_id;

    UPDATE valid_entry
    SET dept_id = NEW.dept_id
    WHERE dept_id = OLD.dept_id;

    UPDATE student_dept_change
    SET new_dept_id = NEW.dept_id
    WHERE new_dept_id = OLD.dept_id;

    UPDATE student_dept_change
    SET old_dept_id = NEW.dept_id
    WHERE old_dept_id = OLD.dept_id;

    UPDATE student
    SET student_id = CONCAT(SUBSTRING(student_id FROM 1 FOR 4), NEW.dept_id, SUBSTRING(student_id FROM 8 FOR 3)), dept_id = NEW.dept_id
    WHERE dept_id = OLD.dept_id;

    UPDATE courses
    SET course_id = CONCAT(NEW.dept_id, SUBSTRING(course_id fROM 4 FOR 3)), dept_id = NEW.dept_id
    WHERE dept_id = OLD.dept_id;

    DELETE FROM department
    WHERE dept_id = OLD.dept_id;

    UPDATE department
    SET dept_name = OLD.dept_name
    where dept_id = NEW.dept_id;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE TRIGGER update_dept_function_trigger
BEFORE UPDATE ON department
FOR EACH ROW
WHEN (OLD.dept_id IS DISTINCT FROM NEW.dept_id)
EXECUTE FUNCTION update_dept_function();


CREATE OR REPLACE FUNCTION delete_department()
RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS(
        SELECT 1
        FROM student
        WHERE dept_id = OLD.dept_id
    )
    THEN
        RAISE EXCEPTION 'Department has students';
    ELSE
        DELETE FROM course_offers
        WHERE SUBSTRING(course_id FROM 1 FOR 3) = OLD.dept_id;

        DELETE FROM courses
        WHERE dept_id = OLD.dept_id;

        DELETE FROM professor
        WHERE dept_id = OLD.dept_id;

        RETURN OLD;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER delete_department_trigger
BEFORE DELETE ON department
FOR EACH ROW
EXECUTE FUNCTION delete_department();





