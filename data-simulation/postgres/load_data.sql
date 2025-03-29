COPY departments(dept_code, division, name, office_number, building_code, blurb)
FROM '/data/department.csv'
DELIMITER ','
CSV HEADER;

COPY students(name, phone, address, gender, student_id, email, year, department)
FROM '/data/students.csv'
DELIMITER ','
CSV HEADER;

COPY faculty(instructor_code, first_name, last_name, dept_code, phone, email, office_number, building_code, status, terminal_degree, institution)
FROM '/data/instructor.csv'
DELIMITER ','
CSV HEADER;

COPY courses(course_code, title, description, dept_code, credits, pre_reqs, core_area, inquiry_area, recommendation)
FROM '/data/course.csv'
DELIMITER ','
CSV HEADER;

COPY enrollment(student_id, course_code, semester, is_taking, grade)
FROM '/data/enrollment.csv'
DELIMITER ','
CSV HEADER;

COPY evaluations(student_id, course_code, evaluation, weight, score)
FROM '/data/evaluations.csv'
DELIMITER ','
CSV HEADER;