COPY academic_years(academic_year_id, start_year, end_year, start_date, end_date)
FROM '/data/academic_years.csv'
DELIMITER ','
CSV HEADER;

COPY departments(dept_code, division, name, office_number, building_code, blurb)
FROM '/data/department.csv'
DELIMITER ','
CSV HEADER;

COPY degree_names(dept_code, department, major, degree_name)
FROM '/data/degree_names.csv'
DELIMITER ','
CSV HEADER;

COPY students(name, phone, address, gender, student_id, email)
FROM '/data/all_students.csv'
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

COPY dropouts(student_id, dropout_year)
FROM '/data/dropouts.csv'
DELIMITER ','
CSV HEADER;

COPY graduations(student_id, dept_code, graduation_year)
FROM '/data/graduations.csv'
DELIMITER ','
CSV HEADER;

COPY leave_of_absence(student_id, loa_year, return_year)
FROM '/data/leave_of_absence.csv'
DELIMITER ','
CSV HEADER;

COPY student_enrollment(student_id, academic_year, year, major)
FROM '/data/student_enrollment.csv'
DELIMITER ','
CSV HEADER;