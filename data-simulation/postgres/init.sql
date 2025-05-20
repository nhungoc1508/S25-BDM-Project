CREATE TABLE academic_years (
    academic_year_id VARCHAR(20) PRIMARY KEY,
    start_year INT NOT NULL,
    end_year INT NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL
);

CREATE TABLE departments (
    dept_code VARCHAR(10) PRIMARY KEY,
    division VARCHAR(50) NOT NULL,
    name VARCHAR(100) NOT NULL,
    office_number VARCHAR(20),
    building_code VARCHAR(10),
    blurb TEXT
);

CREATE TABLE degree_names (
    degree_id SERIAL PRIMARY KEY,
    dept_code VARCHAR(10) NOT NULL,
    department VARCHAR(50) NOT NULL,
    major VARCHAR(100) NOT NULL,
    degree_name VARCHAR(200) NOT NULL,
    FOREIGN KEY (dept_code) REFERENCES departments(dept_code) ON DELETE CASCADE
);

CREATE TABLE students (
    student_id VARCHAR(20) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    phone VARCHAR(30),
    address TEXT,
    gender VARCHAR(10) CHECK (gender IN ('male', 'female', 'other')),
    email VARCHAR(255) UNIQUE NOT NULL
    -- year INT CHECK (year >= 1 AND year <= 4),
    -- department VARCHAR(10) NOT NULL,
    -- FOREIGN KEY (department) REFERENCES departments(dept_code) ON DELETE SET NULL
);

CREATE TABLE faculty (
    instructor_code VARCHAR(20) PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    dept_code VARCHAR(10) NOT NULL,
    phone VARCHAR(20),
    email VARCHAR(100) NOT NULL,
    office_number VARCHAR(20),
    building_code VARCHAR(10),
    status VARCHAR(50),
    terminal_degree VARCHAR(50),
    institution VARCHAR(100),
    FOREIGN KEY (dept_code) REFERENCES departments(dept_code) ON DELETE SET NULL
);

CREATE TABLE courses (
    course_code VARCHAR(20) PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    description TEXT,
    dept_code VARCHAR(10),
    credits INT,
    pre_reqs TEXT,
    core_area TEXT,
    inquiry_area TEXT,
    recommendation TEXT,
    FOREIGN KEY (dept_code) REFERENCES departments(dept_code) ON DELETE SET NULL
);

CREATE TABLE enrollment (
    student_id VARCHAR(20),
    course_code VARCHAR(20),
    semester VARCHAR(50) NOT NULL,
    is_taking BOOLEAN NOT NULL,
    grade DECIMAL(5,2) CHECK (grade >= 0),
    PRIMARY KEY (student_id, course_code, semester),
    FOREIGN KEY (student_id) REFERENCES students(student_id) ON DELETE CASCADE,
    FOREIGN KEY (course_code) REFERENCES courses(course_code) ON DELETE CASCADE
);

CREATE TABLE evaluations (
    evaluation_id SERIAL PRIMARY KEY,
    student_id VARCHAR(20),
    course_code VARCHAR(20),
    evaluation VARCHAR(100) NOT NULL,
    weight INT CHECK (weight >= 0 AND weight <= 100),
    score DECIMAL(5,2) CHECK (score >= 0),
    FOREIGN KEY (student_id) REFERENCES students(student_id) ON DELETE CASCADE,
    FOREIGN KEY (course_code) REFERENCES courses(course_code) ON DELETE CASCADE
);

CREATE TABLE dropouts (
    dropout_id SERIAL PRIMARY KEY,
    student_id VARCHAR(20) NOT NULL,
    dropout_year VARCHAR(20) NOT NULL,
    FOREIGN KEY (student_id) REFERENCES students(student_id) ON DELETE CASCADE,
    FOREIGN KEY (dropout_year) REFERENCES academic_years(academic_year_id) ON DELETE SET NULL
);

CREATE TABLE graduations (
    graduation_id SERIAL PRIMARY KEY,
    student_id VARCHAR(20) NOT NULL,
    dept_code VARCHAR(10) NOT NULL,
    graduation_year VARCHAR(20) NOT NULL,
    FOREIGN KEY (student_id) REFERENCES students(student_id) ON DELETE CASCADE,
    FOREIGN KEY (dept_code) REFERENCES departments(dept_code) ON DELETE SET NULL,
    FOREIGN KEY (graduation_year) REFERENCES academic_years(academic_year_id) ON DELETE SET NULL
);

CREATE TABLE leave_of_absence (
    loa_id SERIAL PRIMARY KEY,
    student_id VARCHAR(20) NOT NULL,
    loa_year VARCHAR(20) NOT NULL,
    return_year VARCHAR(20) NOT NULL,
    FOREIGN KEY (student_id) REFERENCES students(student_id) ON DELETE CASCADE,
    FOREIGN KEY (loa_year) REFERENCES academic_years(academic_year_id) ON DELETE SET NULL,
    FOREIGN KEY (return_year) REFERENCES academic_years(academic_year_id) ON DELETE SET NULL
);

CREATE TABLE student_enrollment (
    student_id VARCHAR(20) NOT NULL,
    academic_year VARCHAR(20) NOT NULL,
    year INT CHECK (year >= 1 AND year <= 4),
    major VARCHAR(10) NOT NULL,
    PRIMARY KEY (student_id, academic_year),
    FOREIGN KEY (student_id) REFERENCES students(student_id) ON DELETE CASCADE,
    FOREIGN KEY (academic_year) REFERENCES academic_years(academic_year_id) ON DELETE SET NULL,
    FOREIGN KEY (major) REFERENCES departments(dept_code) ON DELETE SET NULL
);