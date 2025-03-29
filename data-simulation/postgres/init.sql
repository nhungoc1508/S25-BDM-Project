CREATE TABLE departments (
    dept_code VARCHAR(10) PRIMARY KEY,
    division VARCHAR(50) NOT NULL,
    name VARCHAR(100) NOT NULL,
    office_number VARCHAR(20),
    building_code VARCHAR(10),
    blurb TEXT
);

CREATE TABLE students (
    student_id VARCHAR(20) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    phone VARCHAR(20),
    address TEXT,
    gender VARCHAR(10) CHECK (gender IN ('male', 'female', 'other')),
    email VARCHAR(255) UNIQUE NOT NULL,
    year INT CHECK (year >= 1 AND year <= 4),
    department VARCHAR(10) NOT NULL,
    FOREIGN KEY (department) REFERENCES departments(dept_code) ON DELETE SET NULL
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