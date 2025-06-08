from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import duckdb
import os

app = FastAPI()

origins = [
    "http://localhost:5173",
    "http://127.0.0.1:5173"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_PATH = "/data/exploitation/databases/exploitation.db"

@app.get('/favicon.ico')
def favicon():
    return '', 204


@app.get('/api/gpa_per_dept')
def get_gpa_per_dept():
    try:
        with duckdb.connect(DB_PATH) as con:
            df = con.execute('''
                SELECT major, department, rate FROM dept_gpa
            ''').df()
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=e)

@app.get('/api/average_gpa')
def average_gpa():
    try:
        with duckdb.connect(DB_PATH) as con:
            df = con.execute('''
                SELECT major, department, rate FROM dept_gpa
            ''').df()
        return df.to_dict(orient="records")
    except Exception as e:
        return HTTPException(status_code=500, detail=e)

@app.get('/api/total_student')
def get_total_student():
    try:
        with duckdb.connect(DB_PATH) as con:
            df = con.execute('''
                SELECT count AS total, state, percent_change AS percent FROM total_student_enroll WHERE YEAR=2024
            ''').df()
        return df.to_dict(orient="records")
    except Exception as e:
        return HTTPException(status_code=500, detail=e)


@app.get('/api/current_graduation_rate')
def current_graduation_rate():
    try:
        with duckdb.connect(DB_PATH) as con:
            df = con.execute('''
	SELECT 
	    curr.rate,
	    ROUND(ABS((curr.rate - prev.rate) / prev.rate) * 100, 2) AS percent,
	    CASE 
		WHEN curr.rate > prev.rate THEN 'increase'
		WHEN curr.rate < prev.rate THEN 'decrease'
		ELSE 'no change'
	    END AS state
	FROM 
	    retention_hist_all_year curr
	JOIN 
	    retention_hist_all_year prev ON curr.year = EXTRACT(YEAR FROM CURRENT_DATE) - 1 AND prev.year = EXTRACT(YEAR FROM CURRENT_DATE) - 2;

            ''').df()
        return df.to_dict(orient="records")
    except Exception as e:
        return HTTPException(status_code=500, detail=e)
        
@app.get('/api/meeting_requests_semester')
def meeting_requests_semester():
    try:
        with duckdb.connect(DB_PATH) as con:
            df = con.execute('''
	SELECT meeting_count,  change_status, abs(percent_change) as percent
	FROM meeting_requests_semester
	WHERE 
	    year = EXTRACT(YEAR FROM CURRENT_DATE)
	    AND semester = CASE 
		WHEN EXTRACT(MONTH FROM CURRENT_DATE) BETWEEN 1 AND 6 THEN 1
		ELSE 2
	    END;

            ''').df()
        return df.to_dict(orient="records")
    except Exception as e:
        return HTTPException(status_code=500, detail=e)
        

@app.get('/api/meeting_reports_semester')
def meeting_reports_semester():
    try:
        with duckdb.connect(DB_PATH) as con:
            df = con.execute('''
	SELECT meeting_count,  change_status, abs(percent_change) as percent
	FROM meeting_reports_semester
	WHERE 
	    year = EXTRACT(YEAR FROM CURRENT_DATE)
	    AND semester = CASE 
		WHEN EXTRACT(MONTH FROM CURRENT_DATE) BETWEEN 1 AND 6 THEN 1
		ELSE 2
	    END;

            ''').df()
        return df.to_dict(orient="records")
    except Exception as e:
        return HTTPException(status_code=500, detail=e)

        
@app.get('/api/retention_rate_past_7')
def retention_rate_past():
    try:
        with duckdb.connect(DB_PATH) as con:
            df = con.execute('''
                SELECT cohort_year, dropout_year, retention_percent
FROM retention_rates
            ''').df()
        return df.to_dict(orient="records")
    except Exception as e:
        return HTTPException(status_code=500, detail=e)
        

@app.get('/api/graduation_rate_past_7')
def graduation_rate_past():
    try:
        with duckdb.connect(DB_PATH) as con:
            df = con.execute('''
                SELECT cohort_year AS YEAR, ROUND(grad_4yr_rate,2) AS graduate_after_4_year, ROUND(grad_5yr_rate,2) AS graduate_after_5_year,
ROUND(grad_6yr_rate,2) AS graduate_after_6_year
FROM graduation_rates WHERE cohort_year < EXTRACT(YEAR FROM CURRENT_DATE) - 4
            ''').df()
        return df.to_dict(orient="records")
    except Exception as e:
        return HTTPException(status_code=500, detail=e)
       

@app.get('/api/retention_hist')
def get_retention():
    try:
        with duckdb.connect(DB_PATH) as con:
            df = con.execute('''
                SELECT YEAR, rate FROM retention_hist_all_year WHERE year BETWEEN 2020 	AND 2024
            ''').df()
        return df.to_dict(orient="records")
    except Exception as e:
        return HTTPException(status_code=500, detail=e)

@app.get('/api/graduation_hist')
def get_graduation_hist():
    try:
        with duckdb.connect(DB_PATH) as con:
            df = con.execute('''
               SELECT YEAR,rate FROM graduation_hist_all_year WHERE YEAR BETWEEN 2020 AND 2024
            ''').df()
        return df.to_dict(orient="records")
    except Exception as e:
        return HTTPException(status_code=500, detail=e)
        
        
@app.get('/api/nearest_meeting_reports')
def nearest_meeting_reports():
    try:
        with duckdb.connect(DB_PATH) as con:
            df = con.execute('''
               SELECT * FROM monthly_meeting_reports
        ORDER BY "year" DESC, "month" DESC
        LIMIT 6
            ''').df()
        return df.to_dict(orient="records")
    except Exception as e:
        return HTTPException(status_code=500, detail=e)