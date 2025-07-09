package database

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

type State struct {
	Ctx  *context.Context
	Conn *pgx.Conn
}

type Config struct {
	User     string `json:"user"`
	Port     string `json:"port"`
	Password string `json:"password"`
	Addr     string `json:"address"`
	Name     string `json:"name"`
}

func Connect(c Config) (State, error) {
	connUrl := fmt.Sprintf("postgres://%s:%s@%s:%s/%s", c.User, c.Password, c.Addr, c.Port, c.Name)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, connUrl)
	if err != nil {
		return State{}, err
	}
	return State{
		Ctx:  &ctx,
		Conn: conn,
	}, nil
}

func (db *State) QueryGrades(grades []int32) (pgx.Rows, error) {
	rows, err := db.Conn.Query(*db.Ctx,
		`SELECT email, first_name, last_name FROM parents WHERE grade && $1`, grades)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (db *State) Close() error {
	return db.Conn.Close(*db.Ctx)
}

// TODO make the other DB transformations/cleanups
func (db *State) InsertTranscriptInfo(columns []string, values []any) error {
	if len(columns) != len(values) {
		return fmt.Errorf("columns and values length mismatch")
	}
	columnsStr := strings.Join(columns, ",")

	// Build placeholders like $1, $2, ..., $N
	placeholders := make([]string, len(values))
	for i := range values {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}
	placeholdersStr := strings.Join(placeholders, ",")

	query := fmt.Sprintf(`INSERT INTO transcripts (%s) VALUES (%s)
				ON CONFLICT (student_user_id,term_id,group_id,course_id,grade_id) 
				DO UPDATE SET 
				(student_first,student_last,grad_year,course_title,course_code,group_description,term_name,grade_description,grade_mode,grade,score,transcript_category,school_year,address_1,address_2,address_3,address_city,address_state,address_zip) = (EXCLUDED.student_first,EXCLUDED.student_last,EXCLUDED.grad_year,EXCLUDED.course_title,EXCLUDED.course_code,EXCLUDED.group_description,EXCLUDED.term_name,EXCLUDED.grade_description,EXCLUDED.grade_mode,EXCLUDED.grade,EXCLUDED.score,EXCLUDED.transcript_category,EXCLUDED.school_year,EXCLUDED.address_1,EXCLUDED.address_2,EXCLUDED.address_3,EXCLUDED.address_city,EXCLUDED.address_state,EXCLUDED.address_zip);`, columnsStr, placeholdersStr)
	_, err := db.Conn.Exec(*db.Ctx, query, values...)
	return err
}

// transformation used in the transcript ETL, used for taking yearlong courses with only 1 grade and fixing them to have both grades and be graded for both semesters
func (db *State) FixNoYearlong() error {
	_, err := db.Conn.Exec(*db.Ctx, `
		WITH potential_updates AS (
			SELECT student_user_id, school_year, course_id
			FROM public.transcripts
			WHERE grade_description IN ('Fall Term Grades YL', 'Spring Term Grades YL', 'Year-Long Grades')
				OR grade_id = 999999
			GROUP BY student_user_id, school_year, course_id
			HAVING COUNT(*) = 1
		)
		UPDATE public.transcripts t
		SET grade_description = 'no_yearlong_possible',
			grade_id = 888888
		FROM potential_updates p
		WHERE t.student_user_id = p.student_user_id
			AND t.school_year = p.school_year
			AND t.course_id = p.course_id
			AND t.grade_id != 999999
			AND t.grade_description != 'Year-Long Grades';
		`)
	return err
}

// fixes classes that use credit no credit or "audit", or with specific failing grades
func (db *State) FixNonstandardGrades() error {
	_, err := db.Conn.Exec(*db.Ctx, `
WITH transcript_grades AS (
    SELECT 
        t.*,
        CASE 
            WHEN grade IN ('NC', 'CR', 'I', 'WF', 'WP', 'AU') THEN 'non_letter'
            ELSE 'letter'
        END AS grade_type
    FROM public.transcripts t
    WHERE grade_description IN ('Fall Term Grades YL', 'Spring Term Grades YL', 'Year-Long Grades')
),
paired_grades AS (
    SELECT 
        student_user_id,
        school_year,
        course_id,
        COUNT(*) AS count,
        COUNT(DISTINCT grade_type) AS type_count
    FROM transcript_grades
    GROUP BY student_user_id, school_year, course_id
    HAVING COUNT(*) = 2 AND COUNT(DISTINCT grade_type) = 2
)
UPDATE public.transcripts t
SET grade_description = 'no_yearlong_possible',
    grade_id = 777777
FROM paired_grades p
WHERE t.student_user_id = p.student_user_id
  AND t.school_year = p.school_year
  AND t.course_id = p.course_id
  AND t.grade_description IN ('Fall Term Grades YL', 'Spring Term Grades YL', 'Year-Long Grades');
	`)
	return err
}

/*
Reassigns grade_id for Fall YL grades when the year is the current year. We need to do this to
allow them to show up in powerBI. Typically Fll YL grades are filtered out because they are overwritten
by YL grades.
*/
func (db *State) FixFallYearlongs(year int) error {
	yearStr := fmt.Sprintf("%d", year)
	_, err := db.Conn.Exec(*db.Ctx, `UPDATE public.transcripts
                        SET grade_description = 'current_fall_yl',
                                grade_id = 666666
                        WHERE (school_year = $1 AND
                                grade_description = 'Fall Term Grades YL');`, yearStr)
	return err
}
