package database

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"

	"github.com/BushSchoolIT/extractor/blackbaud"
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
	Name     string `json:"database"`
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

func (db *State) InsertEmails(t blackbaud.UnorderedTable) error {
	tx, err := db.Conn.BeginTx(*db.Ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	primaryKeys := map[string]bool{
		"email": true,
	}

	// remove null primary keys
	removeNull(primaryKeys, t)
	query := fmt.Sprintf(`
	INSERT INTO parents (%s) VALUES (%s)
	ON CONFLICT (%s)
	DO UPDATE SET %s;`,
		strings.Join(t.Columns, ","),
		placeHolders(len(t.Columns)),
		strings.Join(slices.Collect(maps.Keys(primaryKeys)), ","),
		updateAssignments(t.Columns, primaryKeys),
	)
	slog.Info("Query", slog.String("query", query))
	for _, row := range t.Rows {
		slog.Info("Found row", slog.Any("row", row))
		_, err = tx.Exec(*db.Ctx, query, row...)
		if err != nil {
			tx.Rollback(*db.Ctx)
			return err
		}
	}
	return tx.Commit(*db.Ctx)
}

// Build SQL placeholders like $1, $2, ..., $N
func placeHolders(count int) string {
	placeholders := make([]string, count)
	for i := range count {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}
	return strings.Join(placeholders, ",")
}

func removeNull(keys map[string]bool, t blackbaud.UnorderedTable) {
	for i := len(t.Rows) - 1; i >= 0; i-- {
		row := t.Rows[i]
		for j, col := range row {
			if keys[t.Columns[j]] && col == nil {
				t.Rows = slices.Delete(t.Rows, i, i+1)
				break
			}
		}
	}
}

func updateAssignments(columns []string, conflicts map[string]bool) string {
	updateAssignments := ""
	for i, col := range columns {
		if conflicts[col] {
			continue // skip conflict key columns
		}
		if i != 0 {
			updateAssignments += ", "
		}
		updateAssignments += fmt.Sprintf("%s = EXCLUDED.%s", col, col)
	}
	return updateAssignments
}

func (db *State) TranscriptOps(t blackbaud.UnorderedTable, startYear int, endYear int) error {
	tx, err := db.Conn.BeginTx(*db.Ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	err = transcriptCleanup(db.Ctx, tx, endYear)
	if err != nil {
		tx.Rollback(*db.Ctx)
		return err
	}

	// upsert query: https://neon.com/postgresql/postgresql-tutorial/postgresql-upsert
	primaryKeys := map[string]bool{
		"student_user_id": true,
		"term_id":         true,
		"group_id":        true,
		"course_id":       true,
		"grade_id":        true,
	}
	// remove null primary keys
	removeNull(primaryKeys, t)
	query := fmt.Sprintf(`
	INSERT INTO transcripts (%s) VALUES (%s)
	ON CONFLICT (%s)
	DO UPDATE SET %s;`,
		strings.Join(t.Columns, ","),
		placeHolders(len(t.Columns)),
		strings.Join(slices.Collect(maps.Keys(primaryKeys)), ","),
		updateAssignments(t.Columns, primaryKeys),
	)

	for _, row := range t.Rows {
		_, err := tx.Exec(*db.Ctx, query, row...)
		if err != nil {
			tx.Rollback(*db.Ctx)
			return err
		}
	}
	err = fixNoYearlong(db.Ctx, tx)
	if err != nil {
		tx.Rollback(*db.Ctx)
		return err
	}
	err = fixNonstandardGrades(db.Ctx, tx)
	if err != nil {
		tx.Rollback(*db.Ctx)
		return err
	}
	err = fixFallYearlongs(db.Ctx, tx, startYear, endYear)
	if err != nil {
		tx.Rollback(*db.Ctx)
		return err
	}
	err = insertMissingTranscriptCategories(db.Ctx, tx)
	if err != nil {
		tx.Rollback(*db.Ctx)
		return err
	}
	return tx.Commit(*db.Ctx)
}

// transcript helpers
// func insertTranscriptInfo(ctx *context.Context, tx pgx.Tx, columns []string, values []any) error {
// 	query, err := rowsInsertHelper(columns, values, `
// INSERT INTO transcripts (%s) VALUES (%s)
// ON CONFLICT (student_user_id,term_id,group_id,course_id,grade_id)
// DO UPDATE SET
// (student_first,student_last,grad_year,course_title,course_code,group_description,term_name,grade_description,grade_mode,grade,score,transcript_category,school_year,address_1,address_2,address_3,address_city,address_state,address_zip) = (EXCLUDED.student_first,EXCLUDED.student_last,EXCLUDED.grad_year,EXCLUDED.course_title,EXCLUDED.course_code,EXCLUDED.group_description,EXCLUDED.term_name,EXCLUDED.grade_description,EXCLUDED.grade_mode,EXCLUDED.grade,EXCLUDED.score,EXCLUDED.transcript_category,EXCLUDED.school_year,EXCLUDED.address_1,EXCLUDED.address_2,EXCLUDED.address_3,EXCLUDED.address_city,EXCLUDED.address_state,EXCLUDED.address_zip);`)
// 	if err != nil {
// 		return err
// 	}
// 	_, err = tx.Exec(*ctx, query, values...)
// 	return err
// }

// func insertEnrollmentInfo(ctx *context.Context, tx pgx.Tx, columns []string, values []any) error {
// 	query, err := colInsertHelper(columns, values, `
// INSERT INTO enrollment (%s) VALUES %s
// ON CONFLICT (student_user_id)
// DO UPDATE SET
// (student_first,student_last,grad_year,enroll_date, graduated, enroll_grade, enroll_year) = (EXCLUDED.student_first,EXCLUDED.student_last,EXCLUDED.grad_year,EXCLUDED.enroll_date, EXCLUDED.graduated, EXCLUDED.enroll_grade, EXCLUDED.enroll_year);
// 	`)
// 	if err != nil {
// 		return err
// 	}
// 	_, err = tx.Exec(*ctx, query, values...)
// 	return err
// }

// transformation used in the transcript ETL, used for taking yearlong courses with only 1 grade and fixing them to have both grades and be graded for both semesters
func fixNoYearlong(ctx *context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(*ctx, `
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
func fixNonstandardGrades(ctx *context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(*ctx, `
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
func fixFallYearlongs(ctx *context.Context, tx pgx.Tx, startYear int, endYear int) error {
	yearStr := fmt.Sprintf("%d - %d", startYear, endYear)
	_, err := tx.Exec(*ctx, `
		UPDATE public.transcripts
        SET grade_description = 'current_fall_yl',
        grade_id = 666666
        WHERE (school_year = $1 AND
        grade_description = 'Fall Term Grades YL');`,
		yearStr,
	)
	return err
}

/*
This function tx *pgx.Tx, removes records with grade_id = 999999, 888888, 777777, 666666 and restores Fall YL grades.
This is done to prevent duplicates on a reimport because the grade_id is part of the primary key.
*/
func transcriptCleanup(ctx *context.Context, tx pgx.Tx, endYear int) error {
	// List of the the last 4 academic years
	yearList := []int{}
	for i := range 5 {
		yearList = append(yearList, endYear-i)
	}
	transcript_query := `
                DELETE FROM public.transcripts
                                     WHERE (grade_id = 888888
                                     OR grade_id = 777777
                                     OR grade_id = 666666
                                     OR grade_description = \'Senior Mid-Term Grades\')
                                     AND school_year = $1
	`
	for _, year := range yearList {
		_, err := tx.Exec(*ctx, transcript_query, fmt.Sprintf("%d - %d", year, year+1))
		if err != nil {
			return err
		}
	}

	deleteScheduledCourses := `
UPDATE public.transcripts
    SET grade_description = 'Fall Term Grades YL', grade_id = 2154180
    WHERE (school_year != $1 
    AND grade_id = 666666);
	`
	_, err := tx.Exec(*ctx, deleteScheduledCourses, endYear)
	if err != nil {
		return err
	}
	restoreFallYlQuery := `
DELETE FROM public.transcripts
    WHERE grade_id = 999999
	`
	_, err = tx.Exec(*ctx, restoreFallYlQuery)
	return err
}

/*
This function tx *pgx.Tx, will insert transcript categories where none exist.
This is always the case for grade_id = 999999 as they do not exist for scheduled courses.
The transcript categories are identified based on the course prefix, which is the first
word in the course code. The transcript category mappings are stored in the public.course_codes table, which needs to
be manually kept up to date until we can get a better solution.
*/
func insertMissingTranscriptCategories(ctx *context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(*ctx, `
        WITH ranked_prefixes AS (
            SELECT
                transcripts.course_code,
                course_codes.transcript_category,
                ROW_NUMBER() OVER (PARTITION BY transcripts.course_code ORDER BY LENGTH(course_codes.course_prefix) DESC) AS rn
            FROM
                public.transcripts
            JOIN
                public.course_codes
            ON
                transcripts.course_code::text LIKE course_codes.course_prefix || '%'
            WHERE
                transcripts.transcript_category = 'NaN'
        )
        UPDATE public.transcripts
        SET transcript_category = ranked_prefixes.transcript_category
        FROM ranked_prefixes
        WHERE public.transcripts.course_code = ranked_prefixes.course_code
        AND public.transcripts.transcript_category = 'NaN'
        AND ranked_prefixes.rn = 1;
	`)
	return err
}

// end transcript helpers
