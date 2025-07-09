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
