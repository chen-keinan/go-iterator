package internal

import (
	"database/sql"
	"errors"
	"github.com/jmoiron/sqlx"
	"net/http"
	"reflect"
)

type Iterator interface {
	Read(rows sql.Rows, slice interface{})
	Stream(w http.ResponseWriter)
}

type SqlIterator struct {
	rows   *sqlx.Rows
	result interface{}
}

func (sit *SqlIterator) Read(rows *sqlx.Rows, result interface{}) {
	sit.rows = rows
	sit.result = result
}

func (sit *SqlIterator) Stream(w http.ResponseWriter) error {

	resultv := reflect.ValueOf(sit.result)
	if resultv.Kind() != reflect.Ptr || resultv.Elem().Kind() != reflect.Slice {
		panic("result argument must be a slice address")
	}
	slicev := resultv.Elem()
	sliceType := slicev.Type() //
	elemt := sliceType.Elem()
	prototype := elemt
	isPointer := false
	if elemt.Kind() == reflect.Ptr {
		prototype = elemt.Elem()
		isPointer = true
	}

	slicev = slicev.Slice(0, slicev.Cap())
	i := 0
	for sit.rows.Next() {
		newRecord := reflect.New(prototype).Interface().(interface{})
		if err := sit.rows.StructScan(newRecord); err != nil {
			return errors.New("error during scanning query results")
		}
		finalValue := reflect.ValueOf(newRecord)
		if !isPointer {
			finalValue = finalValue.Elem()
		}

		if slicev.Len() == i {
			slicev = reflect.Append(slicev, finalValue)
			slicev = slicev.Slice(0, slicev.Cap())
		} else {
			slicev.Index(i).Set(finalValue)
		}
		i++
	}

	if err := sit.rows.Err(); err != nil {
		return errors.New("Error during processing query results")
	}
	resultv.Elem().Set(slicev.Slice(0, i))
	return nil
}
