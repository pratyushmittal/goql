/*
Run SQL format queries against a CSV

Go has a powerful GoRoutines. This is an
experiment to see if the concurrent processing
of each individual row gives back faster
results.
*/

package main

import (
    "fmt"
    "strings"
    "strconv"
    "os"
    "time"
    "io"
    "regexp"
    "encoding/csv"
)

type Row []string
type Head []string

type DB struct {
    columns Head
    rows []Row
}


type Equation struct {
    lhs string
    rhs string
    operator string
}


func csv2db(filepath string, max_rows int) DB {
    // Read CSV file
    csvFile, err := os.Open(filepath)
    if err != nil { panic(err) }
    defer csvFile.Close()

    // For each line convert to struct
    csv := csv.NewReader(csvFile)
    columns, err := csv.Read()
    if err != nil { panic(err) }
    rows := []Row{}
    for {
        line, err := csv.Read()
        if err == io.EOF || (max_rows == len(rows) && max_rows != -1) {
            break
        }
        rows = append(rows, line)
    }
    return DB{columns, rows}
}


func strToEq(query string) Equation {
    reOperator := regexp.MustCompile("((>|<)=?|[*/+=-])")
    operators := reOperator.FindAllString(query, -1)
    if len(operators) > 1 {
        fmt.Println("Multiple conditions not supported")
        fmt.Println(operators)
    }
    operator := operators[0]
    parts := strings.Split(query, operator)
    lhs := strings.Trim(parts[0], " ")
    rhs := strings.Trim(parts[1], " ")
    return Equation{lhs, rhs, operator}
}


func indexOf(values []string, val string) int {
    for i, ival := range values {
        if ival == val {
            return i
        }
    }
    return -1
}


func (row Row) get(text string, head Head) float64 {
    number, err := strconv.ParseFloat(text, 32)
    if err == nil {
        return number
    }
    cellPos := indexOf(head, text)
    if cellPos == -1 {
        panic("Column not found:" + text)
    }
    val, err := strconv.ParseFloat(row[cellPos], 32)
    return val
}

func evaluate(row Row, query Equation, head Head, last bool, filtered chan Row) {
    lhs := row.get(query.lhs, head)
    rhs := row.get(query.rhs, head)
    satisfies := false
    switch query.operator{
    case ">":
        if lhs > rhs {satisfies = true}
    case ">=":
        if lhs >= rhs {satisfies = true}
    case "<":
        if lhs < rhs {satisfies = true}
    case "<=":
        if lhs <= rhs {satisfies = true}
    case "=":
        if lhs == rhs {satisfies = true}
    }
    if satisfies == true {
        filtered <- row
    }
    if last {
        close(filtered)
    }
}


func filter(db DB, query string) []Row{
    // Convert string query to query struct
    eq := strToEq(query)

    // Loop each row to see if eq satisfied
    filtered := []Row{}
    for _, row:= range db.rows {
        func(row Row) {
            lhs := row.get(eq.lhs, db.columns)
            rhs := row.get(eq.rhs, db.columns)
            satisfies := false
            switch eq.operator{
            case ">":
                if lhs > rhs {satisfies = true}
            case ">=":
                if lhs >= rhs {satisfies = true}
            case "<":
                if lhs < rhs {satisfies = true}
            case "<=":
                if lhs <= rhs {satisfies = true}
            case "=":
                if lhs == rhs {satisfies = true}
            }
            if satisfies == true {
                filtered = append(filtered, row)
            }
        }(row)
    }
    return filtered
}


func main() {
    // convert csv to goql-db
    db := csv2db("sample.csv", -1)
    fmt.Println(len(db.rows))

    //Begin time profiling
    start := time.Now()

    // Run Query
    query := "Height <= Age"
    fmt.Println(query)

    filtered := filter(db, query)
    fmt.Println(len(filtered))
    fmt.Println(time.Since(start))

    /*
    Try 1:
    Time taken on MySql without cache .655 seconds
    Time on Go: .998 seconds (without go routines)
    Time on Pandas: .045 seconds
    Pandas fucking rocks man

    Try 2:
    change 1000 to 100000
    db := csv2db("sample.csv", 1000)
    Can't get go routines to work yet.
    Python is very Pythonic in too many ways. Example
    support for default arguments. In Go, the default
    function arguments def abc(k=1) is not supported.
    */
}
