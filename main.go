package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/lib/pq"
)

var apiToken = "RAHASIA_TOKEN_KAMU"

// Structs
type SensorData struct {
	ID             int     `json:"id"`
	DeviceUniqueID string  `json:"device_unique_id"`
	ParameterName  string  `json:"parameter_name"`
	Value          float64 `json:"value,string"`
	RecordedAt     string  `json:"recorded_at"`
}

type AggregatedData struct {
	DeviceUniqueID string  `json:"device_unique_id"`
	ParameterName  string  `json:"parameter_name"`
	Value          float64 `json:"value"`
	Mode           string  `json:"mode"`
	FromTime       string  `json:"from_time"`
	ToTime         string  `json:"to_time"`
}

type Response struct {
	Status    bool        `json:"status"`
	Filter    string      `json:"filter"`
	Mode      string      `json:"mode"`
	Timezone  string      `json:"timezone,omitempty"`
	DeviceID  string      `json:"device_id,omitempty"`
	Month     string      `json:"month,omitempty"`
	Year      string      `json:"year,omitempty"`
	TimeRange string      `json:"time_range,omitempty"`
	Value     string      `json:"value,omitempty"`
	Limit     int         `json:"limit,omitempty"`
	Total     int         `json:"total"`
	Data      interface{} `json:"data"`
	Message   string      `json:"message,omitempty"`
}

var db *sql.DB

// Database connection
func initDB() {
	connStr := "host=localhost port=5432 user=postgres password=example dbname=temins sslmode=disable"
	var err error
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err = db.Ping(); err != nil {
		log.Fatal("Failed to ping database:", err)
	}

	log.Println("✅ Database connected successfully")
}

// Get timezone offset in hours
func getTimezoneOffset(zonaWaktu string) (offset int, label string) {
	switch strings.ToUpper(zonaWaktu) {
	case "WITA":
		return 1, "WITA"
	case "WIT":
		return 2, "WIT"
	default:
		return 0, "WIB"
	}
}

// Build timezone query
func buildTimezoneQuery(offset int) string {
	if offset == 0 {
		return "recorded_at"
	}
	return fmt.Sprintf("(recorded_at + INTERVAL '%d hours')", offset)
}

// Parse month parameter
func parseMonth(bulan string) (month, year string) {
	parts := strings.Split(bulan, "-")

	switch len(parts) {
	case 3:
		return parts[0], parts[2]
	case 2:
		return parts[0], parts[1]
	default:
		return bulan, strconv.Itoa(time.Now().Year())
	}
}

// Main handler
func getSensorData(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}

	// Parse parameters
	q := r.URL.Query()
	deviceID := q.Get("device_id")
	jenis := q.Get("jenis")
	periode := q.Get("periode")
	if periode == "" {
		periode = "hari"
	}
	mode := q.Get("mode")
	if mode == "" {
		mode = "raw"
	}
	tahun := q.Get("tahun")
	if tahun == "" {
		tahun = strconv.Itoa(time.Now().Year())
	}
	bulan := q.Get("bulan")
	tanggal := q.Get("tanggal")
	valueMode := q.Get("value")
	zonaWaktu := q.Get("zonawaktu")
	
	// ⭐ FITUR BARU: LIMIT
	limitStr := q.Get("limit")
	limit := 0 // 0 = tidak ada limit
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
			limit = l
		}
	}

	// Validate device_id
	if deviceID == "" {
		respondError(w, "device_id wajib diisi", http.StatusBadRequest)
		return
	}

	// Get timezone configuration
	tzOffset, tzLabel := getTimezoneOffset(zonaWaktu)
	tzQuery := buildTimezoneQuery(tzOffset)

	// MULTI PARAMETER RANDOM SAMPLING
	if strings.Contains(jenis, ",") && bulan != "" {
		handleMultiParamRandom(w, deviceID, jenis, bulan, tzQuery, tzLabel, limit)
		return
	}

	// MODE: MULTI DEVICE LATEST
	if strings.Contains(deviceID, ",") && mode == "latest" {
		handleMultiDeviceLatest(w, deviceID, tzQuery, tzLabel)
		return
	}

	// MODE: SINGLE DEVICE LATEST
	if mode == "latest" {
		handleLatestMode(w, deviceID, tzQuery, tzLabel)
		return
	}

	// MODE: ALL PARAMETERS
	if jenis == "" && valueMode == "" && periode == "hari" {
		if bulan != "" {
			handleAllParametersByMonth(w, deviceID, bulan, tzQuery, tzLabel, limit)
		} else {
			handleAllParameters(w, deviceID, tzQuery, tzLabel, limit)
		}
		return
	}

	// MODE: NOW (Latest data)
	if periode == "now" {
		handleNowMode(w, deviceID, tzQuery, tzLabel)
		return
	}

	// Validate jenis parameter
	if jenis == "" {
		respondError(w, "parameter jenis diperlukan", http.StatusBadRequest)
		return
	}

	// ⭐ MODE: VALUE AGREGAT (high/low/avg) - HANYA 1 DATA
	if valueMode != "" {
		handleValueAggregateMode(w, deviceID, jenis, periode, valueMode, tahun, bulan, tanggal, tzQuery, tzLabel)
		return
	}

	// MODE: RINGKAS/MINGGU_INI/BULAN (tanpa value = agregasi per periode)
	if mode == "ringkas" || periode == "minggu_ini" || periode == "bulan" {
		handleAggregatedMode(w, deviceID, jenis, periode, mode, tahun, bulan, tzQuery, tzLabel, limit)
		return
	}

	// MODE: TANGGAL (by specific date)
	if tanggal != "" {
		handlePeriodeByDate(w, deviceID, jenis, tanggal, mode, tzQuery, tzLabel, limit)
		return
	}

	// MODE: PERIODE (raw or ringkas)
	handlePeriode(w, deviceID, jenis, periode, mode, tahun, bulan, tzQuery, tzLabel, limit)
}

// ⭐ HANDLER BARU: VALUE AGGREGATE - HANYA RETURN 1 DATA
func handleValueAggregateMode(w http.ResponseWriter, deviceID, jenis, periode, valueMode, tahun, bulan, tanggal, tzQuery, tzLabel string) {
	var aggFunc string
	switch valueMode {
	case "high":
		aggFunc = "MAX(value)"
	case "low":
		aggFunc = "MIN(value)"
	case "avg":
		aggFunc = "AVG(value)"
	default:
		respondError(w, "value hanya high | low | avg", http.StatusBadRequest)
		return
	}

	var query string
	var args []interface{}
	var filter string

	// Tentukan range waktu
	if tanggal != "" {
		// Tanggal spesifik
		query = fmt.Sprintf(`
			SELECT 
				MIN(id) AS id,
				device_unique_id,
				parameter_name,
				ROUND((%s)::numeric, 2) AS value,
				TO_CHAR(MIN(%s), 'YYYY-MM-DD HH24:MI:SS') AS recorded_at
			FROM sensor_logs
			WHERE device_unique_id = $1
			  AND parameter_name = $2
			  AND recorded_at >= $3::date
			  AND recorded_at < ($3::date + INTERVAL '1 day')
			GROUP BY device_unique_id, parameter_name
		`, aggFunc, tzQuery)
		args = []interface{}{deviceID, jenis, tanggal}
		filter = "tanggal_" + valueMode
	} else if bulan != "" {
		// Bulan spesifik
		month, year := parseMonth(bulan)
		query = fmt.Sprintf(`
			SELECT 
				MIN(id) AS id,
				device_unique_id,
				parameter_name,
				ROUND((%s)::numeric, 2) AS value,
				TO_CHAR(MIN(%s), 'YYYY-MM-DD HH24:MI:SS') AS recorded_at
			FROM sensor_logs
			WHERE device_unique_id = $1
			  AND parameter_name = $2
			  AND EXTRACT(YEAR FROM recorded_at) = $3
			  AND EXTRACT(MONTH FROM recorded_at) = $4
			GROUP BY device_unique_id, parameter_name
		`, aggFunc, tzQuery)
		args = []interface{}{deviceID, jenis, year, month}
		filter = "bulan_" + valueMode
	} else if periode == "minggu_ini" {
		// 7 hari terakhir
		query = fmt.Sprintf(`
			SELECT 
				MIN(id) AS id,
				device_unique_id,
				parameter_name,
				ROUND((%s)::numeric, 2) AS value,
				TO_CHAR(MIN(%s), 'YYYY-MM-DD HH24:MI:SS') AS recorded_at
			FROM sensor_logs
			WHERE device_unique_id = $1
			  AND parameter_name = $2
			  AND recorded_at >= CURRENT_DATE - INTERVAL '6 DAYS'
			GROUP BY device_unique_id, parameter_name
		`, aggFunc, tzQuery)
		args = []interface{}{deviceID, jenis}
		filter = "minggu_ini_" + valueMode
	} else {
		// Default: 24 jam terakhir
		query = fmt.Sprintf(`
			SELECT 
				MIN(id) AS id,
				device_unique_id,
				parameter_name,
				ROUND((%s)::numeric, 2) AS value,
				TO_CHAR(MIN(%s), 'YYYY-MM-DD HH24:MI:SS') AS recorded_at
			FROM sensor_logs
			WHERE device_unique_id = $1
			  AND parameter_name = $2
			  AND recorded_at >= NOW() - INTERVAL '24 HOURS'
			GROUP BY device_unique_id, parameter_name
		`, aggFunc, tzQuery)
		args = []interface{}{deviceID, jenis}
		filter = "hari_" + valueMode
	}

	var s SensorData
	err := db.QueryRow(query, args...).Scan(
		&s.ID,
		&s.DeviceUniqueID,
		&s.ParameterName,
		&s.Value,
		&s.RecordedAt,
	)

	if err == sql.ErrNoRows {
		respond(w, Response{
			Status:   false,
			Filter:   filter,
			Mode:     "aggregate",
			Timezone: tzLabel,
			DeviceID: deviceID,
			Value:    valueMode,
			Total:    0,
			Data:     nil,
			Message:  "Tidak ada data ditemukan",
		})
		return
	}

	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	respond(w, Response{
		Status:   true,
		Filter:   filter,
		Mode:     "aggregate",
		Timezone: tzLabel,
		DeviceID: deviceID,
		Value:    valueMode,
		Total:    1,
		Data:     s,
	})
}

// Handler: Multi Param Random dengan LIMIT
func handleMultiParamRandom(w http.ResponseWriter, deviceID, jenis, bulan, tzQuery, tzLabel string, limit int) {
	month, year := parseMonth(bulan)
	y, _ := strconv.Atoi(year)
	m, _ := strconv.Atoi(month)

	fromTime := time.Date(y, time.Month(m), 1, 0, 0, 0, 0, time.Local)
	toTime := fromTime.AddDate(0, 1, 0)

	paramList := strings.Split(jenis, ",")

	// Default limit jika tidak diset
	perParamLimit := 100
	if limit > 0 {
		perParamLimit = limit
	}

	query := fmt.Sprintf(`
		SELECT
			s.id,
			s.device_unique_id,
			s.parameter_name,
			s.value,
			TO_CHAR(%s, 'YYYY-MM-DD HH24:MI:SS') AS recorded_at
		FROM unnest($2::text[]) p(parameter_name)
		JOIN LATERAL (
			SELECT id, device_unique_id, parameter_name, value, recorded_at
			FROM sensor_logs
			WHERE device_unique_id = $1
			  AND parameter_name = p.parameter_name
			  AND recorded_at >= $3
			  AND recorded_at <  $4
			ORDER BY recorded_at DESC
			LIMIT $5
		) s ON true
		ORDER BY s.parameter_name, s.recorded_at
	`, tzQuery)

	rows, err := db.Query(query, deviceID, pq.Array(paramList), fromTime, toTime, perParamLimit)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	data := []SensorData{}
	for rows.Next() {
		var s SensorData
		if err := rows.Scan(&s.ID, &s.DeviceUniqueID, &s.ParameterName, &s.Value, &s.RecordedAt); err != nil {
			continue
		}
		data = append(data, s)
	}

	resp := Response{
		Status:   true,
		Filter:   "multi_param_random",
		Mode:     "random_sample",
		Timezone: tzLabel,
		DeviceID: deviceID,
		Month:    month,
		Year:     year,
		Total:    len(data),
		Data:     data,
	}
	if limit > 0 {
		resp.Limit = limit
	}

	respond(w, resp)
}

// Handler: Latest mode
func handleLatestMode(w http.ResponseWriter, deviceID, tzQuery, tzLabel string) {
	query := fmt.Sprintf(`
		SELECT id, device_unique_id, parameter_name, value,
		       TO_CHAR(%s, 'YYYY-MM-DD HH24:MI:SS') AS recorded_at
		FROM sensor_logs
		WHERE device_unique_id = $1
		ORDER BY recorded_at DESC
		LIMIT 1
	`, tzQuery)

	var s SensorData
	err := db.QueryRow(query, deviceID).Scan(&s.ID, &s.DeviceUniqueID, &s.ParameterName, &s.Value, &s.RecordedAt)

	if err == sql.ErrNoRows {
		respond(w, Response{
			Status:   false,
			Filter:   "latest",
			Mode:     "latest",
			Timezone: tzLabel,
			DeviceID: deviceID,
			Total:    0,
			Data:     []SensorData{},
			Message:  "Tidak ada data ditemukan",
		})
		return
	}

	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	respond(w, Response{
		Status:   true,
		Filter:   "latest",
		Mode:     "latest",
		Timezone: tzLabel,
		DeviceID: deviceID,
		Total:    1,
		Data:     s,
	})
}

// Handler: All parameters dengan LIMIT
func handleAllParameters(w http.ResponseWriter, deviceID, tzQuery, tzLabel string, limit int) {
	defaultLimit := 20000
	if limit > 0 {
		defaultLimit = limit
	}

	query := fmt.Sprintf(`
		SELECT id, device_unique_id, parameter_name, value, 
		       TO_CHAR(%s, 'YYYY-MM-DD HH24:MI:SS') AS recorded_at
		FROM sensor_logs
		WHERE device_unique_id = $1
		  AND recorded_at >= NOW() - INTERVAL '24 HOURS'
		ORDER BY recorded_at DESC
		LIMIT $2
	`, tzQuery)

	rows, err := db.Query(query, deviceID, defaultLimit)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	data := []SensorData{}
	for rows.Next() {
		var s SensorData
		if err := rows.Scan(&s.ID, &s.DeviceUniqueID, &s.ParameterName, &s.Value, &s.RecordedAt); err != nil {
			continue
		}
		data = append(data, s)
	}

	resp := Response{
		Status:    true,
		Filter:    "all_parameters",
		Mode:      "raw",
		Timezone:  tzLabel,
		DeviceID:  deviceID,
		TimeRange: "24_hours",
		Total:     len(data),
		Data:      data,
	}
	if limit > 0 {
		resp.Limit = limit
	}

	respond(w, resp)
}

// Handler: All parameters by month dengan LIMIT
func handleAllParametersByMonth(w http.ResponseWriter, deviceID, bulan, tzQuery, tzLabel string, limit int) {
	month, year := parseMonth(bulan)

	// Default tidak ada limit untuk mode ini, kecuali diminta
	limitClause := ""
	args := []interface{}{deviceID, year, month}
	
	if limit > 0 {
		limitClause = "LIMIT $4"
		args = append(args, limit)
	}

	query := fmt.Sprintf(`
		SELECT id, device_unique_id, parameter_name, value, 
		       TO_CHAR(%s, 'YYYY-MM-DD HH24:MI:SS') AS recorded_at
		FROM sensor_logs
		WHERE device_unique_id = $1
		  AND EXTRACT(YEAR FROM recorded_at) = $2
		  AND EXTRACT(MONTH FROM recorded_at) = $3
		ORDER BY recorded_at DESC, parameter_name ASC
		%s
	`, tzQuery, limitClause)

	rows, err := db.Query(query, args...)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	data := []SensorData{}
	for rows.Next() {
		var s SensorData
		if err := rows.Scan(&s.ID, &s.DeviceUniqueID, &s.ParameterName, &s.Value, &s.RecordedAt); err != nil {
			continue
		}
		data = append(data, s)
	}

	resp := Response{
		Status:   true,
		Filter:   "all_parameters_by_month",
		Mode:     "raw",
		Timezone: tzLabel,
		DeviceID: deviceID,
		Month:    month,
		Year:     year,
		Total:    len(data),
		Data:     data,
	}
	if limit > 0 {
		resp.Limit = limit
	}

	respond(w, resp)
}

// Handler: NOW mode
func handleNowMode(w http.ResponseWriter, deviceID, tzQuery, tzLabel string) {
	query := fmt.Sprintf(`
		SELECT DISTINCT ON (parameter_name)
		       id, device_unique_id, parameter_name, value,
		       TO_CHAR(%s, 'YYYY-MM-DD HH24:MI:SS') AS recorded_at
		FROM sensor_logs
		WHERE device_unique_id = $1
		ORDER BY parameter_name ASC, recorded_at DESC
	`, tzQuery)

	rows, err := db.Query(query, deviceID)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	data := []SensorData{}
	for rows.Next() {
		var s SensorData
		if err := rows.Scan(&s.ID, &s.DeviceUniqueID, &s.ParameterName, &s.Value, &s.RecordedAt); err != nil {
			continue
		}
		data = append(data, s)
	}

	respond(w, Response{
		Status:   true,
		Filter:   "now",
		Mode:     "latest",
		Timezone: tzLabel,
		Total:    len(data),
		Data:     data,
	})
}

// Handler: Aggregated mode (ringkas tanpa value) dengan LIMIT
func handleAggregatedMode(w http.ResponseWriter, deviceID, jenis, periode, mode, tahun, bulan, tzQuery, tzLabel string, limit int) {
	var query string
	var args []interface{}
	var filter string

	switch periode {
	case "hari":
		query = fmt.Sprintf(`
			SELECT MIN(id) AS id, device_unique_id, parameter_name,
			       ROUND(AVG(value)::numeric, 2) AS value,
			       TO_CHAR(DATE_TRUNC('hour', %s), 'YYYY-MM-DD HH24:MI:SS') AS recorded_at
			FROM sensor_logs
			WHERE device_unique_id = $1
			  AND parameter_name = $2
			  AND recorded_at >= NOW() - INTERVAL '24 HOURS'
			GROUP BY device_unique_id, parameter_name, DATE_TRUNC('hour', %s)
			ORDER BY DATE_TRUNC('hour', %s) DESC
		`, tzQuery, tzQuery, tzQuery)
		args = []interface{}{deviceID, jenis}
		filter = "hari"

	case "minggu_ini":
		query = fmt.Sprintf(`
			SELECT MIN(id) AS id, device_unique_id, parameter_name,
			       ROUND(AVG(value)::numeric, 2) AS value,
			       TO_CHAR(DATE(%s), 'YYYY-MM-DD') AS recorded_at
			FROM sensor_logs
			WHERE device_unique_id = $1
			  AND parameter_name = $2
			  AND recorded_at >= CURRENT_DATE - INTERVAL '6 DAYS'
			GROUP BY device_unique_id, parameter_name, DATE(%s)
			ORDER BY DATE(%s) DESC
		`, tzQuery, tzQuery, tzQuery)
		args = []interface{}{deviceID, jenis}
		filter = "minggu_ini"

	case "bulan":
		if bulan != "" {
			month, year := parseMonth(bulan)
			query = fmt.Sprintf(`
				SELECT MIN(id) AS id, device_unique_id, parameter_name,
				       ROUND(AVG(value)::numeric, 2) AS value,
				       TO_CHAR(DATE(%s), 'YYYY-MM-DD') AS recorded_at
				FROM sensor_logs
				WHERE device_unique_id = $1
				  AND parameter_name = $2
				  AND EXTRACT(YEAR FROM recorded_at) = $3
				  AND EXTRACT(MONTH FROM recorded_at) = $4
				GROUP BY device_unique_id, parameter_name, DATE(%s)
				ORDER BY DATE(%s) DESC
			`, tzQuery, tzQuery, tzQuery)
			args = []interface{}{deviceID, jenis, year, month}
		} else {
			query = fmt.Sprintf(`
				SELECT MIN(id) AS id, device_unique_id, parameter_name,
				       ROUND(AVG(value)::numeric, 2) AS value,
				       TO_CHAR(DATE(%s), 'YYYY-MM-DD') AS recorded_at
				FROM sensor_logs
				WHERE device_unique_id = $1
				  AND parameter_name = $2
				  AND recorded_at >= CURRENT_DATE - INTERVAL '29 DAYS'
				GROUP BY device_unique_id, parameter_name, DATE(%s)
				ORDER BY DATE(%s) DESC
			`, tzQuery, tzQuery, tzQuery)
			args = []interface{}{deviceID, jenis}
		}
		filter = "bulan"

	default:
		respondError(w, "periode tidak valid", http.StatusBadRequest)
		return
	}

	// Tambahkan LIMIT jika ada
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	rows, err := db.Query(query, args...)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	data := []SensorData{}
	for rows.Next() {
		var s SensorData
		if err := rows.Scan(&s.ID, &s.DeviceUniqueID, &s.ParameterName, &s.Value, &s.RecordedAt); err != nil {
			continue
		}
		data = append(data, s)
	}

	resp := Response{
		Status:   true,
		Filter:   filter,
		Mode:     mode,
		Timezone: tzLabel,
		DeviceID: deviceID,
		Total:    len(data),
		Data:     data,
	}
	if limit > 0 {
		resp.Limit = limit
	}

	respond(w, resp)
}

// Handler: Periode by date dengan LIMIT
func handlePeriodeByDate(w http.ResponseWriter, deviceID, jenis, tanggal, mode, tzQuery, tzLabel string, limit int) {
	var query string

	if mode == "ringkas" {
		query = fmt.Sprintf(`
			SELECT MIN(id) AS id, device_unique_id, parameter_name,
			       ROUND(AVG(value)::numeric, 2) AS value,
			       TO_CHAR(DATE_TRUNC('hour', %s), 'YYYY-MM-DD HH24:MI:SS') AS recorded_at
			FROM sensor_logs
			WHERE device_unique_id = $1
			  AND parameter_name = $2
			  AND recorded_at >= $3::date
			  AND recorded_at < ($3::date + INTERVAL '1 day')
			GROUP BY device_unique_id, parameter_name, DATE_TRUNC('hour', %s)
			ORDER BY DATE_TRUNC('hour', %s) DESC
		`, tzQuery, tzQuery, tzQuery)
	} else {
		query = fmt.Sprintf(`
			SELECT id, device_unique_id, parameter_name, value,
			       TO_CHAR(%s, 'YYYY-MM-DD HH24:MI:SS') AS recorded_at
			FROM sensor_logs
			WHERE device_unique_id = $1
			  AND parameter_name = $2
			  AND recorded_at >= $3::date
			  AND recorded_at < ($3::date + INTERVAL '1 day')
			ORDER BY recorded_at DESC
		`, tzQuery)
	}

	// Tambahkan LIMIT
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	rows, err := db.Query(query, deviceID, jenis, tanggal)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	data := []SensorData{}
	for rows.Next() {
		var s SensorData
		if err := rows.Scan(&s.ID, &s.DeviceUniqueID, &s.ParameterName, &s.Value, &s.RecordedAt); err != nil {
			continue
		}
		data = append(data, s)
	}

	resp := Response{
		Status:   true,
		Filter:   "tanggal",
		Mode:     mode,
		Timezone: tzLabel,
		Total:    len(data),
		Data:     data,
	}
	if limit > 0 {
		resp.Limit = limit
	}

	respond(w, resp)
}

// Handler: Periode dengan LIMIT
func handlePeriode(w http.ResponseWriter, deviceID, jenis, periode, mode, tahun, bulan, tzQuery, tzLabel string, limit int) {
	var query string
	var args []interface{}

	switch periode {
	case "hari":
		if mode == "ringkas" {
			query = fmt.Sprintf(`
				SELECT MIN(id) AS id, device_unique_id, parameter_name,
				       ROUND(AVG(value)::numeric, 2) AS value,
				       TO_CHAR(DATE_TRUNC('hour', %s), 'YYYY-MM-DD HH24:MI:SS') AS recorded_at
				FROM sensor_logs
				WHERE device_unique_id = $1
				  AND parameter_name = $2
				  AND recorded_at >= NOW() - INTERVAL '24 HOURS'
				GROUP BY device_unique_id, parameter_name, DATE_TRUNC('hour', %s)
				ORDER BY DATE_TRUNC('hour', %s) DESC
			`, tzQuery, tzQuery, tzQuery)
		} else {
			query = fmt.Sprintf(`
				SELECT id, device_unique_id, parameter_name, value,
				       TO_CHAR(%s, 'YYYY-MM-DD HH24:MI:SS') AS recorded_at
				FROM sensor_logs
				WHERE device_unique_id = $1
				  AND parameter_name = $2
				  AND recorded_at >= NOW() - INTERVAL '24 HOURS'
				ORDER BY