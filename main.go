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
	Total     int         `json:"total"`
	Data      interface{} `json:"data"`
	Message   string      `json:"message,omitempty"`
}

var db *sql.DB

// Database connection
func initDB() {
connStr := "host=localhost port=5432 user=postgres password=example dbname=temins sslmode=disable application_name=api-data"

	var err error
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}

	// Connection pool settings
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
		return 1, "WITA" // +1 jam dari WIB
	case "WIT":
		return 2, "WIT" // +2 jam dari WIB
	default: // WIB or empty
		return 0, "WIB" // Tidak ada offset (data sudah WIB)
	}
}

// Build timezone query - karena data sudah WIB, tinggal tambah/kurang offset
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
	case 3: // MM-DD-YYYY
		return parts[0], parts[2]
	case 2: // MM-YYYY
		return parts[0], parts[1]
	default: // MM only
		return bulan, strconv.Itoa(time.Now().Year())
	}
}

// Helper untuk menambahkan LIMIT ke query
func appendLimit(query string, limit int) string {
	if limit > 0 {
		return fmt.Sprintf("%s LIMIT %d", query, limit)
	}
	return query
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
	
	// PARSE LIMIT
	limitStr := q.Get("limit")
	limit := 0
	if limitStr != "" {
		l, err := strconv.Atoi(limitStr)
		if err == nil && l > 0 {
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

	// =========================================================================
	// FITUR: MULTI PARAMETER RANDOM SAMPLING
	// =========================================================================
	if strings.Contains(jenis, ",") && bulan != "" {
		handleMultiParamRandom(w, deviceID, jenis, bulan, tzQuery, tzLabel)
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
			handleAllParametersByMonth(w, deviceID, bulan, tzQuery, tzLabel)
		} else {
			handleAllParameters(w, deviceID, limit, tzQuery, tzLabel)
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

	// MODE: RINGKAS/MINGGU_INI/BULAN WITH VALUE (high/low/avg)
	if valueMode != "" && (mode == "ringkas" || periode == "minggu_ini" || periode == "bulan") {
		handleAggregatedValueMode(w, deviceID, jenis, periode, mode, valueMode, tahun, bulan, tanggal, limit, tzQuery, tzLabel)
		return
	}

	// MODE: TANGGAL (by specific date)
	if tanggal != "" {
		handlePeriodeByDate(w, deviceID, jenis, tanggal, mode, valueMode, limit, tzQuery, tzLabel)
		return
	}

	// MODE: PERIODE (raw or ringkas without value aggregation)
	handlePeriode(w, deviceID, jenis, periode, mode, tahun, bulan, limit, tzQuery, tzLabel)
}

// -------------------------------------------------------------------------
// Handler: Multi Param Random (Max 30)
// -------------------------------------------------------------------------
func handleMultiParamRandom(w http.ResponseWriter, deviceID, jenis, bulan, tzQuery, tzLabel string) {
	month, year := parseMonth(bulan)
	y, _ := strconv.Atoi(year)
	m, _ := strconv.Atoi(month)

	fromTime := time.Date(y, time.Month(m), 1, 0, 0, 0, 0, time.Local)
	toTime := fromTime.AddDate(0, 1, 0)

	paramList := strings.Split(jenis, ",")

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
		LIMIT 100
	) s ON true
	ORDER BY s.parameter_name, s.recorded_at
	`, tzQuery)

	rows, err := db.Query(query, deviceID, pq.Array(paramList), fromTime, toTime)

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
		Filter:   "multi_param_random",
		Mode:     "random_sample",
		Timezone: tzLabel,
		DeviceID: deviceID,
		Month:    month,
		Year:     year,
		Total:    len(data),
		Data:     data,
	})
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
	err := db.QueryRow(query, deviceID).Scan(
		&s.ID,
		&s.DeviceUniqueID,
		&s.ParameterName,
		&s.Value,
		&s.RecordedAt,
	)

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
	} else if err != nil {
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

// Handler: All parameters (24 hours) - Support Limit
func handleAllParameters(w http.ResponseWriter, deviceID string, limit int, tzQuery, tzLabel string) {
	// Default limit 20000 jika tidak ada input limit
	limitClause := "LIMIT 20000"
	if limit > 0 {
		limitClause = fmt.Sprintf("LIMIT %d", limit)
	}

	query := fmt.Sprintf(`
		SELECT id, device_unique_id, parameter_name, value, 
		       TO_CHAR(%s, 'YYYY-MM-DD HH24:MI:SS') AS recorded_at
		FROM sensor_logs
		WHERE device_unique_id = $1
		  AND recorded_at >= NOW() - INTERVAL '24 HOURS'
		ORDER BY recorded_at DESC
		%s
	`, tzQuery, limitClause)

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
		Status:    true,
		Filter:    "all_parameters",
		Mode:      "raw",
		Timezone:  tzLabel,
		DeviceID:  deviceID,
		TimeRange: "24_hours",
		Total:     len(data),
		Data:      data,
	})
}

// Handler: All parameters by month
func handleAllParametersByMonth(w http.ResponseWriter, deviceID, bulan, tzQuery, tzLabel string) {
	month, year := parseMonth(bulan)
	query := fmt.Sprintf(`
		SELECT id, device_unique_id, parameter_name, value, 
		       TO_CHAR(%s, 'YYYY-MM-DD HH24:MI:SS') AS recorded_at
		FROM sensor_logs
		WHERE device_unique_id = $1
		  AND EXTRACT(YEAR FROM recorded_at) = $2
		  AND EXTRACT(MONTH FROM recorded_at) = $3
		ORDER BY recorded_at DESC, parameter_name ASC
	`, tzQuery)

	rows, err := db.Query(query, deviceID, year, month)
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
		Filter:   "all_parameters_by_month",
		Mode:     "raw",
		Timezone: tzLabel,
		DeviceID: deviceID,
		Month:    month,
		Year:     year,
		Total:    len(data),
		Data:     data,
	})
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

// Handler: Aggregated value mode (ringkas + value high/low/avg) - Support Limit
func handleAggregatedValueMode(w http.ResponseWriter, deviceID, jenis, periode, mode, valueMode, tahun, bulan, tanggal string, limit int, tzQuery, tzLabel string) {
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

	switch periode {
	case "hari":
		if tanggal != "" {
			// Untuk tanggal tertentu
			baseQuery := fmt.Sprintf(`
				SELECT MIN(id) AS id, device_unique_id, parameter_name,
				       ROUND((%s)::numeric, 2) AS value,
				       TO_CHAR(DATE_TRUNC('hour', %s), 'YYYY-MM-DD HH24:MI:SS') AS recorded_at
				FROM sensor_logs
				WHERE device_unique_id = $1
				  AND parameter_name = $2
				  AND recorded_at >= $3::date
				  AND recorded_at < ($3::date + INTERVAL '1 day')
				GROUP BY device_unique_id, parameter_name, DATE_TRUNC('hour', %s)
				ORDER BY DATE_TRUNC('hour', %s) DESC
			`, aggFunc, tzQuery, tzQuery, tzQuery)
			
			if limit > 0 {
				query = fmt.Sprintf(`
					WITH ranked_data AS (%s)
					SELECT * FROM ranked_data ORDER BY recorded_at DESC LIMIT $4
				`, baseQuery)
				args = []interface{}{deviceID, jenis, tanggal, limit}
			} else {
				query = baseQuery
				args = []interface{}{deviceID, jenis, tanggal}
			}
		} else {
			// 24 jam terakhir
			baseQuery := fmt.Sprintf(`
				SELECT MIN(id) AS id, device_unique_id, parameter_name,
				       ROUND((%s)::numeric, 2) AS value,
				       TO_CHAR(DATE_TRUNC('hour', %s), 'YYYY-MM-DD HH24:MI:SS') AS recorded_at
				FROM sensor_logs
				WHERE device_unique_id = $1
				  AND parameter_name = $2
				  AND recorded_at >= NOW() - INTERVAL '24 HOURS'
				GROUP BY device_unique_id, parameter_name, DATE_TRUNC('hour', %s)
				ORDER BY DATE_TRUNC('hour', %s) DESC
			`, aggFunc, tzQuery, tzQuery, tzQuery)
			
			if limit > 0 {
				query = fmt.Sprintf(`
					WITH ranked_data AS (%s)
					SELECT * FROM ranked_data ORDER BY recorded_at DESC LIMIT $3
				`, baseQuery)
				args = []interface{}{deviceID, jenis, limit}
			} else {
				query = baseQuery
				args = []interface{}{deviceID, jenis}
			}
		}
		filter = "hari"

	case "minggu_ini":
		baseQuery := fmt.Sprintf(`
			SELECT MIN(id) AS id, device_unique_id, parameter_name,
			       ROUND((%s)::numeric, 2) AS value,
			       TO_CHAR(DATE(%s), 'YYYY-MM-DD') AS recorded_at
			FROM sensor_logs
			WHERE device_unique_id = $1
			  AND parameter_name = $2
			  AND recorded_at >= CURRENT_DATE - INTERVAL '6 DAYS'
			GROUP BY device_unique_id, parameter_name, DATE(%s)
			ORDER BY DATE(%s) DESC
		`, aggFunc, tzQuery, tzQuery, tzQuery)
		
		if limit > 0 {
			query = fmt.Sprintf(`
				WITH ranked_data AS (%s)
				SELECT * FROM ranked_data ORDER BY recorded_at DESC LIMIT $3
			`, baseQuery)
			args = []interface{}{deviceID, jenis, limit}
		} else {
			query = baseQuery
			args = []interface{}{deviceID, jenis}
		}
		filter = "minggu_ini"

	case "bulan":
		if bulan != "" {
			month, year := parseMonth(bulan)
			baseQuery := fmt.Sprintf(`
				SELECT MIN(id) AS id, device_unique_id, parameter_name,
				       ROUND((%s)::numeric, 2) AS value,
				       TO_CHAR(DATE(%s), 'YYYY-MM-DD') AS recorded_at
				FROM sensor_logs
				WHERE device_unique_id = $1
				  AND parameter_name = $2
				  AND EXTRACT(YEAR FROM recorded_at) = $3
				  AND EXTRACT(MONTH FROM recorded_at) = $4
				GROUP BY device_unique_id, parameter_name, DATE(%s)
				ORDER BY DATE(%s) DESC
			`, aggFunc, tzQuery, tzQuery, tzQuery)
			
			if limit > 0 {
				query = fmt.Sprintf(`
					WITH ranked_data AS (%s)
					SELECT * FROM ranked_data ORDER BY recorded_at DESC LIMIT $5
				`, baseQuery)
				args = []interface{}{deviceID, jenis, year, month, limit}
			} else {
				query = baseQuery
				args = []interface{}{deviceID, jenis, year, month}
			}
		} else {
			baseQuery := fmt.Sprintf(`
				SELECT MIN(id) AS id, device_unique_id, parameter_name,
				       ROUND((%s)::numeric, 2) AS value,
				       TO_CHAR(DATE(%s), 'YYYY-MM-DD') AS recorded_at
				FROM sensor_logs
				WHERE device_unique_id = $1
				  AND parameter_name = $2
				  AND recorded_at >= CURRENT_DATE - INTERVAL '29 DAYS'
				GROUP BY device_unique_id, parameter_name, DATE(%s)
				ORDER BY DATE(%s) DESC
			`, aggFunc, tzQuery, tzQuery, tzQuery)
			
			if limit > 0 {
				query = fmt.Sprintf(`
					WITH ranked_data AS (%s)
					SELECT * FROM ranked_data ORDER BY recorded_at DESC LIMIT $3
				`, baseQuery)
				args = []interface{}{deviceID, jenis, limit}
			} else {
				query = baseQuery
				args = []interface{}{deviceID, jenis}
			}
		}
		filter = "bulan"

	default:
		respondError(w, "periode tidak valid untuk value mode", http.StatusBadRequest)
		return
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

	// Balik urutan data untuk grafik
	for i, j := 0, len(data)-1; i < j; i, j = i+1, j-1 {
		data[i], data[j] = data[j], data[i]
	}

	respond(w, Response{
		Status:   true,
		Filter:   filter,
		Mode:     mode,
		Timezone: tzLabel,
		DeviceID: deviceID,
		Value:    valueMode,
		Total:    len(data),
		Data:     data,
	})
}

// Handler: Periode by date - Support Limit & Single Value High/Low
func handlePeriodeByDate(w http.ResponseWriter, deviceID, jenis, tanggal, mode, valueMode string, limit int, tzQuery, tzLabel string) {
	var query string

	// 1. Jika valueMode ada (high/low/avg) DAN mode BUKAN ringkas
	// Maka ambil 1 data agregat untuk seharian penuh
	if valueMode != "" && mode != "ringkas" {
		var aggFunc string
		var sortDir string // Untuk high/low bisa pakai order, tapi lebih aman pakai MAX/MIN
		switch valueMode {
		case "high":
			aggFunc = "MAX(value)"
			sortDir = "DESC"
		case "low":
			aggFunc = "MIN(value)"
			sortDir = "ASC"
		case "avg":
			aggFunc = "AVG(value)"
		default:
			respondError(w, "value hanya high | low | avg", http.StatusBadRequest)
			return
		}

		// Jika AVG, id mungkin tidak relevan, tapi kita ambil dummy min(id)
		// Jika High/Low, kita ingin row sebenarnya.
		if valueMode == "avg" {
			query = fmt.Sprintf(`
				SELECT 0 AS id, device_unique_id, parameter_name,
				       ROUND((%s)::numeric, 2) AS value,
				       $3::text AS recorded_at
				FROM sensor_logs
				WHERE device_unique_id = $1
				  AND parameter_name = $2
				  AND recorded_at >= $3::date
				  AND recorded_at < ($3::date + INTERVAL '1 day')
				GROUP BY device_unique_id, parameter_name
			`, aggFunc)
		} else {
			// Untuk High/Low, ambil row yang memiliki nilai tersebut
			query = fmt.Sprintf(`
				SELECT id, device_unique_id, parameter_name, value,
				       TO_CHAR(%s, 'YYYY-MM-DD HH24:MI:SS') AS recorded_at
				FROM sensor_logs
				WHERE device_unique_id = $1
				  AND parameter_name = $2
				  AND recorded_at >= $3::date
				  AND recorded_at < ($3::date + INTERVAL '1 day')
				ORDER BY value %s
				LIMIT 1
			`, tzQuery, sortDir)
		}
		
		// Eksekusi khusus untuk single value ini
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

		respond(w, Response{
			Status:   true,
			Filter:   "tanggal",
			Mode:     "single_aggregate", // Mode khusus single value
			Timezone: tzLabel,
			Total:    len(data),
			Data:     data,
			Value:    valueMode,
		})
		return
	}

	// 2. Logic Normal (Raw atau Ringkas per jam)
	if valueMode != "" && mode == "ringkas" {
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

		// Query dengan DESC untuk mengambil data terbaru, lalu kita balik
		baseQuery := fmt.Sprintf(`
			SELECT MIN(id) AS id, device_unique_id, parameter_name,
			       ROUND((%s)::numeric, 2) AS value,
			       TO_CHAR(DATE_TRUNC('hour', %s), 'YYYY-MM-DD HH24:MI:SS') AS recorded_at
			FROM sensor_logs
			WHERE device_unique_id = $1
			  AND parameter_name = $2
			  AND recorded_at >= $3::date
			  AND recorded_at < ($3::date + INTERVAL '1 day')
			GROUP BY device_unique_id, parameter_name, DATE_TRUNC('hour', %s)
			ORDER BY DATE_TRUNC('hour', %s) DESC
		`, aggFunc, tzQuery, tzQuery, tzQuery)
		
		if limit > 0 {
			query = fmt.Sprintf(`
				WITH ranked_data AS (%s)
				SELECT * FROM ranked_data ORDER BY recorded_at DESC LIMIT $4
			`, baseQuery)
		} else {
			query = baseQuery
		}
	} else if mode == "ringkas" {
		// Ringkas tanpa value mode (default avg)
		baseQuery := fmt.Sprintf(`
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
		
		if limit > 0 {
			query = fmt.Sprintf(`
				WITH ranked_data AS (%s)
				SELECT * FROM ranked_data ORDER BY recorded_at DESC LIMIT $4
			`, baseQuery)
		} else {
			query = baseQuery
		}
	} else {
		// RAW DATA
		// Data terbaru dengan DESC
		baseQuery := fmt.Sprintf(`
			SELECT id, device_unique_id, parameter_name, value,
			       TO_CHAR(%s, 'YYYY-MM-DD HH24:MI:SS') AS recorded_at
			FROM sensor_logs
			WHERE device_unique_id = $1
			  AND parameter_name = $2
			  AND recorded_at >= $3::date
			  AND recorded_at < ($3::date + INTERVAL '1 day')
			ORDER BY recorded_at DESC
		`, tzQuery)
		
		if limit > 0 {
			query = fmt.Sprintf("%s LIMIT $4", baseQuery)
		} else {
			query = baseQuery
		}
	}

	// Eksekusi query
	var rows *sql.Rows
	var err error
	
	if limit > 0 && (mode == "ringkas" || (valueMode != "" && mode == "ringkas")) {
		rows, err = db.Query(query, deviceID, jenis, tanggal, limit)
	} else if limit > 0 && mode != "ringkas" {
		rows, err = db.Query(query, deviceID, jenis, tanggal, limit)
	} else {
		rows, err = db.Query(query, deviceID, jenis, tanggal)
	}
	
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

	// Balik urutan data untuk konsistensi (dari lama ke baru)
	if mode == "ringkas" || (valueMode != "" && mode == "ringkas") {
		for i, j := 0, len(data)-1; i < j; i, j = i+1, j-1 {
			data[i], data[j] = data[j], data[i]
		}
	}

	resp := Response{
		Status:   true,
		Filter:   "tanggal",
		Mode:     mode,
		Timezone: tzLabel,
		Total:    len(data),
		Data:     data,
	}

	if valueMode != "" {
		resp.Value = valueMode
	}

	respond(w, resp)
}

// Handler: Periode - Support Limit (DIPERBAIKI UNTUK RINGKAS)
func handlePeriode(w http.ResponseWriter, deviceID, jenis, periode, mode, tahun, bulan string, limit int, tzQuery, tzLabel string) {
	var query string
	var args []interface{}

	switch periode {
	case "hari":
		if mode == "ringkas" {
			// Data terbaru dengan DESC, lalu kita ambil limit, lalu balik
			baseQuery := fmt.Sprintf(`
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
			
			if limit > 0 {
				query = fmt.Sprintf(`
					WITH ranked_data AS (%s)
					SELECT * FROM ranked_data ORDER BY recorded_at DESC LIMIT $3
				`, baseQuery)
				args = []interface{}{deviceID, jenis, limit}
			} else {
				query = baseQuery
				args = []interface{}{deviceID, jenis}
			}
		} else {
			// RAW DATA - Data terbaru dengan DESC
			baseQuery := fmt.Sprintf(`
				SELECT id, device_unique_id, parameter_name, value,
				       TO_CHAR(%s, 'YYYY-MM-DD HH24:MI:SS') AS recorded_at
				FROM sensor_logs
				WHERE device_unique_id = $1
				  AND parameter_name = $2
				  AND recorded_at >= NOW() - INTERVAL '24 HOURS'
				ORDER BY recorded_at DESC
			`, tzQuery)
			
			if limit > 0 {
				query = fmt.Sprintf("%s LIMIT $3", baseQuery)
				args = []interface{}{deviceID, jenis, limit}
			} else {
				query = baseQuery
				args = []interface{}{deviceID, jenis}
			}
		}

	case "minggu_ini":
		if mode == "ringkas" {
			baseQuery := fmt.Sprintf(`
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
			
			if limit > 0 {
				query = fmt.Sprintf(`
					WITH ranked_data AS (%s)
					SELECT * FROM ranked_data ORDER BY recorded_at DESC LIMIT $3
				`, baseQuery)
				args = []interface{}{deviceID, jenis, limit}
			} else {
				query = baseQuery
				args = []interface{}{deviceID, jenis}
			}
		} else {
			baseQuery := fmt.Sprintf(`
				SELECT id, device_unique_id, parameter_name, value,
				       TO_CHAR(%s, 'YYYY-MM-DD HH24:MI:SS') AS recorded_at
				FROM sensor_logs
				WHERE device_unique_id = $1
				  AND parameter_name = $2
				  AND recorded_at >= NOW() - INTERVAL '7 DAYS'
				ORDER BY recorded_at DESC
			`, tzQuery)
			
			if limit > 0 {
				query = fmt.Sprintf("%s LIMIT $3", baseQuery)
				args = []interface{}{deviceID, jenis, limit}
			} else {
				query = baseQuery
				args = []interface{}{deviceID, jenis}
			}
		}

	case "bulan":
		if bulan != "" {
			month, year := parseMonth(bulan)
			tahun = year
			bulan = month
		} else if bulan == "" {
			bulan = fmt.Sprintf("%02d", time.Now().Month())
		}

		if mode == "ringkas" {
			baseQuery := fmt.Sprintf(`
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
			
			if limit > 0 {
				query = fmt.Sprintf(`
					WITH ranked_data AS (%s)
					SELECT * FROM ranked_data ORDER BY recorded_at DESC LIMIT $5
				`, baseQuery)
				args = []interface{}{deviceID, jenis, tahun, bulan, limit}
			} else {
				query = baseQuery
				args = []interface{}{deviceID, jenis, tahun, bulan}
			}
		} else {
			baseQuery := fmt.Sprintf(`
				SELECT id, device_unique_id, parameter_name, value,
				       TO_CHAR(%s, 'YYYY-MM-DD HH24:MI:SS') AS recorded_at
				FROM sensor_logs
				WHERE device_unique_id = $1
				  AND parameter_name = $2
				  AND EXTRACT(YEAR FROM recorded_at) = $3
				  AND EXTRACT(MONTH FROM recorded_at) = $4
				ORDER BY recorded_at DESC
			`, tzQuery)
			
			if limit > 0 {
				query = fmt.Sprintf("%s LIMIT $5", baseQuery)
				args = []interface{}{deviceID, jenis, tahun, bulan, limit}
			} else {
				query = baseQuery
				args = []interface{}{deviceID, jenis, tahun, bulan}
			}
		}

	default:
		respondError(w, "Periode tidak valid", http.StatusBadRequest)
		return
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

	// Balik urutan data untuk mode ringkas agar dari lama ke baru
	if mode == "ringkas" {
		for i, j := 0, len(data)-1; i < j; i, j = i+1, j-1 {
			data[i], data[j] = data[j], data[i]
		}
	}

	respond(w, Response{
		Status:   true,
		Filter:   periode,
		Mode:     mode,
		Timezone: tzLabel,
		Total:    len(data),
		Data:     data,
	})
}

// Helper: Respond with JSON
func respond(w http.ResponseWriter, data Response) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

// Helper: Respond with error
func respondError(w http.ResponseWriter, message string, code int) {
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(Response{
		Status:  false,
		Message: message,
	})
}

func handleMultiDeviceLatest(w http.ResponseWriter, deviceIDs string, tzQuery, tzLabel string) {
	idList := strings.Split(deviceIDs, ",")

	query := fmt.Sprintf(`
	SELECT
		s.id,
		s.device_unique_id,
		s.parameter_name,
		s.value,
		TO_CHAR(%s, 'YYYY-MM-DD HH24:MI:SS') AS recorded_at
	FROM unnest($1::text[]) AS d(device_unique_id)
	JOIN LATERAL (
		SELECT id, device_unique_id, parameter_name, value, recorded_at
		FROM sensor_logs
		WHERE device_unique_id = d.device_unique_id
		ORDER BY recorded_at DESC, id DESC
		LIMIT 1
	) s ON true
	ORDER BY s.device_unique_id;
	`, tzQuery)

	rows, err := db.Query(query, pq.Array(idList))
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	data := []SensorData{}
	for rows.Next() {
		var s SensorData
		if err := rows.Scan(
			&s.ID,
			&s.DeviceUniqueID,
			&s.ParameterName,
			&s.Value,
			&s.RecordedAt,
		); err != nil {
			continue
		}
		data = append(data, s)
	}

	respond(w, Response{
		Status:   true,
		Filter:   "multi_device_latest",
		Mode:     "latest",
		Timezone: tzLabel,
		DeviceID: deviceIDs,
		Total:    len(data),
		Data:     data,
	})
}


func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			respondError(w, "Authorization token tidak ditemukan", http.StatusUnauthorized)
			return
		}
		parts := strings.Split(authHeader, " ")
		if len(parts) != 2 || parts[0] != "Bearer" {
			respondError(w, "Format Authorization harus: Bearer <token>", http.StatusUnauthorized)
			return
		}
		token := parts[1]
		if token != apiToken {
			respondError(w, "Token tidak valid", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func main() {
	initDB()
	defer db.Close()



//INI PAKAI TOKEN
// http.Handle(
// 	"/api/get-data",
// 	corsMiddleware(
// 		authMiddleware(
// 			http.HandlerFunc(getSensorData),
// 		),
// 	),
// )



	// INI TANPA TOKEN
	http.HandleFunc("/api/get-data", getSensorData)
	http.HandleFunc("/api/export/excel-multi", exportExcelMultiSensor)

	port := ":8089"
	log.Printf("🚀 Server running on http://localhost%s", port)
	log.Fatal(http.ListenAndServe(port, nil))
}