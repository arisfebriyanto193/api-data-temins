package main

import (
	"encoding/csv"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/lib/pq"
	"github.com/xuri/excelize/v2"
)

// ===============================
// PARSE SENSOR META
// format: su:Suhu Udara (°C),ku:Kelembapan (%)
// ===============================
func parseSensorMeta(meta string) map[string]string {
	result := map[string]string{}
	if meta == "" {
		return result
	}
	items := strings.Split(meta, ",")
	for _, item := range items {
		parts := strings.SplitN(item, ":", 2)
		if len(parts) == 2 {
			result[parts[0]] = parts[1]
		}
	}
	return result
}

// ===============================
// KONVERSI ZONA WAKTU
// ===============================
func convertTimezone(t time.Time, zonaWaktu string) time.Time {
	// Data di database sudah dalam WIB
	// WIB = UTC+7
	// WITA = UTC+8 (WIB + 1 jam)
	// WIT = UTC+9 (WIB + 2 jam)
	
	switch strings.ToLower(zonaWaktu) {
	case "wita":
		// Tambah 1 jam dari WIB
		return t.Add(1 * time.Hour)
	case "wit":
		// Tambah 2 jam dari WIB
		return t.Add(2 * time.Hour)
	case "wib":
		fallthrough
	default:
		// Tidak perlu konversi, sudah WIB
		return t
	}
}

// ===============================
// GET ZONA WAKTU LABEL
// ===============================
func getZonaWaktuLabel(zonaWaktu string) string {
	switch strings.ToLower(zonaWaktu) {
	case "wita":
		return "WITA"
	case "wit":
		return "WIT"
	case "wib":
		fallthrough
	default:
		return "WIB"
	}
}

// ===============================
// STRUKTUR DATA PIVOT
// ===============================
type TimeData struct {
	Time   time.Time
	Values map[string]float64
}

// ===============================
// FUNGSI UNTUK AMBIL DAN PIVOT DATA
// ===============================
func fetchAndPivotData(deviceID string, sensors []string, start, end time.Time, zonaWaktu string) (map[string]*TimeData, []string, error) {
	// Query database
	rows, err := db.Query(`
		SELECT
			recorded_at  AS waktu,
			parameter_name,
			value
		FROM sensor_logs
		WHERE device_unique_id = $1
		  AND parameter_name = ANY($2)
		  AND recorded_at >= $3
		  AND recorded_at <  $4
		ORDER BY waktu ASC
	`, deviceID, pq.Array(sensors), start, end)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	// Pivot data
	dataMap := map[string]*TimeData{}
	var timeKeys []string

	for rows.Next() {
		var t time.Time
		var name string
		var val float64
		if err := rows.Scan(&t, &name, &val); err != nil {
			continue
		}

		// Konversi waktu sesuai zona waktu yang diminta
		convertedTime := convertTimezone(t, zonaWaktu)
		key := convertedTime.Format("2006-01-02 15:04:05")

		if _, ok := dataMap[key]; !ok {
			dataMap[key] = &TimeData{
				Time:   convertedTime,
				Values: map[string]float64{},
			}
			timeKeys = append(timeKeys, key)
		}
		dataMap[key].Values[name] = val
	}

	// Sort berdasarkan waktu
	sort.Slice(timeKeys, func(i, j int) bool {
		return dataMap[timeKeys[i]].Time.Before(dataMap[timeKeys[j]].Time)
	})

	return dataMap, timeKeys, nil
}

// ===============================
// FORMAT FLOAT UNTUK CSV/EXCEL
// ===============================
func formatFloatValue(v float64) string {
	// Format float untuk menghilangkan trailing zeros
	// Contoh: 12.370000 -> "12.37", 21.600000 -> "21.6", 0.000000 -> "0"
	str := strconv.FormatFloat(v, 'f', -1, 64)
	
	// Jika ada titik desimal, hapus trailing zeros
	if strings.Contains(str, ".") {
		str = strings.TrimRight(str, "0")
		str = strings.TrimRight(str, ".")
	}
	
	return str
}

// ===============================
// EXPORT CSV MULTI SENSOR (DIPERBAIKI)
// ===============================
func exportCSVMultiSensor(w http.ResponseWriter, deviceID, bulan, tahun string, sensors []string, sensorMeta map[string]string, zonaWaktu string) {
	// Range waktu bulan
	start, err := time.Parse("2006-01-02", fmt.Sprintf("%s-%s-01", tahun, bulan))
	if err != nil {
		http.Error(w, "format bulan/tahun salah", http.StatusBadRequest)
		return
	}
	end := start.AddDate(0, 1, 0)

	// Fetch dan pivot data
	dataMap, timeKeys, err := fetchAndPivotData(deviceID, sensors, start, end, zonaWaktu)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Set header untuk download CSV
	zonaLabel := getZonaWaktuLabel(zonaWaktu)
	filename := fmt.Sprintf("Report_AllSensors_%s_%s_%s.csv", bulan, tahun, zonaLabel)
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", `attachment; filename="`+filename+`"`)

	// Tulis BOM UTF-8 agar Excel bisa baca encoding dengan benar
	w.Write([]byte{0xEF, 0xBB, 0xBF})

	// Buat CSV writer
	writer := csv.NewWriter(w)
	defer writer.Flush()

	// Header
	headers := []string{"No"}
	for _, s := range sensors {
		if label, ok := sensorMeta[s]; ok {
			headers = append(headers, label)
		} else {
			headers = append(headers, strings.ToUpper(s))
		}
	}
	headers = append(headers, fmt.Sprintf("Waktu (%s)", zonaLabel))
	writer.Write(headers)

	// Data rows
	for no, timeKey := range timeKeys {
		values := dataMap[timeKey].Values

		row := []string{fmt.Sprintf("%d", no+1)}
		for _, s := range sensors {
			if v, ok := values[s]; ok {
				// Gunakan formatFloatValue untuk menghilangkan trailing zeros
				row = append(row, formatFloatValue(v))
			} else {
				row = append(row, "0") // atau "" jika ingin kosong
			}
		}
		row = append(row, timeKey)

		writer.Write(row)
	}
}

// ===============================
// EXPORT EXCEL MULTI SENSOR (DIPERBAIKI)
// ===============================
func exportExcelMultiSensorData(w http.ResponseWriter, deviceID, bulan, tahun string, sensors []string, sensorMeta map[string]string, zonaWaktu string) {
	// Range waktu bulan
	start, err := time.Parse("2006-01-02", fmt.Sprintf("%s-%s-01", tahun, bulan))
	if err != nil {
		http.Error(w, "format bulan/tahun salah", http.StatusBadRequest)
		return
	}
	end := start.AddDate(0, 1, 0)

	// Fetch dan pivot data
	dataMap, timeKeys, err := fetchAndPivotData(deviceID, sensors, start, end, zonaWaktu)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Buat Excel
	f := excelize.NewFile()
	sheet := "Data Sensor"
	f.SetSheetName("Sheet1", sheet)

	// Header
	headers := []interface{}{"No"}
	for _, s := range sensors {
		if label, ok := sensorMeta[s]; ok {
			headers = append(headers, label)
		} else {
			headers = append(headers, strings.ToUpper(s))
		}
	}
	zonaLabel := getZonaWaktuLabel(zonaWaktu)
	headers = append(headers, fmt.Sprintf("Waktu (%s)", zonaLabel))
	f.SetSheetRow(sheet, "A1", &headers)

	// Atur style untuk angka (opsional, agar tampilan lebih rapi)
	style, _ := f.NewStyle(&excelize.Style{
		NumFmt: 2, // Format angka dengan 2 desimal
	})

	// Data rows
	rowNum := 2
	for no, timeKey := range timeKeys {
		values := dataMap[timeKey].Values

		rowData := []interface{}{no + 1}
		for _, s := range sensors {
			if v, ok := values[s]; ok {
				rowData = append(rowData, v)
			} else {
				rowData = append(rowData, 0) // atau nil jika ingin kosong
			}
		}
		rowData = append(rowData, timeKey)

		cell, _ := excelize.CoordinatesToCellName(1, rowNum)
		f.SetSheetRow(sheet, cell, &rowData)
		
		// Terapkan style untuk kolom angka (kolom B sampai kolom sebelum waktu)
		for col := 2; col <= len(sensors)+1; col++ {
			cellName, _ := excelize.CoordinatesToCellName(col, rowNum)
			f.SetCellStyle(sheet, cellName, cellName, style)
		}
		
		rowNum++
	}

	// Auto fit column width
	for col := 1; col <= len(sensors)+2; col++ {
		colName, _ := excelize.ColumnNumberToName(col)
		f.SetColWidth(sheet, colName, colName, 15)
	}

	// Response download
	zonaLabel = getZonaWaktuLabel(zonaWaktu)
	filename := fmt.Sprintf("Report_AllSensors_%s_%s_%s.xlsx", bulan, tahun, zonaLabel)
	w.Header().Set("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
	w.Header().Set("Content-Disposition", `attachment; filename="`+filename+`"`)
	if err := f.Write(w); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// ===============================
// HANDLER UTAMA: EXPORT MULTI SENSOR
// ===============================
func exportExcelMultiSensor(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	deviceID := q.Get("device_id")
	bulan := q.Get("bulan")
	tahun := q.Get("tahun")
	sensorParam := q.Get("sensors")
	sensorMetaParam := q.Get("sensor_meta")
	zonaWaktu := q.Get("zonawaktu")
	outputFormat := q.Get("out")

	// Default zona waktu adalah WIB jika tidak diisi
	if zonaWaktu == "" {
		zonaWaktu = "wib"
	}

	// Default output format adalah excel jika tidak diisi
	if outputFormat == "" {
		outputFormat = "excel"
	}

	// Validasi zona waktu
	zonaWaktuLower := strings.ToLower(zonaWaktu)
	if zonaWaktuLower != "wib" && zonaWaktuLower != "wita" && zonaWaktuLower != "wit" {
		http.Error(w, "zonawaktu harus wib, wita, atau wit", http.StatusBadRequest)
		return
	}

	// Validasi output format
	outputFormatLower := strings.ToLower(outputFormat)
	if outputFormatLower != "excel" && outputFormatLower != "csv" {
		http.Error(w, "out harus excel atau csv", http.StatusBadRequest)
		return
	}

	// Validasi parameter wajib
	if deviceID == "" || bulan == "" || tahun == "" || sensorParam == "" {
		http.Error(w, "device_id, bulan, tahun, sensors wajib diisi", http.StatusBadRequest)
		return
	}

	sensors := strings.Split(sensorParam, ",")
	sensorMeta := parseSensorMeta(sensorMetaParam)

	// Route ke fungsi export yang sesuai
	if outputFormatLower == "csv" {
		exportCSVMultiSensor(w, deviceID, bulan, tahun, sensors, sensorMeta, zonaWaktu)
	} else {
		exportExcelMultiSensorData(w, deviceID, bulan, tahun, sensors, sensorMeta, zonaWaktu)
	}
}