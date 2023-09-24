package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	mongoURI       = "mongodb://localhost:27023"
	databaseName   = "traffyFondue"
	collectionName = "postsTraffyFondue"
)

var client *mongo.Client
var postsCollection *mongo.Collection

type Data struct {
	Status     string    `json:"status"`
	Message    string    `json:"message"`
	ExecTime   string    `json:"exec_time"`
	Source     string    `json:"source"`
	Total      int       `json:"total"`
	SumState   SumState  `json:"sum_state"`
	CountTotal int       `json:"count_total"`
	Count      int       `json:"count"`
	Type       string    `json:"type"`
	Features   []Feature `json:"features"`
}

type SumState struct {
	Finish     int `json:"finish"`
	Follow     int `json:"follow"`
	Forward    int `json:"forward"`
	InProgress int `json:"inprogress"`
	Irrelevant int `json:"irrelevant"`
	Start      int `json:"start"`
}

type Feature struct {
	Type       string      `json:"type"`
	Geometry   Coordinates `json:"geometry"`
	Properties Properties  `json:"properties"`
	CreatedAt  time.Time   `json:"created_at"`
}

type Properties struct {
	ProblemTypeFondue   []string    `json:"problem_type_fondue"`
	Org                 []string    `json:"org"`
	Description         string      `json:"description"`
	TicketID            string      `json:"ticket_id"`
	PhotoURL            string      `json:"photo_url"`
	AfterPhoto          string      `json:"after_photo"`
	Address             string      `json:"address"`
	Subdistrict         string      `json:"subdistrict"`
	District            string      `json:"district"`
	Province            string      `json:"province"`
	Timestamp           string      `json:"timestamp"`
	ProblemTypeAbdul    interface{} `json:"problem_type_abdul"`
	Star                interface{} `json:"star"`
	CountReopen         int         `json:"count_reopen"`
	Note                interface{} `json:"note"`
	DescriptionReporter interface{} `json:"description_reporter"`
	State               string      `json:"state"`
	StateTypeLatest     string      `json:"state_type_latest"`
	LastActivity        string      `json:"last_activity"`
	Type                string      `json:"type"`
	SeeInfo             bool        `json:"see_info"`
}

type Complaint struct {
	Address            string `json:"address"`
	Comment            string `json:"comment"`
	Coords             string `json:"coords"`
	CountReopen        string `json:"count_reopen"`
	District           string `json:"district"`
	LastActivity       string `json:"last_activity"`
	Organization       string `json:"organization"`
	OrganizationAction string `json:"organization_action"`
	Photo              string `json:"photo"`
	PhotoAfter         string `json:"photo_after"`
	Province           string `json:"province"`
	Star               string `json:"star"`
	State              string `json:"state"`
	Subdistrict        string `json:"subdistrict"`
	Timestamp          string `json:"timestamp"`
	Type               string `json:"type"`
	TicketID           string `json:"ticket_id"`
}

type Coordinates struct {
	Type        string    `json:"type"`
	Coordinates []float64 `json:"coordinates"`
}

var dataCache Data // Data

func fetchData(start, end string, offset, limit int) error {
	url := fmt.Sprintf(
		"https://publicapi.traffy.in.th/teamchadchart-stat-api/geojson/v1?output_format=json/?start=%s&end=%s&limit=%d&offset=%d",
		start, end, limit, offset,
	)

	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var newData Data

	if err := json.NewDecoder(resp.Body).Decode(&newData); err != nil {
		return err
	}

	dataCache = newData

	return nil
}

func fetchDataCSV(start, end string, offset, limit int, name, org, purpose, email string) (string, error) {
	baseURL := "https://publicapi.traffy.in.th/teamchadchart-stat-api/geojson/v1"
	params := url.Values{}
	params.Add("output_format", "csv")
	params.Add("start", start)
	params.Add("end", end)
	params.Add("limit", fmt.Sprintf("%d", limit))
	params.Add("offset", fmt.Sprintf("%d", offset))
	params.Add("name", name)
	params.Add("org", org)
	params.Add("purpose", purpose)
	params.Add("email", email)

	fetchURL := fmt.Sprintf("%s?%s", baseURL, params.Encode())

	resp, err := http.Get(fetchURL)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

func convertCSVToJSON(csvData string) (string, error) {
	r := csv.NewReader(bytes.NewReader([]byte(csvData)))

	records, err := r.ReadAll()
	if err != nil {
		return "", err
	}

	if len(records) == 0 {
		return "", nil
	}

	var jsonArray []map[string]string
	headers := records[0]

	for _, record := range records[1:] {
		jsonItem := make(map[string]string)
		for i, header := range headers {
			jsonItem[header] = record[i]
		}
		jsonArray = append(jsonArray, jsonItem)
	}

	jsonData, err := json.Marshal(jsonArray)

	if err != nil {
		return "", err
	}

	return string(jsonData), nil
}

func initMongoDB() error {
	clientOptions := options.Client().ApplyURI(mongoURI)
	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		return err
	}

	err = client.Ping(context.Background(), nil)
	if err != nil {
		return err
	}

	postsCollection = client.Database(databaseName).Collection(collectionName)

	return nil
}

func saveFeaturesToMongoDB(ctx context.Context, data Data) error {
	var featuresAsInterfaces []interface{}
	for _, feature := range data.Features {
		featuresAsInterfaces = append(featuresAsInterfaces, feature)
	}
	_, err := postsCollection.InsertMany(ctx, featuresAsInterfaces)
	return err
}

func saveFeaturesToMongoDBCSV(ctx context.Context, data []Complaint) error {
	var featuresAsInterfaces []interface{}
	for _, complaint := range data {
		featuresAsInterfaces = append(featuresAsInterfaces, complaint)
	}
	_, err := postsCollection.InsertMany(ctx, featuresAsInterfaces)
	return err
}

func isValidDate(date string) bool {
	_, err := time.Parse("2006-01-02", date)
	return err == nil
}

func main() {

	if err := initMongoDB(); err != nil {
		fmt.Println("Failed to connect to MongoDB:", err)
		return
	}

	if err := fetchData("", "", 0, 0); err != nil {
		fmt.Println("Failed to fetch initial data:", err)
		return
	}

	r := gin.Default()

	r.POST("/saveToMongoDBCSV", func(c *gin.Context) {
		offsetStr := c.Query("offset")
		limitStr := c.Query("limit")
		startDate := c.Query("start")
		endDate := c.Query("end")
		name := c.Query("name")
		org := c.Query("org")
		purpose := c.Query("purpose")
		email := c.Query("email")
		totalCount := dataCache.CountTotal

		if startDate != "" && !isValidDate(startDate) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid start_date format"})
			return
		}

		if endDate != "" && !isValidDate(endDate) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid end_date format"})
			return
		}

		offset, err := strconv.Atoi(strings.TrimSpace(offsetStr))
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid offset"})
			return
		}

		limit, err := strconv.Atoi(strings.TrimSpace(limitStr))
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid limit", "details": err.Error()})
			return
		}

		totalCount = dataCache.Total
		iterations := totalCount / 25000

		if totalCount%25000 > 0 {
			iterations++
		}

		for i := 0; i < iterations; i++ {
			fmt.Println("Iteration", i)
			fmt.Println("Offset", offset)
			fmt.Println("Limit", limit)

			csvData, err := fetchDataCSV(startDate, endDate, offset, limit, name, org, purpose, email)
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch data"})
				return
			}

			jsonData, err := convertCSVToJSON(csvData)
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to convert CSV to JSON"})
				return
			}

			var Complaints []Complaint
			if err := json.Unmarshal([]byte(jsonData), &Complaints); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to unmarshal JSON to dataCache", "details": err.Error()})
				return
			}

			if len(Complaints) == 0 {
				c.JSON(http.StatusOK, gin.H{"status": "No data to insert into MongoDB"})
				return
			}

			if err := saveFeaturesToMongoDBCSV(c.Request.Context(), Complaints); err != nil {
				fmt.Println("Failed to append data to MongoDB:", err)
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to append data to MongoDB", "details": err.Error()})
				return
			}

			offset += limit
		}

		c.JSON(http.StatusOK, gin.H{"status": "Data successfully saved to MongoDB"})
	})

	r.POST("/saveToMongoDB", func(c *gin.Context) {
		ctx := c.Request.Context()
		offsetStr := c.Query("offset")
		limitStr := c.Query("limit")
		startDate := c.Query("start")
		endDate := c.Query("end")
		totalCount := dataCache.CountTotal

		offset, err := strconv.Atoi(strings.TrimSpace(offsetStr))
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid offset"})
			return
		}

		limit, err := strconv.Atoi(strings.TrimSpace(limitStr))
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid limit"})
			return
		}

		totalCount = dataCache.Total
		iterations := totalCount / 1000

		if totalCount%1000 > 0 {
			iterations++
		}

		for i := 0; i < iterations; i++ {
			fmt.Println("iterations", i)
			fmt.Println("offset", offset)
			fmt.Println("limit", limit)
			if err := fetchData(startDate, endDate, offset, limit); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch data"})
				return
			}

			if err := saveFeaturesToMongoDB(ctx, dataCache); err != nil { // Assuming dataCache is of type Data
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to append data to MongoDB"})
				return
			}

			offset += limit
		}

		c.JSON(http.StatusOK, gin.H{"status": "Data successfully saved to MongoDB"})
	})

	r.GET("/", func(c *gin.Context) {
		offsetStr := c.Query("offset")
		limitStr := c.Query("limit")
		startDate := c.Query("start")
		endDate := c.Query("end")

		if startDate != "" && !isValidDate(startDate) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid start_date format"})
			return
		}

		if endDate != "" && !isValidDate(endDate) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid end_date format"})
			return
		}

		offset, err := strconv.Atoi(strings.TrimSpace(offsetStr))
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid offset"})
			return
		}

		limit, err := strconv.Atoi(strings.TrimSpace(limitStr))
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid limit", "details": err.Error()})
			return
		}

		if err := fetchData(startDate, endDate, offset, limit); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch data"})
			return
		}

		c.JSON(http.StatusOK, dataCache)
	})

	r.GET("/topojson", func(c *gin.Context) {
		offsetStr := c.Query("offset")
		limitStr := c.Query("limit")
		startDate := c.Query("start")
		endDate := c.Query("end")
		name := c.Query("name")
		org := c.Query("org")
		purpose := c.Query("purpose")
		email := c.Query("email")

		if startDate != "" && !isValidDate(startDate) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid start_date format"})
			return
		}

		if endDate != "" && !isValidDate(endDate) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid end_date format"})
			return
		}

		offset, err := strconv.Atoi(strings.TrimSpace(offsetStr))
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid offset"})
			return
		}

		limit, err := strconv.Atoi(strings.TrimSpace(limitStr))
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid limit", "details": err.Error()})
			return
		}

		csvData, err := fetchDataCSV(startDate, endDate, offset, limit, name, org, purpose, email)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch CSV data"})
			return
		}

		jsonData, err := convertCSVToJSON(csvData)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to convert CSV to JSON"})
			return
		}

		var Complaints []Complaint
		if err := json.Unmarshal([]byte(jsonData), &Complaints); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to unmarshal JSON to dataCache", "details": err.Error()})
			return
		}

		c.JSON(http.StatusOK, Complaints)
	})

	err := r.Run(":8000")
	if err != nil {
		return
	}
}
