package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var mongo_uri string
var redis_url string
var redis_password string
var mdb *mongo.Client
var rdb *redis.Client

var ctx = context.Background()

func init() {
	godotenv.Load(".env")
	redis_url = os.Getenv("REDIS_URL")
	redis_password = os.Getenv("REDIS_PASSWORD")
	mongo_uri = os.Getenv("MONGODB_URI")
}

func main() {
	var err error
	mdb, err = mongo.NewClient(options.Client().ApplyURI(mongo_uri))
	if err != nil {
		panic(err)
	}

	err = mdb.Connect(ctx)
	if err != nil {
		panic(err)
	}

	defer mdb.Disconnect(ctx)

	opt := &redis.Options{
		Addr:     redis_url,
		Password: redis_password,
		DB:       0,
	}

	rdb = redis.NewClient(opt)

	fmt.Println("Connected to redis and mongodb..............")

	fmt.Println("Starting worker..............")
	fmt.Println("\nCtrl+C to stop worker..............")

	for {
		result, err := rdb.BLPop(ctx, 1*time.Second, "queue:raw_rebate_data").Result()
		if err != nil {
			// fmt.Println("Error while reading from redis")
			continue
		}

		if result != nil {
			insertRebateTracing(mdb, readJsonFromRedis(result[1]))
			fmt.Println(result[0], result[1])
		}

	}
}

func insertRebateTracing(client *mongo.Client, data map[string]interface{}) {
	collection := client.Database("busserebatetraces").Collection("tracings")
	_, err := collection.InsertOne(ctx, data)
	if err != nil {
		panic(err)
	}
}

type RebateTracing struct {
	Period         string  `json:"period"`
	Name           string  `json:"name"`
	Addr           string  `json:"addr"`
	City           string  `json:"city"`
	State          string  `json:"state"`
	Postal         string  `json:"postal"`
	Gpo            string  `json:"gpo"`
	License        string  `json:"license"`
	SearchScore    float64 `json:"searchScore"`
	Contract       string  `json:"contract"`
	ClaimNbr       string  `json:"claim_nbr"`
	OrderNbr       string  `json:"order_nbr"`
	InvoiceNbr     string  `json:"invoice_nbr"`
	InvoiceDate    string  `json:"invoice_date"`
	Part           string  `json:"part"`
	UnitRebate     float64 `json:"unit_rebate"`
	ShipQty        int     `json:"ship_qty"`
	Uom            string  `json:"uom"`
	ShipQtyAsCs    int     `json:"ship_qty_as_cs"`
	Rebate         float64 `json:"rebate"`
	Cost           float64 `json:"cost"`
	CheckLicense   bool    `json:"check_license"`
	LicenseChecked bool    `json:"license_checked"`
	AddedToQueue   string  `json:"added_to_queue"`
}

func (r RebateTracing) Interface() map[string]interface{} {
	invoice_date, _ := time.Parse("2006-01-02T15:04:05.000", r.InvoiceDate)
	added_to_queue, _ := time.Parse("2006-01-02T15:04:05.000", r.AddedToQueue)

	return map[string]interface{}{
		"period":          r.Period,
		"name":            r.Name,
		"addr":            r.Addr,
		"city":            r.City,
		"state":           r.State,
		"postal":          r.Postal,
		"gpo":             r.Gpo,
		"license":         r.License,
		"searchScore":     r.SearchScore,
		"contract":        r.Contract,
		"claim_nbr":       r.ClaimNbr,
		"order_nbr":       r.OrderNbr,
		"invoice_nbr":     r.InvoiceNbr,
		"invoice_date":    invoice_date,
		"part":            r.Part,
		"unit_rebate":     r.UnitRebate,
		"ship_qty":        r.ShipQty,
		"uom":             r.Uom,
		"ship_qty_as_cs":  r.ShipQtyAsCs,
		"rebate":          r.Rebate,
		"cost":            r.Cost,
		"check_license":   r.CheckLicense,
		"license_checked": r.LicenseChecked,
		"added_to_queue":  added_to_queue,
		"created_at":      time.Now(),
	}
}

func readJsonFromRedis(data string) map[string]interface{} {
	var r RebateTracing

	json.Unmarshal([]byte(data), &r)

	return r.Interface()
}
