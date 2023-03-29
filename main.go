package main

import (
	"C"
	"log"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"
)
import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	arrowschema "github.com/anaray/fluent-bit-arrow-plugin/internal/arrow"
	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/flight"
	"github.com/apache/arrow/go/v12/arrow/ipc"
	"github.com/itchyny/timefmt-go"
	"google.golang.org/grpc"
)

const PluginName = "arrow"
const Name = "Name"
const Id = "Id"
const Desc = "Fluent Bit Arrow Output plugin"
const TimeFields = "Time_Fields"
const FlightServerUrl = "Arrow_Flight_Server_Url"
const InferSchema = "Infer_Schema"
const SchemaFile = "Schema_File"

type Plugin interface {
	Create(ctx unsafe.Pointer) (PluginContext, error)
	//BatchWrite(r arrow.Record)
	BatchWrite(records map[interface{}]interface{})
}

type PluginContext struct {
	Id         string
	TimeFields map[string]string
	Schema     *arrow.Schema
	flightSvc  *ArrowFlightService
}

type ArrowFlightService struct {
	ArrowFlightServerUrl string
	Writer               flight.Writer
}

// wtr := flight.NewRecordWriter(putClient, ipc.WithSchema(schema))
func NewFlightService(url string, schema *arrow.Schema) (*ArrowFlightService, error) {
	conn, err := grpc.Dial(url, grpc.WithInsecure()) // TODO: convert this into secure
	if err != nil {
		return nil, fmt.Errorf("failed to create grpc connection [%s]", url)
	}
	defer conn.Close()

	client := flight.NewFlightServiceClient(conn)
	p, err := client.DoPut(context.Background()) // TODO: Pass GRPC call option here
	if err != nil {
		return nil, err
	}
	wtr := flight.NewRecordWriter(p, ipc.WithSchema(schema))

	return &ArrowFlightService{
		ArrowFlightServerUrl: url,
		Writer:               *wtr,
	}, nil
}

func (svc ArrowFlightService) Write(record arrow.Record) {
	svc.Writer.Write(record)
}

type FluentArrowPlugin struct {
	contexts map[string]PluginContext
}

func NewPlugin() *FluentArrowPlugin {
	return &FluentArrowPlugin{
		contexts: make(map[string]PluginContext),
	}
}

var plugin = NewPlugin()

func (p *FluentArrowPlugin) Create(ctx unsafe.Pointer) (PluginContext, error) {
	errMsg := "mandatory plugin configuration [%s] missing"

	c := PluginContext{
		TimeFields: make(map[string]string),
	}

	// 1) Id
	id := output.FLBPluginConfigKey(ctx, Id)
	if id == "" {
		return PluginContext{}, fmt.Errorf(errMsg, Id)
	}
	c.Id = id

	// 2) Time_Fields
	tf := output.FLBPluginConfigKey(ctx, TimeFields)
	if tf == "" {
		return PluginContext{}, fmt.Errorf(errMsg, TimeFields)
	} else {
		// Time_Fields key's format is comma seperated string "<key_name>=<date_format>,<key_name>=<date_format>,"
		// example: Time_Fields DATE_TIME=%Y-%m-%dT%H:%M:%S%z,
		// the date format is format described in strptime function.
		log.Printf("Time_Field configured in %s output plugin", PluginName)
		splits := strings.Split(tf, ",")
		for _, split := range splits {
			mapping := strings.Split(split, "=")
			if len(mapping) == 2 {
				log.Printf("Time Field=%s, configured strptime format=%s", mapping[0], mapping[1])
				c.TimeFields[mapping[0]] = mapping[1]
			}
		}
	}

	// 3) Arrow_Flight_Server_Url
	fs := output.FLBPluginConfigKey(ctx, FlightServerUrl)
	if fs == "" {
		return PluginContext{}, fmt.Errorf(errMsg, FlightServerUrl)
	}

	// 4) Schema_File
	sf := output.FLBPluginConfigKey(ctx, SchemaFile)
	if sf == "" {
		return PluginContext{}, fmt.Errorf(errMsg, SchemaFile)
	}
	s, err := parseSchema(sf)
	if err != nil {
		return PluginContext{}, err
	}
	c.Schema = s

	// create ArrowFlightService
	fltSvc, err := NewFlightService(fs, s)
	if err != nil {
		return PluginContext{}, err
	}
	c.flightSvc = fltSvc
	return c, nil
}

func (p *FluentArrowPlugin) BatchWrite(pluginId string, records map[interface{}]interface{}) {
	var strVal string
	var intVal int
	var int64Val int64
	var flt64Val float64

	fmt.Println("----")
	for k, v := range records {
		key := k.(string)
		fmt.Println("key :" + key)

		switch v := v.(type) {
		case []uint8:
			strVal = string(v)
			fmt.Println("format :: " + p.contexts[pluginId].TimeFields[key])
			if dateFormat, ok := p.contexts[pluginId].TimeFields[key]; ok {
				t, err := timefmt.Parse(strVal, dateFormat)
				if err != nil {
					log.Printf("failed to parse date %s using format %s", strVal, dateFormat)
					//return output.FLB_ERROR
				}
				int64Val = t.Unix()
				log.Printf("ctx= %s, got int64 value for key=%s value=%d", pluginId, key, int64Val)
				//z, o := t.Zone()
			} else {
				log.Printf("ctx= %s, got string value for key=%s value=%s", pluginId, key, strVal)
			}
		case float64:
			flt64Val = v
			log.Printf("ctx= %s, got float value for key=%s value=%f", pluginId, key, flt64Val)
		case int:
			intVal = v
			log.Printf("ctx= %s, got int value for key=%s value=%d", pluginId, key, intVal)

		case nil:
			strVal = ""
		default:
			strVal = fmt.Sprintf("%v", v)
		}

		//p.plugins[pluginId].flightSvc.Write(r)
	}

}

func parseSchema(file string) (*arrow.Schema, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, fmt.Errorf("error reading schema file %s", f)
	}
	dec := json.NewDecoder(f)
	var s arrowschema.PayloadSchema
	err = dec.Decode(&s)
	if err != nil {
		return nil, fmt.Errorf("error decoding schema file %s", f)
	}

	schema := arrow.NewSchema(
		arrowschema.SchemaFromJSON(s.ArrowSchema, nil).Fields(),
		nil,
	)
	return schema, nil
}

// Fluentbit's Internal Golang functions

//export FLBPluginRegister
func FLBPluginRegister(def unsafe.Pointer) int {
	log.Printf("registering output plugin: [%s]", PluginName)
	return output.FLBPluginRegister(def, PluginName, Desc)
}

//export FLBPluginInit
func FLBPluginInit(ctx unsafe.Pointer) int {
	id := output.FLBPluginConfigKey(ctx, Id)
	c, err := plugin.Create(ctx)
	plugin.contexts[id] = c
	if err != nil {
		fmt.Printf("error %v", err)
		return FLBPluginExit()
	}
	output.FLBPluginSetContext(ctx, id)
	return output.FLB_OK
}

//export FLBPluginFlushCtx
func FLBPluginFlushCtx(ctx, data unsafe.Pointer, length C.int, tag *C.char) int {
	id := output.FLBPluginGetContext(ctx).(string)
	if id == "" {
		log.Fatalf("failed to read the plugin id")
		return FLBPluginExit()
	}

	dec := output.NewDecoder(data, int(length))
	for {
		ret, _, record := output.GetRecord(dec)
		if ret != 0 {
			break
		}
		plugin.BatchWrite(id, record)
		/*for k, v := range record {
			key := k.(string)

		}*/

	}

	return output.FLB_OK
}

func FLBPluginExit() int {
	log.Printf("[%s] [info] exit", PluginName)
	return output.FLB_OK
}

func main() {
}
