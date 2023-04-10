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
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
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
	WriteString(field string, val string) error
}

type PluginContext struct {
	Id          string
	TimeFields  map[string]string
	recordCount int
	Schema      *arrow.Schema
	builder     *ArrowRecordBuilder
	flightSvc   *ArrowFlightService
}

type ArrowRecordBuilder struct {
	recordBuilder *array.RecordBuilder
	fieldIndex    map[string]array.Builder
}

func NewRecordBuilder(schema *arrow.Schema) (*ArrowRecordBuilder, error) {
	b := array.NewRecordBuilder(&memory.GoAllocator{}, schema)

	bldr := &ArrowRecordBuilder{
		recordBuilder: b,
		fieldIndex:    make(map[string]array.Builder, len(schema.Fields())),
	}

	fields := schema.Fields()
	for indx, field := range fields {
		bldr.fieldIndex[field.Name] = b.Field(indx)
	}
	return bldr, nil
}

type ArrowFlightService struct {
	ArrowFlightServerUrl string
	Writer               flight.Writer
}

func NewFlightService(url string, schema *arrow.Schema) (*ArrowFlightService, error) {
	conn, err := grpc.Dial(url, grpc.WithInsecure()) // TODO: convert this into secure
	if err != nil {
		return nil, fmt.Errorf("failed to create grpc connection [%s]", url)
	}
	//defer conn.Close()

	client := flight.NewFlightServiceClient(conn)
	p, err := client.DoPut(context.Background()) // TODO: Pass GRPC call option here
	if err != nil {
		return nil, err
	}
	wtr := flight.NewRecordWriter(p, ipc.WithSchema(schema))
	desc := &flight.FlightDescriptor{
		Type: flight.DescriptorUNKNOWN,
	}
	wtr.SetFlightDescriptor(desc)

	return &ArrowFlightService{
		ArrowFlightServerUrl: url,
		Writer:               *wtr,
	}, nil
}

func (svc ArrowFlightService) Write(record arrow.Record) {
	log.Printf("Write called &v\n", record)
	svc.Writer.Write(record)
}

type FluentArrowPlugin struct {
	contexts map[string]*PluginContext
}

func NewPlugin() *FluentArrowPlugin {
	return &FluentArrowPlugin{
		contexts: make(map[string]*PluginContext),
	}
}

var plugin = NewPlugin()

func (p *FluentArrowPlugin) Create(ctx unsafe.Pointer) (*PluginContext, error) {
	errMsg := "mandatory plugin configuration [%s] missing"
	c := PluginContext{
		TimeFields: make(map[string]string),
	}

	// 1) Id
	id := output.FLBPluginConfigKey(ctx, Id)
	if id == "" {
		return &PluginContext{}, fmt.Errorf(errMsg, Id)
	}
	c.Id = id

	// 2) Time_Fields
	tf := output.FLBPluginConfigKey(ctx, TimeFields)
	if tf != "" {
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
		return &PluginContext{}, fmt.Errorf(errMsg, FlightServerUrl)
	}

	// 4) Schema_File
	sf := output.FLBPluginConfigKey(ctx, SchemaFile)
	if sf == "" {
		return &PluginContext{}, fmt.Errorf(errMsg, SchemaFile)
	}
	s, err := parseSchema(sf)
	if err != nil {
		return &PluginContext{}, err
	}
	c.Schema = s

	// Debug: print schema and fields in it.
	fields := s.Fields()
	for _, f := range fields {
		log.Printf("field name=%s , field type=%s\n ", f.Name, f.Type)

	}

	// Set schema to RecordBuilder
	b, err := NewRecordBuilder(c.Schema)
	if err != nil {
		return &PluginContext{}, err
	}
	c.builder = b

	// create ArrowFlightService
	fltSvc, err := NewFlightService(fs, s)
	if err != nil {
		return &PluginContext{}, err
	}
	c.flightSvc = fltSvc
	return &c, nil
}

func (p FluentArrowPlugin) WriteString(pluginInd string, fieldName string, values []string, valid []bool) {
	log.Print("PluginId :" + pluginInd)
	log.Print("fieldName :" + fieldName)
	//pick the correct builder and append value to the field
	p.contexts[pluginInd].builder.fieldIndex[fieldName].(*array.StringBuilder).AppendValues(values, valid)
}

func (p FluentArrowPlugin) WriteInt64(pluginInd string, fieldName string, values []int64, valid []bool) {
	//pick the correct builder and append value to the field
	p.contexts[pluginInd].builder.fieldIndex[fieldName].(*array.Int64Builder).AppendValues(values, valid)
}

func (p FluentArrowPlugin) WriteUInt64(pluginInd string, fieldName string, values []uint64, valid []bool) {
	//pick the correct builder and append value to the field
	p.contexts[pluginInd].builder.fieldIndex[fieldName].(*array.Uint64Builder).AppendValues(values, valid)
}

func (p FluentArrowPlugin) WriteInt32(pluginInd string, fieldName string, values []int32, valid []bool) {
	//pick the correct builder and append value to the field
	p.contexts[pluginInd].builder.fieldIndex[fieldName].(*array.Int32Builder).AppendValues(values, valid)
}

func (p FluentArrowPlugin) WriteFloat64(pluginInd string, fieldName string, values []float64, valid []bool) {
	//pick the correct builder and append value to the field
	p.contexts[pluginInd].builder.fieldIndex[fieldName].(*array.Float64Builder).AppendValues(values, valid)
}

func (p FluentArrowPlugin) WriteFloat32(pluginInd string, fieldName string, values []float32, valid []bool) {
	//pick the correct builder and append value to the field
	p.contexts[pluginInd].builder.fieldIndex[fieldName].(*array.Float32Builder).AppendValues(values, valid)
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
		return nil, fmt.Errorf("error decoding schema file %s \n %s", f, err.Error())
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
	if err != nil {
		log.Fatalf("%v\n", err)
		return FLBPluginExit()
	}

	plugin.contexts[id] = c
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

		fmt.Println("----")
		for k, v := range record {
			key := k.(string)
			// proces only if the fields are present in the arrow schema for the plugin
			if _, ok := plugin.contexts[id].builder.fieldIndex[key]; !ok {
				continue
			}
			fmt.Println("key :" + key)

			switch v := v.(type) {
			case []uint8:
				strVal := string(v)
				if dateFormat, ok := plugin.contexts[id].TimeFields[key]; ok {
					t, err := timefmt.Parse(strVal, dateFormat)
					if err != nil {
						log.Printf("failed to parse date %s using format %s", strVal, dateFormat)
						//return output.FLB_ERROR
					}
					int64Val := t.Unix()
					log.Printf("ctx= %s, got int64 value for key=%s value=%d", id, key, int64Val)
					//z, o := t.Zone()
				} else {
					log.Printf("ctx= %s, got string value for key=%s value=%s", id, key, strVal)
					plugin.WriteString(id, key, []string{strVal}, []bool{true})
				}
			case float64:
				flt64Val := v
				log.Printf("ctx= %s, got float value for key=%s value=%f", id, key, flt64Val)
				plugin.WriteFloat64(id, key, []float64{flt64Val}, []bool{true})
			case int:
				intVal := v
				log.Printf("ctx= %s, got int value for key=%s value=%d", id, key, intVal)
				plugin.WriteInt64(id, key, []int64{int64(intVal)}, []bool{true})
			case int64:
				intVal := v
				log.Printf("ctx= %s, got int64 value for key=%s value=%d", id, key, intVal)
				plugin.WriteInt64(id, key, []int64{intVal}, []bool{true})
			case nil:
				strVal := ""
				log.Printf("ctx= %s, got nil value for key=%s value=%f", id, key, strVal)
			default:
				strVal := fmt.Sprintf("%v", v)
				log.Printf("ctx= %s, got default value for key=%s value=%f", id, key, strVal)
			}

		}
		plugin.contexts[id].recordCount++
		if plugin.contexts[id].recordCount > 5 {
			//call make record and flush
			r := plugin.contexts[id].builder.recordBuilder.NewRecord()
			defer r.Release()
			fmt.Println("In Flight")
			plugin.contexts[id].flightSvc.Write(r)
			plugin.contexts[id].recordCount = 0
		}
	}
	return output.FLB_OK
}

func FLBPluginExit() int {
	log.Printf("[%s] [info] exit", PluginName)
	return output.FLB_OK
}

func main() {
}
