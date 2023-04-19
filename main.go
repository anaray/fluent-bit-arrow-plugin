package main

import (
	"C"
	"log"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"
)
import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/anaray/fluent-bit-arrow-plugin/pkg/plugin"

	arrowschema "github.com/anaray/fluent-bit-arrow-plugin/internal/arrow"
	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/itchyny/timefmt-go"
)

const PluginName = "arrow"
const Name = "Name"
const Id = "Id"
const Desc = "Fluent Bit Arrow Output plugin"
const TimeFields = "Time_Fields"
const FlightServerUrl = "Arrow_Flight_Server_Url"
const InferSchema = "Infer_Schema"
const SchemaFile = "Schema_File"
const RecordBatchThreshold = "Record_Batch_Threshold"

// FluentArrowPlugin represents a FluentBit output plugin.

// FluentBitArrowPlugin contains a map which PluginId to PluginContext
type FluentArrowPlugin struct {
	contexts map[string]*plugin.PluginContext
}

// NewPlugin initializes a Plugin
func NewPlugin() plugin.Plugin {
	return FluentArrowPlugin{
		contexts: make(map[string]*plugin.PluginContext),
	}
}

var arrowPlugin plugin.Plugin = NewPlugin()

// Create reads the FluentBit configuration block for FluentArrowPlugin.

// Create reads the configuration block, validates and creates PluginContext
func (p FluentArrowPlugin) Create(ctx unsafe.Pointer) (*plugin.PluginContext, error) {
	errMsg := "mandatory plugin configuration [%s] missing"

	c := plugin.PluginContext{
		TimeFields: make(map[string]string),
	}

	// 1) Id
	id := output.FLBPluginConfigKey(ctx, Id)
	if id == "" {
		return &plugin.PluginContext{}, fmt.Errorf(errMsg, Id)
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
		return &plugin.PluginContext{}, fmt.Errorf(errMsg, FlightServerUrl)
	}

	// 4) Record_Batch_Count
	rb, err := strconv.Atoi(output.FLBPluginConfigKey(ctx, RecordBatchThreshold))
	if err != nil || rb == 0 {
		return &plugin.PluginContext{}, fmt.Errorf(errMsg, RecordBatchThreshold)
	}
	c.RecordBatchThreshold = rb

	// 5) Schema_File
	sf := output.FLBPluginConfigKey(ctx, SchemaFile)
	if sf == "" {
		return &plugin.PluginContext{}, fmt.Errorf(errMsg, SchemaFile)
	}
	s, err := parseSchema(sf)
	if err != nil {
		return &plugin.PluginContext{}, err
	}
	c.Schema = s

	// Debug: print schema and fields in it.
	fields := s.Fields()
	for _, f := range fields {
		log.Printf("field name=%s , field type=%s\n ", f.Name, f.Type)
	}

	// Set schema to RecordBuilder
	b, err := plugin.NewRecordBuilder(c.Schema)
	if err != nil {
		return &plugin.PluginContext{}, err
	}
	c.Builder = b

	// create ArrowFlightService
	fltSvc, err := plugin.NewFlightService(fs, s)
	if err != nil {
		return &plugin.PluginContext{}, err
	}
	c.FlightSvc = fltSvc
	return &c, nil
}

// Writes given string array as a Apache Arrow columnar array
func (p FluentArrowPlugin) WriteString(pluginId string, fieldName string, values []string, valid []bool) {

	p.contexts[pluginId].Builder.FieldIndex[fieldName].(*array.StringBuilder).AppendValues(values, valid)
}

// Writes given int64 array as a Apache Arrow columnar array
func (p FluentArrowPlugin) WriteInt64(pluginId string, fieldName string, values []int64, valid []bool) {
	//pick the correct builder and append value to the field
	p.contexts[pluginId].Builder.FieldIndex[fieldName].(*array.Int64Builder).AppendValues(values, valid)
}

// Writes given uint64 array as a Apache Arrow columnar array
func (p FluentArrowPlugin) WriteUInt64(pluginId string, fieldName string, values []uint64, valid []bool) {
	//pick the correct builder and append value to the field
	p.contexts[pluginId].Builder.FieldIndex[fieldName].(*array.Uint64Builder).AppendValues(values, valid)
}

// Writes given int32 array as a Apache Arrow columnar array
func (p FluentArrowPlugin) WriteInt32(pluginId string, fieldName string, values []int32, valid []bool) {
	//pick the correct builder and append value to the field
	p.contexts[pluginId].Builder.FieldIndex[fieldName].(*array.Int32Builder).AppendValues(values, valid)
}

// Writes given float64 array as a Apache Arrow columnar array
func (p FluentArrowPlugin) WriteFloat64(pluginId string, fieldName string, values []float64, valid []bool) {
	//pick the correct builder and append value to the field
	p.contexts[pluginId].Builder.FieldIndex[fieldName].(*array.Float64Builder).AppendValues(values, valid)
}

// Writes given float32 array as a Apache Arrow columnar array
func (p FluentArrowPlugin) WriteFloat32(pluginId string, fieldName string, values []float32, valid []bool) {
	//pick the correct builder and append value to the field
	p.contexts[pluginId].Builder.FieldIndex[fieldName].(*array.Float32Builder).AppendValues(values, valid)
}

// Writes given arrow timestamp array as a Apache Arrow columnar array
func (p FluentArrowPlugin) WriteTimeStamp(pluginId string, fieldName string, values []arrow.Timestamp, valid []bool) {
	p.contexts[pluginId].Builder.FieldIndex[fieldName].(*array.TimestampBuilder).AppendValues(values, valid)
}

// Reads a file and parses its content as Arrow Schema
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
	c, err := arrowPlugin.Create(ctx)
	if err != nil {
		log.Fatalf("%v\n", err)
		return FLBPluginExit()
	}

	arrowPlugin.(FluentArrowPlugin).contexts[c.Id] = c
	output.FLBPluginSetContext(ctx, c.Id)
	return output.FLB_OK
}

//export FLBPluginFlushCtx
func FLBPluginFlushCtx(ctx, data unsafe.Pointer, length C.int, tag *C.char) int {
	id := output.FLBPluginGetContext(ctx).(string)
	dec := output.NewDecoder(data, int(length))
	for {
		ret, _, record := output.GetRecord(dec)
		if ret != 0 {
			break
		}

		for k, v := range record {
			key := k.(string)
			// process only if the fields are present in the arrow schema for the plugin
			if _, ok := arrowPlugin.(FluentArrowPlugin).contexts[id].Builder.FieldIndex[key]; ok {
				switch v := v.(type) {
				case []uint8:
					strVal := string(v)
					if dateFormat, ok := arrowPlugin.(FluentArrowPlugin).contexts[id].TimeFields[key]; ok {
						t, err := timefmt.Parse(strVal, dateFormat)
						if err != nil {
							log.Printf("failed to parse date %s using format %s", strVal, dateFormat)
							//return output.FLB_ERROR
						}
						int64Val := t.Unix()
						log.Printf("ctx= %s, got int64 value for key=%s value=%d", id, key, int64Val)
						arrowPlugin.WriteTimeStamp(id, key, []arrow.Timestamp{arrow.Timestamp(int64Val)}, []bool{true})
						//z, o := t.Zone()
					} else {
						log.Printf("ctx= %s, got string value for key=%s value=%s", id, key, strVal)
						arrowPlugin.WriteString(id, key, []string{strVal}, []bool{true})
					}
				case float64:
					flt64Val := v
					log.Printf("ctx= %s, got float value for key=%s value=%f", id, key, flt64Val)
					arrowPlugin.WriteFloat64(id, key, []float64{flt64Val}, []bool{true})
				case int:
					intVal := v
					log.Printf("ctx= %s, got int value for key=%s value=%d", id, key, intVal)
					arrowPlugin.WriteInt64(id, key, []int64{int64(intVal)}, []bool{true})
				case int64:
					intVal := v
					log.Printf("ctx= %s, got int64 value for key=%s value=%d", id, key, intVal)
					arrowPlugin.WriteInt64(id, key, []int64{intVal}, []bool{true})
				case nil:
					strVal := ""
					log.Printf("ctx= %s, got nil value for key=%s value=%f", id, key, strVal)
				default:
					strVal := fmt.Sprintf("%v", v)
					log.Printf("ctx= %s, got default value for key=%s value=%f", id, key, strVal)
				}
			}
		}

		arrowPlugin.(FluentArrowPlugin).contexts[id].RecordBatchCount++
		if arrowPlugin.(FluentArrowPlugin).contexts[id].RecordBatchCount > arrowPlugin.(FluentArrowPlugin).contexts[id].RecordBatchThreshold {
			//call make record and flush
			r := arrowPlugin.(FluentArrowPlugin).contexts[id].Builder.RecordBuilder.NewRecord()
			defer r.Release()
			arrowPlugin.(FluentArrowPlugin).contexts[id].FlightSvc.Write(r)
			arrowPlugin.(FluentArrowPlugin).contexts[id].RecordBatchCount = 0
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
