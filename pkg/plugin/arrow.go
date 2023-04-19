package plugin

import (
	"context"
	"fmt"
	"unsafe"

	"github.com/apache/arrow/go/arrow/memory"
	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/flight"
	"github.com/apache/arrow/go/v12/arrow/ipc"
	"google.golang.org/grpc"
)

// Plugin provides an interface for initialising a plugin and to build arrow arrays
type Plugin interface {
	Create(ctx unsafe.Pointer) (*PluginContext, error)
	WriteString(pluginId string, fieldName string, values []string, valid []bool)
	WriteInt64(pluginId string, fieldName string, values []int64, valid []bool)
	WriteUInt64(pluginId string, fieldName string, values []uint64, valid []bool)
	WriteInt32(pluginId string, fieldName string, values []int32, valid []bool)
	WriteFloat64(pluginId string, fieldName string, values []float64, valid []bool)
	WriteFloat32(pluginId string, fieldName string, values []float32, valid []bool)
	WriteTimeStamp(pluginId string, fieldName string, values []arrow.Timestamp, valid []bool)
}

// PluginContext wraps context required for an individual plugin

// Configurations for each plugin is stored in this context
type PluginContext struct {
	Id                   string
	TimeFields           map[string]string
	RecordBatchThreshold int
	RecordBatchCount     int
	Schema               *arrow.Schema
	Builder              *ArrowRecordBuilder
	FlightSvc            *ArrowFlightService
}

// ArrowFlightService aids and creates a Arrow Flight Client and Flight Writer.
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

// Write a Record
func (svc ArrowFlightService) Write(record arrow.Record) {
	svc.Writer.Write(record)
}

// ArrowRecordBuilder maps fieldName to its array.Builder
// This is created based on the given arrow schema.
type ArrowRecordBuilder struct {
	RecordBuilder *array.RecordBuilder
	FieldIndex    map[string]array.Builder
}

// NewRecordBuilder creates ArrowRecordBuilder, which maps fieldName to
// array.Builder. This id derived from a configured arrow Schema.
func NewRecordBuilder(schema *arrow.Schema) (*ArrowRecordBuilder, error) {
	b := array.NewRecordBuilder(&memory.GoAllocator{}, schema)
	bldr := &ArrowRecordBuilder{
		RecordBuilder: b,
		FieldIndex:    make(map[string]array.Builder, len(schema.Fields())),
	}
	fields := schema.Fields()
	for indx, field := range fields {
		bldr.FieldIndex[field.Name] = b.Field(indx)
	}
	return bldr, nil
}
