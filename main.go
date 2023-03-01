package main

import (
	"C"
	"log"
	"unsafe"

	"fmt"
	"strings"

	"github.com/fluent/fluent-bit-go/output"

	"github.com/itchyny/timefmt-go"
)

const PluginName = "arrow"
const PlugingDesc = "Fluent Bit Arrow Output plugin"
const TimeFields = "Time_Fields"
const FlightServerUrl = "Arrow_Flight_Server_Url"

type PluginCfg struct {
	TimeFields           map[string]string
	ArrowFlightServerUrl string
}

//export FLBPluginRegister
func FLBPluginRegister(def unsafe.Pointer) int {
	log.Printf("[%s] registering plugin", PluginName)
	return output.FLBPluginRegister(def, PluginName, PlugingDesc)
}

//export FLBPluginInit
func FLBPluginInit(plugin unsafe.Pointer) int {
	timeFields := output.FLBPluginConfigKey(plugin, TimeFields)
	serverUrl := output.FLBPluginConfigKey(plugin, FlightServerUrl)
	pluginCfg, err := createPluginConfig(timeFields, serverUrl)

	if err != nil {
		return output.FLB_ERROR
	}

	output.FLBPluginSetContext(plugin, pluginCfg)
	return output.FLB_OK
}

//export FLBPluginFlushCtx
func FLBPluginFlushCtx(ctx, data unsafe.Pointer, length C.int, tag *C.char) int {
	cfg := output.FLBPluginGetContext(ctx).(PluginCfg)
	dec := output.NewDecoder(data, int(length))

	for {
		ret, _, record := output.GetRecord(dec)
		if ret != 0 {
			break
		}

		for k, v := range record {
			key := k.(string)
			var strVal string
			var int64Val int64
			var flt64Val float64

			switch v.(type) {
			case []uint8:
				strVal = string(v.([]uint8))
				if dateFormat, ok := cfg.TimeFields[key]; ok {
					t, _ := timefmt.Parse(strVal, dateFormat)
					int64Val = t.Unix()
					log.Printf("Key=%s Int64 Value=%d", key, int64Val)
				} else {
					log.Printf("Key=%s String Value=%s", key, strVal)
				}
			case float64:
				flt64Val = v.(float64)
				log.Printf("Key=%s Float64 Value=%f", key, flt64Val)
			case nil:
				strVal = ""
			default:
				strVal = fmt.Sprintf("%v", v)
			}
		}
	}

	return output.FLB_OK
}

func FLBPluginExit() int {
	log.Printf("[%s] [info] exit", PluginName)
	return output.FLB_OK
}

func createPluginConfig(timeFields string, serverUrl string) (PluginCfg, error) {
	pluginCfg := PluginCfg{
		TimeFields: make(map[string]string),
	}

	if timeFields != "" {
		log.Printf("Time_Field configured in %s output plugin", PluginName)
		splits := strings.Split(timeFields, ",")
		for _, split := range splits {
			mapping := strings.Split(split, "=")
			if len(mapping) == 2 {
				log.Printf("Time Field=%s, configured strptime format=%s", mapping[0], mapping[1])
				pluginCfg.TimeFields[mapping[0]] = mapping[1]
			}
		}
	}

	if serverUrl == "" {
		return pluginCfg, fmt.Errorf("mandatory parameter %s not configured.", FlightServerUrl)
	}

	return pluginCfg, nil
}

func main() {
}
