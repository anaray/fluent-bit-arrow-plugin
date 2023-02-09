package main

import (
	"C"
	"log"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"
)

const PluginName = "arrow"
const PlugingDesc = "Fluent Bit Arrow Output plugin"

//export FLBPluginRegister
func FLBPluginRegister(def unsafe.Pointer) int {
	// Gets called only once when the plugin.so is loaded
	log.Printf("[%s] registering plugin", PluginName)
	return output.FLBPluginRegister(def, PluginName, PlugingDesc)
}

//export FLBPluginInit
func FLBPluginInit(plugin unsafe.Pointer) int {
	return output.FLB_OK
}

//export FLBPluginFlushCtx
func FLBPluginFlushCtx(ctx, data unsafe.Pointer, length C.int, tag *C.char) int {
	//log.Printf("[%s] received flush command", PluginName)

	dec := output.NewDecoder(data, int(length))

	for {
		//ret, ts, record := output.GetRecord(dec)
		ret, _, record := output.GetRecord(dec)
		if ret != 0 {
			break
		}

		// Print record keys and values
		//timestamp := ts.(output.FLBTime)
		//str := fmt.Sprintf("%s %s\n", C.GoString(tag), timestamp.String())

		for k, v := range record {
			//str += fmt.Sprintf("%s: %s\n", k, v)
			log.Printf("record k=%s,v=%s :", k, v)
		}

		//

		log.Println("---")

	}

	return output.FLB_OK
}

func FLBPluginExit() int {
	log.Printf("[%s] [info] exit", PluginName)
	return output.FLB_OK
}

func main() {
}
