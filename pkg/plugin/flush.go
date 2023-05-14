package plugin

import (
	"fmt"
	"time"
)

type FlushEvent struct{}

type FlushEventGenerator interface {
	create() chan FlushEvent
	sendEvent()
}

// TimeEventGenerator is implementation of FlushEventGenerator
// it creates events at fixed interval of time, expressed in milliseconds.
type TimeEventGenerator struct {
	eventChan chan FlushEvent
}

func NewTimeEventGenerator() *TimeEventGenerator {
	return &TimeEventGenerator{}
}

func (s *TimeEventGenerator) create() chan FlushEvent {
	s.eventChan = make(chan FlushEvent)
	return s.eventChan
}

// time based event creator - use this function in a goroutine
func (s *TimeEventGenerator) sendEvent() {
	for {
		fmt.Printf("%v+\n", time.Now())
		s.eventChan <- FlushEvent{}
		time.Sleep(time.Second)
	}
}
