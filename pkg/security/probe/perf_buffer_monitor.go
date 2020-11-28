// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-2020 Datadog, Inc.

// +build linux_bpf

package probe

import (
	"fmt"
	"math"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	lib "github.com/DataDog/ebpf"
	"github.com/DataDog/ebpf/manager"
	"github.com/pkg/errors"

	"github.com/DataDog/datadog-agent/pkg/security/ebpf"
	"github.com/DataDog/datadog-agent/pkg/security/ebpf/probes"
)

// PerfMapStats contains the collected metrics for one event and one cpu in a perf buffer statistics map
type PerfMapStats struct {
	Bytes uint64
	Count uint64
	Lost  uint64

	Usage   int64
	InQueue int64
}

// UnmarshalBinary parses a map entry and populates the current PerfMapStats instance
func (s *PerfMapStats) UnmarshalBinary(data []byte) error {
	if len(data) < 24 {
		return ErrNotEnoughData
	}
	s.Bytes = ebpf.ByteOrder.Uint64(data[0:8])
	s.Count = ebpf.ByteOrder.Uint64(data[8:16])
	s.Lost = ebpf.ByteOrder.Uint64(data[16:24])
	return nil
}

// MarshalBinary encodes the relevant fields of the current PerfMapStats instance into a byte array
func (s *PerfMapStats) MarshalBinary() ([]byte, error) {
	b := make([]byte, 24)
	ebpf.ByteOrder.PutUint64(b, s.Bytes)
	ebpf.ByteOrder.PutUint64(b, s.Count)
	ebpf.ByteOrder.PutUint64(b, s.Lost)
	return b, nil
}

// PerfBufferMonitor holds statistics about the number of lost and received events
//nolint:structcheck,unused
type PerfBufferMonitor struct {
	// probe is a pointer to the Probe
	probe *Probe
	// cpuCount holds the current count of CPU
	cpuCount int
	// perfBufferStatsMaps holds the pointers to the statistics kernel maps
	perfBufferStatsMaps map[string][2]*lib.Map
	// perfBufferSize holds the size of each perf buffer, indexed by the name of the perf buffer
	perfBufferSize map[string]float64

	// perfBufferMapNameToStatsMapsName maps a perf buffer to its statistics maps
	perfBufferMapNameToStatsMapsName map[string][2]string
	// statsMapsNamePerfBufferMapName maps a statistic map to its perf buffer
	statsMapsNameToPerfBufferMapName map[string]string

	// bufferSelector is the kernel map used to select the active buffer ID
	bufferSelector *lib.Map
	// activeMapIndex is the index of the statistic maps we are currently collecting data from
	activeMapIndex uint32

	// stats holds the collected metrics
	stats map[string][][maxEventType]PerfMapStats
	// readLostEvents is the count of lost events, collected by reading the perf buffer
	readLostEvents map[string][]uint64
}

// NewPerfBufferMonitor instantiates a new event statistics counter
func NewPerfBufferMonitor(p *Probe) (*PerfBufferMonitor, error) {
	es := PerfBufferMonitor{
		probe:               p,
		cpuCount:            runtime.NumCPU(),
		perfBufferStatsMaps: make(map[string][2]*lib.Map),
		perfBufferSize:      make(map[string]float64),

		perfBufferMapNameToStatsMapsName: probes.GetPerfBufferStatisticsMaps(),
		statsMapsNameToPerfBufferMapName: make(map[string]string),

		stats:          make(map[string][][maxEventType]PerfMapStats),
		readLostEvents: make(map[string][]uint64),
	}

	// compute statsMapPerfMap
	for perfMap, statsMaps := range es.perfBufferMapNameToStatsMapsName {
		for _, statsMap := range statsMaps {
			es.statsMapsNameToPerfBufferMapName[statsMap] = perfMap
		}
	}

	// Select perf buffer statistics maps
	for perfMapName, statsMapsNames := range es.perfBufferMapNameToStatsMapsName {
		var maps [2]*lib.Map
		for i, statsMapName := range statsMapsNames {
			stats, ok, err := p.manager.GetMap(statsMapName)
			if !ok {
				return nil, errors.Errorf("map %s not found", statsMapName)
			}
			if err != nil {
				return nil, err
			}
			maps[i] = stats
		}
		es.perfBufferStatsMaps[perfMapName] = maps
		// set default perf buffer size, it will be readjusted in the next loop if needed
		es.perfBufferSize[perfMapName] = float64(p.managerOptions.DefaultPerfRingBufferSize)
	}

	// Prepare user space counters
	for _, m := range p.manager.PerfMaps {
		var stats [][maxEventType]PerfMapStats
		var usrLostEvents []uint64

		for i := 0; i < es.cpuCount; i++ {
			stats = append(stats, [maxEventType]PerfMapStats{})
			usrLostEvents = append(usrLostEvents, 0)
		}

		es.stats[m.Name] = stats
		es.readLostEvents[m.Name] = usrLostEvents

		// update perf buffer size if needed
		if m.PerfRingBufferSize != 0 {
			es.perfBufferSize[m.Name] = float64(m.PerfRingBufferSize)
		}
	}

	// select the buffer selector map
	bufferSelector, ok, err := p.manager.GetMap("buffer_selector")
	if !ok {
		return nil, errors.Errorf("map buffer_selector not found")
	}
	if err != nil {
		return nil, err
	}
	es.bufferSelector = bufferSelector
	return &es, nil
}

// getPerfMapFromStatsMap returns the perf map associated with a stats map
func (pbm *PerfBufferMonitor) getPerfMapFromStatsMap(statsMap string) string {
	perfMap, ok := pbm.statsMapsNameToPerfBufferMapName[statsMap]
	if ok {
		return perfMap
	}
	return fmt.Sprintf("unknown_%s", statsMap)
}

// getReadLostCount is an internal function, it can segfault if its parameters are incorrect.
func (pbm *PerfBufferMonitor) getReadLostCount(perfMap string, cpu int) uint64 {
	return atomic.LoadUint64(&pbm.readLostEvents[perfMap][cpu])
}

// GetReadLostCount returns the number of lost events for a given map and cpu. If a cpu of -1 is provided, the function will
// return the sum of all the lost events of all the cpus.
func (pbm *PerfBufferMonitor) GetReadLostCount(perfMap string, cpu int) uint64 {
	var total uint64

	switch {
	case cpu == -1:
		for i := range pbm.readLostEvents[perfMap] {
			total += pbm.getReadLostCount(perfMap, i)
		}
		break
	case cpu >= 0:
		if pbm.cpuCount <= cpu {
			break
		}
		total += pbm.getReadLostCount(perfMap, cpu)
	}

	return total
}

// getAndResetReadLostCount is an internal function, it can segfault if its parameters are incorrect.
func (pbm *PerfBufferMonitor) getAndResetReadLostCount(perfMap string, cpu int) uint64 {
	return atomic.SwapUint64(&pbm.readLostEvents[perfMap][cpu], 0)
}

// GetAndResetReadLostCount returns the number of lost events and resets the counter for a given map and cpu. If a cpu of -1 is
// provided, the function will reset the counters of all the cpus for the provided map, and return the sum of all the
// lost events of all the cpus of the provided map.
func (pbm *PerfBufferMonitor) GetAndResetReadLostCount(perfMap string, cpu int) uint64 {
	var total uint64

	switch {
	case cpu == -1:
		for i := range pbm.readLostEvents[perfMap] {
			total += pbm.getAndResetReadLostCount(perfMap, i)
		}
		break
	case cpu >= 0:
		if pbm.cpuCount <= cpu {
			break
		}
		total += pbm.getAndResetReadLostCount(perfMap, cpu)
	}
	return total
}

// getEventCount is an internal function, it can segfault if its parameters are incorrect.
func (pbm *PerfBufferMonitor) getEventCount(eventType EventType, perfMap string, cpu int) uint64 {
	return atomic.LoadUint64(&pbm.stats[perfMap][cpu][eventType].Count)
}

// getEventBytes is an internal function, it can segfault if its parameters are incorrect.
func (pbm *PerfBufferMonitor) getEventBytes(eventType EventType, perfMap string, cpu int) uint64 {
	return atomic.LoadUint64(&pbm.stats[perfMap][cpu][eventType].Bytes)
}

// getEventUsage is an internal function, it can segfault if its parameters are incorrect.
func (pbm *PerfBufferMonitor) getEventUsage(eventType EventType, perfMap string, cpu int) int64 {
	return atomic.LoadInt64(&pbm.stats[perfMap][cpu][eventType].Usage)
}

// getEventInQueue is an internal function, it can segfault if its parameters are incorrect.
func (pbm *PerfBufferMonitor) getEventInQueue(eventType EventType, perfMap string, cpu int) int64 {
	return atomic.LoadInt64(&pbm.stats[perfMap][cpu][eventType].InQueue)
}

// GetEventStats returns the number of received events of the specified type and resets the counter
func (pbm *PerfBufferMonitor) GetEventStats(eventType EventType, perfMap string, cpu int) PerfMapStats {
	var stats PerfMapStats
	var maps []string

	if eventType >= maxEventType {
		return stats
	}

	switch {
	case len(perfMap) == 0:
		for m := range pbm.stats {
			maps = append(maps, m)
		}
		break
	case pbm.stats[perfMap] != nil:
		maps = append(maps, perfMap)
	}

	for _, m := range maps {

		switch {
		case cpu == -1:
			for i := range pbm.stats[m] {
				stats.Count += pbm.getEventCount(eventType, perfMap, i)
				stats.Bytes += pbm.getEventBytes(eventType, perfMap, i)
				stats.Usage += pbm.getEventUsage(eventType, perfMap, i)
				stats.InQueue += pbm.getEventUsage(eventType, perfMap, i)
			}
			break
		case cpu >= 0:
			if pbm.cpuCount <= cpu {
				break
			}
			stats.Count += pbm.getEventCount(eventType, perfMap, cpu)
			stats.Bytes += pbm.getEventBytes(eventType, perfMap, cpu)
			stats.Usage += pbm.getEventUsage(eventType, perfMap, cpu)
			stats.InQueue += pbm.getEventUsage(eventType, perfMap, cpu)
		}

	}
	return stats
}

// getAndResetEventCount is an internal function, it can segfault if its parameters are incorrect.
func (pbm *PerfBufferMonitor) getAndResetEventCount(eventType EventType, perfMap string, cpu int) uint64 {
	return atomic.SwapUint64(&pbm.stats[perfMap][cpu][eventType].Count, 0)
}

// getAndResetEventBytes is an internal function, it can segfault if its parameters are incorrect.
func (pbm *PerfBufferMonitor) getAndResetEventBytes(eventType EventType, perfMap string, cpu int) uint64 {
	return atomic.SwapUint64(&pbm.stats[perfMap][cpu][eventType].Bytes, 0)
}

// getAndResetEventUsage is an internal function, it can segfault if its parameters are incorrect.
func (pbm *PerfBufferMonitor) getAndResetEventUsage(eventType EventType, perfMap string, cpu int) int64 {
	return atomic.SwapInt64(&pbm.stats[perfMap][cpu][eventType].Usage, 0)
}

// getAndResetEventInQueue is an internal function, it can segfault if its parameters are incorrect.
func (pbm *PerfBufferMonitor) getAndResetEventInQueue(eventType EventType, perfMap string, cpu int) int64 {
	return atomic.SwapInt64(&pbm.stats[perfMap][cpu][eventType].InQueue, 0)
}

// GetAndResetEventStats returns the number of received events of the specified type and resets the counter
func (pbm *PerfBufferMonitor) GetAndResetEventStats(eventType EventType, perfMap string, cpu int) PerfMapStats {
	var stats PerfMapStats
	var maps []string

	if eventType >= maxEventType {
		return stats
	}

	switch {
	case len(perfMap) == 0:
		for m := range pbm.stats {
			maps = append(maps, m)
		}
		break
	case pbm.stats[perfMap] != nil:
		maps = append(maps, perfMap)
	}

	for _, m := range maps {

		switch {
		case cpu == -1:
			for i := range pbm.stats[m] {
				stats.Count += pbm.getAndResetEventCount(eventType, perfMap, i)
				stats.Bytes += pbm.getAndResetEventBytes(eventType, perfMap, i)
				stats.Usage += pbm.getAndResetEventUsage(eventType, perfMap, i)
				stats.InQueue += pbm.getAndResetEventInQueue(eventType, perfMap, i)
			}
			break
		case cpu >= 0:
			if pbm.cpuCount <= cpu {
				break
			}
			stats.Count += pbm.getAndResetEventCount(eventType, perfMap, cpu)
			stats.Bytes += pbm.getAndResetEventBytes(eventType, perfMap, cpu)
			stats.Usage += pbm.getAndResetEventUsage(eventType, perfMap, cpu)
			stats.InQueue += pbm.getAndResetEventInQueue(eventType, perfMap, cpu)
		}

	}
	return stats
}

// CountLostEvent adds `count` to the counter of lost events
func (pbm *PerfBufferMonitor) CountLostEvent(count uint64, m *manager.PerfMap, cpu int) {
	// sanity check
	if (pbm.readLostEvents[m.Name] == nil) || (len(pbm.readLostEvents[m.Name]) <= cpu) {
		return
	}
	atomic.AddUint64(&pbm.readLostEvents[m.Name][cpu], count)
}

// CountEventType adds `count` to the counter of received events of the specified type
func (pbm *PerfBufferMonitor) CountEvent(eventType EventType, count uint64, size uint64, m *manager.PerfMap, cpu int) {
	// sanity check
	if (pbm.stats[m.Name] == nil) || (len(pbm.stats[m.Name]) <= cpu) || (len(pbm.stats[m.Name][cpu]) <= int(eventType)) {
		return
	}

	atomic.AddUint64(&pbm.stats[m.Name][cpu][eventType].Count, count)
	atomic.AddUint64(&pbm.stats[m.Name][cpu][eventType].Bytes, size)

	if pbm.probe.config.PerfBufferMonitor {
		atomic.AddInt64(&pbm.stats[m.Name][cpu][eventType].Usage, -int64(size))
		atomic.AddInt64(&pbm.stats[m.Name][cpu][eventType].InQueue, -int64(count))
	}
}

func (pbm *PerfBufferMonitor) sendEventsAndBytesReadStats(client *statsd.Client) error {
	for m := range pbm.stats {
		for cpu := range pbm.stats[m] {
			for eventType := range pbm.stats[m][cpu] {
				evtType := EventType(eventType)
				tags := []string{
					fmt.Sprintf("map:%s", m),
					fmt.Sprintf("cpu:%d", cpu),
					fmt.Sprintf("event_type:%s", evtType),
				}

				if err := client.Count(MetricPerfBufferEventsRead, int64(pbm.getAndResetEventCount(evtType, m, cpu)), tags, 1.0); err != nil {
					return err
				}

				if err := client.Count(MetricPerfBufferBytesRead, int64(pbm.getAndResetEventBytes(evtType, m, cpu)), tags, 1.0); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (pbm *PerfBufferMonitor) sendLostEventsReadStats(client *statsd.Client) error {
	for m := range pbm.readLostEvents {
		var total int64
		perCPU := map[int]int64{}

		for cpu := range pbm.readLostEvents[m] {
			tags := []string{
				fmt.Sprintf("map:%s", m),
				fmt.Sprintf("cpu:%d", cpu),
			}
			count := int64(pbm.getAndResetReadLostCount(m, cpu))
			if err := client.Count(MetricPerfBufferLostRead, count, tags, 1.0); err != nil {
				return err
			}

			total += count
			perCPU[cpu] += count
		}

		if total > 0 && !pbm.probe.config.PerfBufferMonitor {
			pbm.probe.DispatchCustomEvent(
				NewEventLostReadEvent(m, perCPU, time.Now()),
			)
		}
	}
	return nil
}

func (pbm *PerfBufferMonitor) collectAndSendKernelStats(client *statsd.Client) error {
	var (
		id          uint32
		stats, zero PerfMapStats
		iterator    *lib.MapIterator
		tags        []string
	)

	// loop through the statistics buffers of each perf map
	for perfMapName, statsMaps := range pbm.perfBufferStatsMaps {
		// select the current statistics buffer in use
		statsMap := statsMaps[1-pbm.activeMapIndex]

		// total and perEventPerCPU are used for alerting
		var total uint64
		perEventPerCPU := map[string]map[int]uint64{}

		// loop through all the values of the active buffer
		iterator = statsMap.Iterate()
		for iterator.Next(&id, &stats) {
			// compute cpu and event type from id
			cpu := int(id / uint32(maxEventType))
			evtType := EventType(id % uint32(maxEventType))

			// make sure perEventPerCPU is properly initialized
			if _, ok := perEventPerCPU[evtType.String()]; !ok {
				perEventPerCPU[evtType.String()] = map[int]uint64{}
			}

			// sanity checks:
			//   - check if the computed cpu id is below the current cpu count
			//   - check if we collect some data on the provided perf map
			//   - check if the computed event id is below the current max event id
			if (pbm.stats[perfMapName] == nil) || (len(pbm.stats[perfMapName]) <= cpu) || (len(pbm.stats[perfMapName][cpu]) <= int(evtType)) {
				continue
			}

			// remove the entry from the kernel
			if err := statsMap.Put(&id, &zero); err != nil {
				return err
			}

			// prepare metrics tags
			tags = []string{
				fmt.Sprintf("map:%s", perfMapName),
				fmt.Sprintf("cpu:%d", cpu),
				fmt.Sprintf("event_type:%s", evtType),
			}

			if err := pbm.sendKernelStats(client, stats, tags, perfMapName, cpu, evtType); err != nil {
				return err
			}

			total += stats.Lost
			perEventPerCPU[evtType.String()][cpu] += stats.Lost
		}
		if iterator.Err() != nil {
			return errors.Wrapf(iterator.Err(), "failed to dump statistics buffer %d of map %s", 1-pbm.activeMapIndex, perfMapName)
		}

		// send an alert if events were lost
		if total > 0 {
			pbm.probe.DispatchCustomEvent(
				NewEventLostWriteEvent(perfMapName, perEventPerCPU, time.Now()),
			)
		}
	}
	return nil
}

func (pbm *PerfBufferMonitor) sendKernelStats(client *statsd.Client, stats PerfMapStats, tags []string, perfMapName string, cpu int, evtType EventType) error {
	if err := client.Count(MetricPerfBufferEventsWrite, int64(stats.Count), tags, 1.0); err != nil {
		return err
	}

	if err := client.Count(MetricPerfBufferBytesWrite, int64(stats.Bytes), tags, 1.0); err != nil {
		return err
	}

	if err := client.Count(MetricPerfBufferLostWrite, int64(stats.Lost), tags, 1.0); err != nil {
		return err
	}

	// update usage metric
	newUsage := atomic.AddInt64(&pbm.stats[perfMapName][cpu][evtType].Usage, int64(stats.Bytes))
	newInQueue := atomic.AddInt64(&pbm.stats[perfMapName][cpu][evtType].InQueue, int64(stats.Count))

	// There is a race condition when the system is under pressure: between the time we read the perf buffer stats map
	// and the time we reach this point, the kernel might have written more events in the perf map, and those events
	// might have already been read in user space. In that case, usage will yield a negative value. In that case, set
	// the map usage to 0 as it makes more sense than a negative value.
	usage := math.Max(float64(newUsage)/pbm.perfBufferSize[perfMapName], 0)
	if err := client.Gauge(MetricPerfBufferUsage, usage*100, tags, 1.0); err != nil {
		return err
	}

	// There is a race condition when the system is under pressure: between the time we read the perf buffer stats map
	// and the time we reach this point, the kernel might have written more events in the perf map, and those events
	// might have already been read in user space. In that case, usage will yield a negative value. In that case, set
	// the amount of queued events to 0 as it makes more sens than a negative value.
	if err := client.Count(MetricPerfBufferInQueue, int64(math.Max(float64(newInQueue), 0)), tags, 1.0); err != nil {
		return err
	}
	return nil
}

func (pbm *PerfBufferMonitor) SendStats(client *statsd.Client) error {
	if pbm.probe.config.PerfBufferMonitor {
		if err := pbm.collectAndSendKernelStats(client); err != nil {
			return err
		}
	}

	if err := pbm.sendEventsAndBytesReadStats(client); err != nil {
		return err
	}

	if err := pbm.sendLostEventsReadStats(client); err != nil {
		return err
	}

	// Update the active statistics map id
	if err := pbm.bufferSelector.Put(ebpf.BufferSelectorPerfBufferMonitorKey, 1-pbm.activeMapIndex); err != nil {
		return err
	}
	atomic.SwapUint32(&pbm.activeMapIndex, 1-pbm.activeMapIndex)
	return nil
}
