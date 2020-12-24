// +build linux_bpf

package http

import (
	"fmt"

	"C"

	"time"

	ddebpf "github.com/DataDog/datadog-agent/pkg/ebpf"
	"github.com/DataDog/datadog-agent/pkg/network/ebpf/probes"
	"github.com/DataDog/ebpf/manager"
)

// Monitor is responsible for:
// * Creating a raw socket and attaching an eBPF filter to it;
// * Polling a perf buffer that contains notifications about HTTP transaction batches ready to be read;
// * Querying these batches by doing a map lookup;
// * Aggregating and emitting metrics based on the received HTTP transactions;
type Monitor struct {
	handler func([]httpTX)

	batchManager  *batchManager
	perfMap       *manager.PerfMap
	perfHandler   *ddebpf.PerfHandler
	telemetry     *telemetry
	pollRequests  chan chan struct{}
	closeFilterFn func()
	statkeeper    *httpStatKeeper
}

// NewMonitor returns a new Monitor instance
func NewMonitor(m *manager.Manager, h *ddebpf.PerfHandler, closeFilterFn func()) (*Monitor, error) {
	batchMap, _, err := m.GetMap(string(probes.HttpBatchesMap))
	if err != nil {
		return nil, err
	}

	batchStateMap, _, err := m.GetMap(string(probes.HttpBatchStateMap))
	if err != nil {
		return nil, err
	}

	notificationMap, _, _ := m.GetMap(string(probes.HttpNotificationsMap))
	numCPUs := int(notificationMap.ABI().MaxEntries)

	pm, found := m.GetPerfMap(string(probes.HttpNotificationsMap))
	if !found {
		return nil, fmt.Errorf("unable to find perf map %s", probes.HttpNotificationsMap)
	}

	return &Monitor{
		batchManager:  newBatchManager(batchMap, batchStateMap, numCPUs),
		perfMap:       pm,
		perfHandler:   h,
		telemetry:     newTelemetry(),
		pollRequests:  make(chan chan struct{}),
		closeFilterFn: closeFilterFn,
		statkeeper:    newHTTPStatkeeper(),
	}, nil
}

// Start consuming HTTP events
func (http *Monitor) Start() error {
	if http == nil {
		return nil
	}

	if err := http.perfMap.Start(); err != nil {
		return fmt.Errorf("error starting perf map: %s", err)
	}

	go func() {
		report := time.NewTicker(30 * time.Second)
		defer report.Stop()
		for {
			select {
			case data, ok := <-http.perfHandler.DataChannel:
				if !ok {
					return
				}

				// The notification we read from the perf ring tells us which HTTP batch of transactions is ready to be consumed
				notification := toHTTPNotification(data)
				transactions, err := http.batchManager.GetTransactionsFrom(notification)
				http.process(transactions, err)
			case _, ok := <-http.perfHandler.LostChannel:
				if !ok {
					return
				}

				http.process(nil, errLostBatch)
			case reply := <-http.pollRequests:
				transactions := http.batchManager.GetPendingTransactions()
				http.process(transactions, nil)
				reply <- struct{}{}
			case <-report.C:
				transactions := http.batchManager.GetPendingTransactions()
				http.process(transactions, nil)
			}
		}
	}()

	return nil
}

// Sync HTTP data between userspace and kernel space
func (http *Monitor) Sync() {
	reply := make(chan struct{}, 1)
	defer close(reply)

	// TODO: Add logic to ensure this won't deadlock during termination
	http.pollRequests <- reply
	<-reply
}

// GetHTTPStats returns a map of HTTP stats stored in the following format:
// [source, dest tuple] -> [request path] -> RequestStats object
func (http *Monitor) GetHTTPStats() map[Key]map[string]RequestStats {
	http.Sync()

	if http.statkeeper == nil {
		return nil
	}
	return http.statkeeper.GetAndResetAllStats()
}

func (http *Monitor) GetStats() map[string]interface{} {
	currentTime, telemetryData := http.telemetry.getStats()
	return map[string]interface{}{
		"current_time": currentTime,
		"telemetry":    telemetryData,
	}
}

// Stop HTTP monitoring
func (http *Monitor) Stop() {
	if http == nil {
		return
	}

	http.closeFilterFn()
	_ = http.perfMap.Stop(manager.CleanAll)
	http.perfHandler.Stop()

	if http.statkeeper != nil {
		http.statkeeper.Close()
	}
}

func (http *Monitor) process(transactions []httpTX, err error) {
	http.telemetry.aggregate(transactions, err)

	if http.handler != nil && len(transactions) > 0 {
		http.statkeeper.Process(transactions)
	}
}
