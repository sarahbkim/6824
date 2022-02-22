package mapreduce

import (
	"sync"
	"time"
)

// master
// 1 master
// master has healthchecks on workers wait 1 sec, health
// master checks if worker completes task on time (ie 10 secs)
// master is an RPC server
// master assigns M map tasks and R reduce tasks to workers
type Job struct {
	Map      MapFn
	Reduce   ReduceFn
	Filename string
}

func (j *Job) Run() {
	var wg sync.WaitGroup
	newMaster(&wg, 10, 10)
}

// TODO: make master a singleton?
type master struct {
	nWorkers int
	rWorkers int
	mapFn    MapFn
	reduceFn ReduceFn
}

// stops flow of exec until job is done
func newMaster(wg *sync.WaitGroup, n, r int) *master {
	wg.Add(1)
	defer wg.Done()

	// init workers

	// give tasks to workers

	done := make(chan bool)
	go healthcheck(done)

	// TODO: push to done if exec of master killed
	return &master{}
}

func healthcheck(done <-chan bool) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			// TODO: ping workers periodically
		}
	}
}

// m map tasks & r reduce tasks to assign
type worker struct {
	task chan task
}

func newWorker() *worker {
}

// health check
func (w *worker) ping() {

}

type state int

const (
	idle state = 0
	inprogress
	completed
)

type file struct {
	location string
	size     int // bytes?
}

type task interface {
	run()
}

type mapTask struct {
	state             state
	intermediateFiles map[string]file
	mapFn             MapFn
}

func (m *mapTask) run() {
}

// flushes in-mem buffered-pairs to disk
func (m *mapTask) flush() {
}

type reduceTask struct {
	state             state
	intermediateFiles map[string]file
	reduceFn          ReduceFn
}

func (r *reduceTask) run() {
}

// flushes in-mem buffered-pairs to disk
func (r *reduceTask) flush() {
}

// exported functions of mapreduce apps
type MapFn func(k, v string) (string, string)
type ReduceFn func(k string, vs []string) string
