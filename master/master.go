package master

import (
	"fmt"
	"map-reduce-standalone/node"
	"map-reduce-standalone/worker"
)

type Master struct {
	addChan chan *Job
	now     jobList
}

type jobList struct {
	job  *Job
	pre  *jobList
	next *jobList
}

func NewMaster() *Master {
	header := &jobList{
		job:  nil,
		pre:  nil,
		next: nil,
	}
	addChan := make(chan *Job)
	return &Master{
		addChan: addChan,
		now:     *header,
	}
}

func (m *Master) Add(job *Job) {
	addJob := &jobList{
		job: job,
	}
	m.now.next = addJob
	addJob.pre = &m.now
	m.now = *addJob
	m.addChan <- addJob.job
}

func (m *Master) Run() {
	for {
		newJob := <-m.addChan
		go m.runJob(newJob)
	}
}

func (m *Master) runJob(job *Job) {
	fmt.Printf("job starting: %s\n", job.masterNode.Id)
	// do map worker
	for i := 0; i < len(job.mapNodes); i++ {
		mapWorker := worker.NewMapWorkerNode(job.masterNode, job.mapNodes[i], len(job.reduceNodes))
		go mapWorker.Run()
	}

	for i := 0; i < len(job.mapNodes); i++ {
		for {
			message := <-job.masterNode.MessageChan
			if message.MessageType == node.MapDone {
				fmt.Printf("map worker is done: %s\n", message.Content)
				job.ChangeMapStates(message.Content, MapDone)
				break
			}
		}
	}

	// do reduce worker
	for i := 0; i < len(job.reduceNodes); i++ {
		reduceWorker := worker.NewReduceWorkerNode(job.masterNode, job.reduceNodes[i])
		go reduceWorker.Run()
	}

	for i := 0; i < len(job.reduceNodes); i++ {
		for {
			message := <-job.masterNode.MessageChan
			if message.MessageType == node.ReduceDone {
				fmt.Printf("reduce worker is done: %s\n", message.Content)
				job.ChangeReduceStates(message.Content, ReduceDone)
				break
			}
		}
	}

	// end
}
