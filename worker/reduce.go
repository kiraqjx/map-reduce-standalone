package worker

import (
	"fmt"
	"map-reduce-standalone/node"
	"time"
)

type ReduceWorkerNode struct {
	masterNode  *node.Node
	node        *node.Node
	temFilePath string
}

type ReduceWorker interface {
	Reduce()
}

func NewReduceWorkerNode(masterNode *node.Node, node *node.Node) *ReduceWorkerNode {
	temFilePath := node.Id

	return &ReduceWorkerNode{
		masterNode:  masterNode,
		node:        node,
		temFilePath: temFilePath,
	}
}

func (r *ReduceWorkerNode) Run() {
	fmt.Printf("reduce worker started: %s\n", r.node.Id)
	done := make(chan int)
	go r.work(done)
DO:
	for {
		select {
		case <-done:
			break DO
		case <-r.node.MessageChan:
			continue
		}
	}
	r.masterNode.MessageChan <- &node.Message{
		MessageType: node.ReduceDone,
		Content:     r.node.Id,
	}
}

func (r *ReduceWorkerNode) work(done chan int) {
	time.Sleep(2 * time.Second)
	done <- 1
}
