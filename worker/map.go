package worker

import (
	"fmt"
	"map-reduce-standalone/node"
	"time"
)

type MapWorkerNode struct {
	masterNode   *node.Node
	node         *node.Node
	temFilePaths []string
	reduceNum    int
}

type MapWorker interface {
	Map()
}

func NewMapWorkerNode(masterNode *node.Node, node *node.Node, reduceNum int) *MapWorkerNode {
	temFilePaths := make([]string, reduceNum)

	for i := 0; i < reduceNum; i++ {
		temFilePaths[i] = fmt.Sprintf("%s-%s-%d", masterNode.Id, node.Id, i)
	}

	return &MapWorkerNode{
		masterNode:   masterNode,
		node:         node,
		temFilePaths: temFilePaths,
		reduceNum:    reduceNum,
	}
}

func (m *MapWorkerNode) Run() {
	fmt.Printf("map worker started: %s\n", m.node.Id)
	done := make(chan int)
	go m.work(done)
DO:
	for {
		select {
		case <-done:
			break DO
		case <-m.node.MessageChan:
			continue
		}
	}
	m.masterNode.MessageChan <- &node.Message{
		MessageType: node.MapDone,
		Content:     m.node.Id,
	}
}

func (m *MapWorkerNode) work(done chan int) {
	time.Sleep(2 * time.Second)
	done <- 1
}
