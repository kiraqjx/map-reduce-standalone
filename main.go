package main

import (
	"map-reduce-standalone/master"
	"map-reduce-standalone/node"
	"time"
)

func main() {
	masterIns := master.NewMaster()
	go masterIns.Run()

	// create map node
	mapNodes := make([]*node.Node, 5)
	for i := 0; i < 5; i++ {
		node := node.NewNode()
		mapNodes[i] = node
	}

	// create reduce node
	reduceNodes := make([]*node.Node, 3)
	for i := 0; i < 3; i++ {
		node := node.NewNode()
		reduceNodes[i] = node
	}

	job := master.NewJob("./test_data/test.text", mapNodes, reduceNodes)
	masterIns.Add(job)

	time.Sleep(6 * time.Second)
}
