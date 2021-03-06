package master

import (
	"map-reduce-standalone/node"
	"map-reduce-standalone/split"
	"os"
	"strings"
)

type Job struct {
	masterNode  *node.Node
	filePath    string
	mapNodes    []*node.Node
	reduceNodes []*node.Node
	state       [][]State
}

type State int

const (
	Start = iota
	MapDone
	MapError
	ReduceDone
	ReduceError
)

func NewJob(filePath string, mapNodes []*node.Node, reduceNodes []*node.Node) *Job {
	if len(mapNodes) == 0 || len(reduceNodes) == 0 {
		panic("map worker and reduce worker can not be nil")
	}

	newState := make([][]State, len(mapNodes))
	for i := range newState {
		newState[i] = make([]State, len(reduceNodes))
	}

	return &Job{
		masterNode:  node.NewNode(),
		filePath:    filePath,
		mapNodes:    mapNodes,
		reduceNodes: reduceNodes,
		state:       newState,
	}
}

func (j *Job) ChangeState(mapId string, reduceId string, state State) {
	mapIndex, reduceIndex := j.getIndex(mapId, reduceId, false, false)
	j.state[mapIndex][reduceIndex] = state
}

func (j *Job) ChangeMapStates(mapId string, state State) {
	mapIndex, _ := j.getIndex(mapId, "", true, false)
	states := j.state[mapIndex]

	for index := range states {
		states[index] = state
	}
}

func (j *Job) ChangeReduceStates(reduceId string, state State) {
	_, reduceIndex := j.getIndex("", reduceId, false, true)
	for index := range j.state {
		j.state[index][reduceIndex] = state
	}
}

func (j *Job) GetState(mapId string, reduceId string) State {
	mapIndex, reduceIndex := j.getIndex(mapId, reduceId, false, false)
	return j.state[mapIndex][reduceIndex]
}

func (j *Job) getIndex(mapId string, reduceId string, onlyMap bool, onlyReduce bool) (int, int) {
	mapIndex := -1

	if !onlyReduce {
		for index, value := range j.mapNodes {
			if strings.Compare(value.Id, mapId) == 0 {
				mapIndex = index
			}
		}

		if onlyMap {
			if mapIndex == -1 {
				panic("map not found")
			}
			return mapIndex, 0
		}
	}

	reduceIndex := -1

	if !onlyMap {
		for index, value := range j.reduceNodes {
			if strings.Compare(value.Id, reduceId) == 0 {
				reduceIndex = index
			}
		}

		if onlyReduce {
			if reduceIndex == -1 {
				panic("reduce not found")
			}
			return 0, reduceIndex
		}
	}

	if mapIndex == -1 || reduceIndex == -1 {
		panic("map or reduce not found")
	}

	return mapIndex, reduceIndex
}

func (j *Job) SplitInput(splitFunc split.Split) {
	file, err := os.Open(j.filePath)
	if err != nil {
		panic(err)
	}
	splitFunc.Split(file)
}
