package node

type MessageType int

type Message struct {
	MessageType MessageType
	Content     string
}

const (
	HealthCheck = iota + 1
	TemFilePath
	MapDone
	ReduceDone
)
