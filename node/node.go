package node

import (
	"github.com/google/uuid"
)

type Node struct {
	Id          string
	MessageChan chan *Message
}

func NewNode() *Node {
	id := uuid.New()
	messageChan := make(chan *Message)

	return &Node{
		Id:          id.String(),
		MessageChan: messageChan,
	}
}

func (n *Node) SendHealthCheck() bool {
	message := &Message{
		MessageType: HealthCheck,
		Content:     "health check",
	}
	n.MessageChan <- message
	return true
}

func (n *Node) SendTemFilePath(content string) {
	message := &Message{
		MessageType: TemFilePath,
		Content:     content,
	}
	n.MessageChan <- message
}
