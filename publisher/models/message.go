package models

type Message struct {
	ID   string      `json:"id"`
	Data interface{} `json:"data"`
}
