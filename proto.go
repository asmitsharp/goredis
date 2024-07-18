package main

import (
	"bytes"
	"fmt"
	"io"
	"log"

	"github.com/tidwall/resp"
)

const (
	CommandSet = "SET"
	CommandGet = "GET"
)

type Command interface{}

type SetCommand struct {
	key, value []byte
}

type GetCommand struct {
	key, value []byte
}

func parseCommand(raw string) (Command, error) {
	rd := resp.NewReader(bytes.NewBufferString(raw))
	for {
		v, _, err := rd.ReadValue()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		for _, value := range v.Array() {
			switch value.String() {
			case CommandGet:
				if len(v.Array()) != 2 {
					return nil, fmt.Errorf("invalid variables for GET command")
				}
				cmd := GetCommand{
					key: v.Array()[1].Bytes(),
				}
				return cmd, nil
			case CommandSet:
				if len(v.Array()) != 3 {
					return nil, fmt.Errorf("invalid variables for SET command")
				}
				cmd := SetCommand{
					key:   v.Array()[1].Bytes(),
					value: v.Array()[2].Bytes(),
				}
				return cmd, nil
			}
		}
	}
	return nil, fmt.Errorf("invalid or unknown command recieved: %s", raw)
}
