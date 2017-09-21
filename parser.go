package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"

	"github.com/Shopify/sarama"
)

// Burrow-based Consumer Offset Message parser function.
func formatConsumerMessage(message *sarama.ConsumerMessage) (*PartitionOffset, error) {
	readString := func(buf *bytes.Buffer) (string, error) {
		var strlen uint16
		err := binary.Read(buf, binary.BigEndian, &strlen)
		if err != nil {
			return "", err
		}
		strbytes := make([]byte, strlen)
		n, err := buf.Read(strbytes)
		if (err != nil) || (n != int(strlen)) {
			return "", fmt.Errorf("string underflow")
		}
		return string(strbytes), nil
	}

	var (
		keyver, valver    uint16
		group, topic      string
		partition         uint32
		offset, timestamp uint64
	)

	buf := bytes.NewBuffer(message.Key)
	err := binary.Read(buf, binary.BigEndian, &keyver)
	switch keyver {
	case 0, 1:
		group, err = readString(buf)
		if err != nil {
			return nil, err
		}
		topic, err = readString(buf)
		if err != nil {
			return nil, err
		}
		err = binary.Read(buf, binary.BigEndian, &partition)
		if err != nil {
			return nil, err
		}
	case 2:
		log.Println("Version 2 is unsupported.")
		return nil, err
	default:
		log.Println("Unknown Version Key:", keyver)
		return nil, err
	}

	buf = bytes.NewBuffer(message.Value)
	err = binary.Read(buf, binary.BigEndian, &valver)
	if (err != nil) || ((valver != 0) && (valver != 1)) {
		return nil, err
	}
	err = binary.Read(buf, binary.BigEndian, &offset)
	if err != nil {
		return nil, err
	}
	_, err = readString(buf)
	if err != nil {
		return nil, err
	}
	err = binary.Read(buf, binary.BigEndian, &timestamp)
	if err != nil {
		return nil, err
	}

	partitionOffset := &PartitionOffset{
		Topic:     topic,
		Partition: int32(partition),
		Group:     group,
		Timestamp: int64(timestamp),
		Offset:    int64(offset),
	}
	return partitionOffset, nil
}
