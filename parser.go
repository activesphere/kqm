package main

import (
	"bytes"
	"encoding/binary"
	"fmt"

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
			return nil, fmt.Errorf("Error parsing group message key. Details: %s", err)
		}
		topic, err = readString(buf)
		if err != nil {
			return nil, fmt.Errorf("Error parsing topic from key. Details: %s", err)
		}
		err = binary.Read(buf, binary.BigEndian, &partition)
		if err != nil {
			return nil, fmt.Errorf("Error parsing partition from key. Details: %s", err)
		}
	case 2:
		return nil, err
	default:
		return nil, fmt.Errorf("Unknown version error in message key. Details: %s", err)
	}

	buf = bytes.NewBuffer(message.Value)
	err = binary.Read(buf, binary.BigEndian, &valver)
	if err != nil {
		return nil, nil
	}
	err = binary.Read(buf, binary.BigEndian, &offset)
	if err != nil {
		return nil, fmt.Errorf("Error reading offset from message value. Details: %s", err)
	}
	_, err = readString(buf)
	if err != nil {
		return nil, fmt.Errorf("Error reading metadata(omitted) from message value. Details: %s", err)
	}
	err = binary.Read(buf, binary.BigEndian, &timestamp)
	if err != nil {
		return nil, fmt.Errorf("Error reading timestamp from message value. Details: %s", err)
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
