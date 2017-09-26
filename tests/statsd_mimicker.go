package main

/*
	This is a UDP server implementation to mimick Statsd for testing purpose.
	This server will listen to the port passed as command line arg and print
	the result to stdout.
*/

import (
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/activesphere/kqm/monitor"
	log "github.com/sirupsen/logrus"
)

func parseGauge(gauge string) (*monitor.PartitionOffset, error) {
	partOff := monitor.PartitionOffset{}

	var props []string

	props = strings.Split(gauge, ".")
	partOff.Group, partOff.Topic = props[2], props[3]

	props = strings.Split(strings.Trim(props[4], "|g"), ":")

	partition, err := strconv.Atoi(props[0])
	if err != nil {
		log.Errorln("Conversion from string to int failed for partition.")
		return nil, err
	}
	partOff.Partition = int32(partition)

	lag, err := strconv.Atoi(props[1])
	if err != nil {
		log.Errorln("Conversion from string to int failed for lag.")
		return nil, err
	}
	partOff.Offset = int64(lag)

	return &partOff, nil
}

func main() {
	serverAddr, err := net.ResolveUDPAddr("udp", ":8125")
	if err != nil {
		log.Fatalln("Error in resolving Addr:", err)
	}
	conn, err := net.ListenUDP("udp", serverAddr)
	if err != nil {
		log.Fatalln("Error in listening to UDP port.")
	}
	defer conn.Close()
	buffer := make([]byte, 1024)

	for {
		n, _, err := conn.ReadFromUDP(buffer)
		if err != nil {
			log.Errorln("Error reading from UDP: ", err)
			continue
		}

		partOff, err := parseGauge(string(buffer[:n]))
		if err != nil {
			os.Exit(1)
		}
		log.Printf("Group: %s, Topic: %s, Partn: %d, Lag: %d",
			partOff.Group, partOff.Topic, partOff.Partition, partOff.Offset)
	}
}
