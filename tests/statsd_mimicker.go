package main

/*
	This is a UDP server implementation to mimick Statsd for testing purpose.
	This server will listen to the port passed as command line arg and print
	the result to stdout.
*/

import (
	"net"

	log "github.com/sirupsen/logrus"
)

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
		_, _, err := conn.ReadFromUDP(buffer)
		if err != nil {
			log.Errorln("Error reading from UDP: ", err)
			continue
		}
		log.Println("Received: ", string(buffer))
	}
}
