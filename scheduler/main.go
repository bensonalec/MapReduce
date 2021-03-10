package main

import (
        "bufio"
        "fmt"
        "net"
		"strings"
		// "time"
)

var pool []net.Conn
var activePool []net.Conn
var jobs []string

var CONNECTION_PACKET string = "HEADER:CONN||BODY:"
var ACK_PACKET string =  "HEADER:ACK||BODY:"
var DATA_PACKET string = "HEADER:DATA||BODY:"
var DISCON_PACKET string = "HEADER:STOP||BODY:"

var toSum string = "[2,4,6,8]"

func main() {
	jobs = append(jobs,toSum)
	go assignJob()

	listenForNewConnection()
}

func assignJob() {
	for {
		// fmt.Println("Handing out jobs...")
		for _,job := range jobs {
			for _,i := range pool {

				// fmt.Println(DATA_PACKET+job)
				sendMessage(i,DATA_PACKET+job)
			}
		
		}

	}
}

func removeFromPool(c net.Conn) {
	//first, get the index of c in the array
	foundInd := 0
	for ind,i := range(pool) {
		if(i == c) {
			foundInd = ind
		}
	}
	fmt.Println("Removed from Pool:",c)
	pool = append(pool[:foundInd], pool[foundInd+1:]...)

}

func parsePacket(received string, c net.Conn) string {
	packetSplit := strings.Split(received,"||")
	headerContents := strings.Split(packetSplit[0],":")[1]
	bodyContents := strings.Split(packetSplit[1],":")[1]
	if (headerContents == "CONN") {
		pool = append(pool, c)
		fmt.Println("Added to pool",pool)
		return ACK_PACKET
	} else if (headerContents == "ACK") {
		return "N/A"
	} else if (headerContents == "DATA") {
		//read the data in the body into an array of integers
		fmt.Println(bodyContents)
		return ACK_PACKET
	} else if (headerContents == "STOP") {
		removeFromPool(c)
		c.Close()
		return "STOP"
	}
	return "N/A"
}

func sendMessage(c net.Conn, response string) {
	c.Write([]byte(response + "\n"))
}

func readMessage(c net.Conn) string {
	netData, err := bufio.NewReader(c).ReadString('\n')
	if err != nil {
			fmt.Println(err)
			return "Issue"
	}

	return netData
}


func waitForMessages(c net.Conn) {
	for {
		netData := readMessage(c)
		response := parsePacket(netData,c)
		

		if(response == "STOP") {
			return
		} else if (response != "N/A") {
			sendMessage(c,response)
		} else {
			sendMessage(c,response)
		}
		
	}

}

func listenForNewConnection() {
	l, err := net.Listen("tcp", ":8090")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer l.Close()

	for {
		//accept a new connection
		c, err := l.Accept()
		if err != nil {
				fmt.Println(err)
				return
		}
		//add that connection to the worker pool
		//
		go waitForMessages(c)
	}

}

func sendToAddress(address string, message string) string {
	CONNECT := address
	c, err := net.Dial("tcp", CONNECT)
	if err != nil {
			fmt.Println(err)
			return "Something went wrong..."
	}

	fmt.Fprintf(c, message+"\n")

	received, _ := bufio.NewReader(c).ReadString('\n')
	fmt.Print("->: " + received)
	c.Close()
	return received
}

