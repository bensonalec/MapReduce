package main

import (
        "bufio"
        "fmt"
        "net"
		// "time"
		"os"
		"os/signal"
		"syscall"
		"strings"
		"strconv"
)

var CONNECTION_PACKET string = "HEADER:CONN||BODY:"
var ACK_PACKET string =  "HEADER:ACK||BODY:"
var DATA_PACKET string = "HEADER:DATA||BODY:"
var DISCON_PACKET string = "HEADER:STOP||BODY:"

var nodeName string = "1"

func main() {
	sendToAddress("127.0.0.1:8090",CONNECTION_PACKET)
}

func sendMessage(c net.Conn, message string) {
	fmt.Fprintf(c, message+"\n")
}

func readMessage(c net.Conn) string {
	received, _ := bufio.NewReader(c).ReadString('\n')
	return received
}

func endSession(c net.Conn) {
	sendMessage(c,DISCON_PACKET)
	_ = readMessage(c)
	fmt.Println("Closed")
	c.Close()
}

func parsePacket(received string) string{
	packetSplit := strings.Split(received,"||")
	headerContents := strings.Split(packetSplit[0],":")[1]
	bodyContents := strings.Split(packetSplit[1],":")[1]
	fmt.Print("-> ", string(received))
	if (headerContents == "ACK") {
		fmt.Println("Acknowledged")
		return "N/A"
	} else if(headerContents == "DATA") {
		//read the data in the body into an array of integers
		//first, remove the brackets
		toSum := bodyContents[1:len(bodyContents)-2]
		toSumArray := strings.Split(toSum,",")
		var toSumIntArray []int
		for _,i := range toSumArray {
			toAdd, _ := strconv.Atoi(i)
			toSumIntArray = append(toSumIntArray,toAdd)
		}
		sum := 0
		for _,i := range toSumIntArray {
			sum += i
		}
		fmt.Println(toSumIntArray)
		return DATA_PACKET + strconv.Itoa(sum)
	}
	return "N/A"
}

func sendToAddress(address string, message string) string {
	//Establish Connection
	CONNECT := address
	c, err := net.Dial("tcp", CONNECT)
	if err != nil {
			fmt.Println(err)
			return "Something went wrong..."
	}

	//Set up kill on sigint
	nel := make(chan os.Signal)
    signal.Notify(nel, os.Interrupt, syscall.SIGTERM)
    go func() {
        <-nel
        endSession(c)
		os.Exit(1)
    }()

	//Send the connection packet
	sendMessage(c,message+nodeName)
	//Check that the response was properly acknowledged
	received := readMessage(c)
	// Parse the newly received packet
	_ = parsePacket(received)
	//Wait for instructions
	for {
		_ = readMessage(c)
		toSend := parsePacket(received)
		if(toSend != "N/A") {
			sendMessage(c,toSend)
		}
	}
}
