package main

import (
        "bufio"
        "fmt"
        "net"
		"strings"
		// "time"
)

var CONNECTION_PACKET string = "HEADER:CONN||BODY:"
var ACK_PACKET string =  "HEADER:ACK||BODY:"
var DATA_PACKET string = "HEADER:DATA||BODY:"
var DISCON_PACKET string = "HEADER:STOP||BODY:"

type node struct {
	name string
	connection net.Conn
	next *node
}

var pool []net.Conn
var nodeHead node = node{name:"Unused"}
var nodeTail node = nodeHead
var jobs []string


var toSum string = "[2,4,6,8]"

func main() {
	if(nodeHead.name != "Unused") {
		fmt.Println(nodeHead)
	} else {
		fmt.Println("No head")
	}
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

func addToPool(c net.Conn,name string) {
	fmt.Print(name)
	if(nodeHead == nodeTail) {
		//now set the next node to be this node
		//head is always empty?
		//so maybe append to tail
		var newNode = &node{name:strings.TrimSpace(name),connection:c}
		nodeHead.next = newNode
		nodeTail = *nodeHead.next
		// fmt.Println("FSOSFSSF")
		// fmt.Println("No head")
	}  else {
		fmt.Println("Appending")
		var newNode = &node{name:strings.TrimSpace(name),connection:c}
		var currentNode = nodeHead

		for(currentNode.next != nil) {
			fmt.Println("Node Find:",currentNode.name)
			currentNode = *currentNode.next
		}
		
		currentNode.next = newNode
		nodeTail = *currentNode.next
	}
	printNodes()
} 

func printNodes() {
	var currentNode = nodeHead
	fmt.Println("Printing List")
	fmt.Println(nodeTail.name)
	
	for(currentNode.next != nil) {
		fmt.Println("Node:",currentNode.name)
		currentNode = *currentNode.next
	}
	fmt.Println("Node:",currentNode.name)
	currentNode = *currentNode.next
	fmt.Println("Node:",currentNode.name)

}

func parsePacket(received string, c net.Conn) string {
	packetSplit := strings.Split(received,"||")
	headerContents := strings.Split(packetSplit[0],":")[1]
	bodyContents := strings.Split(packetSplit[1],":")[1]
	if (headerContents == "CONN") {
		// pool = append(pool, c)
		// fmt.Println("Added to pool",pool)
		addToPool(c,bodyContents)
		return ACK_PACKET
	} else if (headerContents == "ACK") {
		return "N/A"
	} else if (headerContents == "DATA") {
		//read the data in the body into an array of integers
		fmt.Println("Body:",bodyContents)
		//remove that problem from the problem queue
		return ACK_PACKET
	} else if (headerContents == "STOP") {
		fmt.Println("STOPPED!")
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

