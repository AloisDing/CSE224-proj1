package main

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net"
	"os"
	"sort"
	"strconv"
	"time"
)

const recordLength = 101

type ServerConfigs struct {
	Servers []struct {
		ServerId int    `yaml:"serverId"`
		Host     string `yaml:"host"`
		Port     string `yaml:"port"`
	} `yaml:"servers"`
}

func readServerConfigs(configPath string) ServerConfigs {
	f, err := ioutil.ReadFile(configPath)

	if err != nil {
		log.Fatalf("could not read config file %s : %v", configPath, err)
	}

	scs := ServerConfigs{}
	err = yaml.Unmarshal(f, &scs)

	return scs
}

func checkError(err error) {
	if err != nil {
		log.Panicln("Fatal error: %s", err.Error())
	}
}

func genEnd(num int, val int) []byte {
	record := make([]byte, num)
	for i, _ := range record {
		record[i] = byte(val)
	}
	return record
}

func listen(ch chan<- []byte, addr string) {
	l, err := net.Listen("tcp", addr)
	checkError(err)
	defer l.Close()
	time.Sleep(100 * time.Millisecond)
	for {
		conn, err := l.Accept()
		checkError(err)
		fmt.Println("Accepted connection from: ")
		fmt.Println(conn)
		go func(conn net.Conn, ch chan<- []byte) {
			defer conn.Close()
			var record []byte
			for {
				for {
					buffer := make([]byte, recordLength)
					bytes, err := conn.Read(buffer)
					if err != nil && err != io.EOF {
						log.Panicln("Fatal error: %s", err.Error())
					}
					if err == io.EOF {
						//conn.Close()
						return
					}
					record = append(record, buffer[0:bytes]...)
					if len(record) >= recordLength {
						break
					}
				}
				fullRecord := record[0:recordLength]
				record = record[recordLength:]
				ch <- fullRecord
			}
		}(conn, ch)
	}
}

func dial(serverId int, scs ServerConfigs, distribution map[int][][]byte) {
	time.Sleep(100 * time.Millisecond)
	for _, server := range scs.Servers {
		if server.ServerId != serverId {
			for {
				addr := server.Host + ":" + server.Port
				conn, err := net.Dial("tcp", addr)
				if err == nil {
					go func(conn net.Conn, Data [][]byte) {
						for _, record := range Data {
							record = append([]byte{byte(0)}, record...)
							conn.Write(record)
						}
						conn.Write(genEnd(recordLength, 1))
						conn.Close()
					}(conn, distribution[server.ServerId])
					break
				}
			}
		}
	}
}

func sortWrite(outPath string, result [][]byte) {
	sort.Slice(result, func(i, j int) bool { return string(result[i][:10]) < string(result[j][:10]) })

	outputFile, err := os.Create(outPath)
	checkError(err)

	for _, record := range result {
		outputFile.Write(record)
	}
	defer outputFile.Close()

	outputFile.Sync()
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if len(os.Args) != 5 {
		log.Fatal("Usage : ./netsort {serverId} {inputFilePath} {outputFilePath} {configFilePath}")
	}

	// What is my serverId
	serverId, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("Invalid serverId, must be an int %v", err)
	}
	fmt.Println("My server Id:", serverId)

	// Read server configs from file
	scs := readServerConfigs(os.Args[4])
	fmt.Println("Got the following server configs:", scs)

	numServers := len(scs.Servers)
	fmt.Printf("got in total %s servers\n", numServers)
	//handle the case when there is only one server(localsort)
	inputPath := os.Args[2]
	outputPath := os.Args[3]
	if numServers == 1 {
		readFile, err := os.Open(inputPath)
		checkError(err)
		defer readFile.Close()

		log.Printf("Sorting %s to %s\n", os.Args[1], os.Args[2])

		var result [][]byte

		for {
			buf := make([]byte, 100)
			n, err := readFile.Read(buf)
			if err != nil && err != io.EOF {
				log.Fatal(err)
			}
			if err == io.EOF {
				break
			}
			buf = buf[:n]
			result = append(result, buf)
		}

		sortWrite(outputPath, result)
		return
	}
	inputFile, err := os.Open(inputPath)
	checkError(err)
	defer inputFile.Close()

	//check if empty
	fi, err := inputFile.Stat()
	checkError(err)
	if fi.Size() == 0 {
		outputFile, err := os.Create(outputPath)
		checkError(err)
		defer outputFile.Close()
		return
	}

	//deciding which server the data belongs to
	distribution := make(map[int][][]byte)
	for {
		buf := make([]byte, 100)
		n, err := inputFile.Read(buf)
		if err != nil && err != io.EOF {
			log.Fatal(err)
		}
		if err == io.EOF {
			break
		}
		buf = buf[:n]
		for _, serv := range scs.Servers {
			ID := int(buf[0] >> (8 - int(math.Log2(float64(numServers)))))
			if ID == serv.ServerId {
				distribution[ID] = append(distribution[ID], buf)
			}
		}
	}

	ch := make(chan []byte)
	for _, server := range scs.Servers {
		if server.ServerId == serverId {
			addr := server.Host + ":" + server.Port
			go listen(ch, addr)
		}
	}
	go dial(serverId, scs, distribution)

	records := distribution[serverId]
	cnt := 0
	for {
		if cnt == numServers-1 {
			break
		}
		record := <-ch
		if record[0] == byte(1) {
			cnt++
		} else {
			records = append(records, record[1:])
		}
	}
	sortWrite(outputPath, records)
}
