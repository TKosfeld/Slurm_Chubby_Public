package main

import (
	"bufio"
	"chubbylock/api"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	concurrency   = 5
	lockPoolSize  = 100
	keepAliveFreq = 15000 * time.Millisecond
)

func main() {
	op := flag.String("op", "read", "Operation: read, write, or mixed")
	duration := flag.Duration("duration", 30*time.Second, "Test duration")
	rps := flag.Int("rps", 100, "Requests per second per client")
	output := flag.String("out", "op_latencies.csv", "Output CSV file")
	populate := flag.Bool("populate", true, "Whether to prepopulate locks (leader init)")
	flag.Parse()

	if *op != "read" && *op != "write" && *op != "mixed" {
		log.Fatalf("Invalid operation: %s. Must be 'read', 'write', or 'mixed'", *op)
	}

	servers, err := readReplicaList("replica_list.txt")
	if err != nil || len(servers) == 0 {
		log.Fatalf("Failed to read replica list: %v", err)
	}

	if *populate {
		prepopulateLocks(servers)
	}

	opLogFile, err := os.Create(*output)
	if err != nil {
		log.Fatalf("Failed to create %s: %v", *output, err)
	}
	defer opLogFile.Close()
	opWriter := csv.NewWriter(opLogFile)
	defer opWriter.Flush()
	opWriter.Write([]string{"ClientID", "Timestamp", "Operation", "Latency_us", "Success"})
	var opWriterMu sync.Mutex

	var (
		summaryMu sync.Mutex
		summary   = make(map[string][2]int)
	)

	log.Printf("Starting stress test: %d clients doing '%s' for %v", concurrency, *op, *duration)
	var wg sync.WaitGroup
	endTime := time.Now().Add(*duration)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go stressWorker(
			servers,
			fmt.Sprintf("client%d-%d", i, time.Now().UnixNano()),
			api.FilePath(fmt.Sprintf("/file%d", rand.Intn(lockPoolSize))),
			*op,
			time.Second/time.Duration(*rps),
			endTime,
			&wg,
			opWriter,
			&opWriterMu,
			summary,
			&summaryMu,
		)
	}

	wg.Wait()

	log.Println("Stress test completed.\n\nOperation Summary (per client):")
	summaryMu.Lock()
	for client, counts := range summary {
		log.Printf("%s - Success: %d, Fail: %d", client, counts[0], counts[1])
	}
	summaryMu.Unlock()
}

func connectToLeader(clientID string, servers []string) (*rpc.Client, error) {
	for _, addr := range shuffleServers(servers) {
		client, err := rpc.Dial("tcp", addr)
		if err != nil {
			continue
		}
		var sessResp api.InitSessionResponse
		sessReq := api.InitSessionRequest{ClientID: api.ClientID(clientID)}
		err = client.Call("Handler.InitSession", sessReq, &sessResp)
		if err != nil || sessResp.Error != nil {
			client.Close()
			continue
		}
		log.Printf("Client %s: Connected to leader at %s", clientID, addr)
		return client, nil
	}
	return nil, fmt.Errorf("Client %s: could not connect to any leader", clientID)
}

func stressWorker(
	servers []string,
	clientID string,
	lockPath api.FilePath,
	op string,
	interval time.Duration,
	endTime time.Time,
	wg *sync.WaitGroup,
	opWriter *csv.Writer,
	opWriterMu *sync.Mutex,
	summary map[string][2]int,
	summaryMu *sync.Mutex,
) {
	defer wg.Done()

	var clientMu sync.Mutex
	var clientPtr *rpc.Client

	client, err := connectToLeader(clientID, servers)
	if err != nil {
		log.Printf("Client %s: Could not connect to any leader", clientID)
		return
	}
	clientPtr = client

	go func() {
		ticker := time.NewTicker(keepAliveFreq)
		defer ticker.Stop()
		for {
			if time.Now().After(endTime.Add(5 * time.Second)) {
				return
			}
			clientMu.Lock()
			currentClient := clientPtr
			clientMu.Unlock()
			if currentClient == nil {
				time.Sleep(keepAliveFreq)
				continue
			}
			kaReq := api.KeepAliveRequest{
				ClientID: api.ClientID(clientID),
				Locks:    map[api.FilePath]api.LockMode{lockPath: api.FREE},
			}
			var kaResp struct{}
			err := currentClient.Call("Handler.KeepAlive", kaReq, &kaResp)
			if err != nil {
				log.Printf("Client %s: KeepAlive failed: %v", clientID, err)
				currentClient.Close()
				newClient, err := connectToLeader(clientID, servers)
				if err == nil {
					clientMu.Lock()
					clientPtr = newClient
					clientMu.Unlock()
				} else {
					log.Printf("Client %s: Reconnect failed", clientID)
				}
			}
			time.Sleep(keepAliveFreq)
		}
	}()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for now := range ticker.C {
		if now.After(endTime) {
			return
		}

		var currentClient *rpc.Client
		for {
			clientMu.Lock()
			currentClient = clientPtr
			clientMu.Unlock()
			if currentClient != nil {
				break
			}
			log.Printf("Client %s: Waiting for reconnection before ops", clientID)
			time.Sleep(keepAliveFreq)
		}

		start := time.Now()
		selectedOp := op
		if op == "mixed" {
			if rand.Intn(2) == 0 {
				selectedOp = "read"
			} else {
				selectedOp = "write"
			}
		}

		var opErr error
		for {
			clientMu.Lock()
			currentClient = clientPtr
			clientMu.Unlock()
			if currentClient == nil {
				time.Sleep(keepAliveFreq)
				continue
			}
			switch selectedOp {
			case "read":
				opErr = performRead(currentClient, clientID, lockPath)
			case "write":
				content := fmt.Sprintf("Data from %s at %s", clientID, time.Now().String())
				opErr = performWrite(currentClient, clientID, lockPath, content)
			}
			if opErr != nil && isConnectionError(opErr) {
				log.Printf("Client %s: Detected transient RPC error (%v). Retrying...", clientID, opErr)
				time.Sleep(keepAliveFreq)
				continue
			}
			break
		}

		elapsed := time.Since(start)
		timestamp := time.Now().Format(time.RFC3339Nano)
		latencyUs := fmt.Sprintf("%d", elapsed.Microseconds())
		success := "true"
		if opErr != nil {
			success = "false"
		}
		opWriterMu.Lock()
		opWriter.Write([]string{clientID, timestamp, selectedOp, latencyUs, success})
		opWriter.Flush()
		opWriterMu.Unlock()
		summaryMu.Lock()
		counts := summary[clientID]
		if opErr == nil {
			counts[0]++
		} else {
			counts[1]++
		}
		summary[clientID] = counts
		summaryMu.Unlock()
	}
}

func isConnectionError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "connection is shut down") ||
		strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "connection reset by peer") ||
		strings.Contains(msg, "EOF")
}

func performRead(client *rpc.Client, clientID string, lockPath api.FilePath) error {
	openReq := api.OpenLockRequest{ClientID: api.ClientID(clientID), Filepath: lockPath}
	openResp := api.OpenLockResponse{}
	if err := client.Call("Handler.OpenLock", openReq, &openResp); err != nil || openResp.Error != nil {
		return fmt.Errorf("OpenLock failed: %v %v", err, openResp.Error)
	}
	for {
		acquireReq := api.TryAcquireLockRequest{ClientID: api.ClientID(clientID), Filepath: lockPath, Mode: api.SHARED}
		acquireResp := api.TryAcquireLockResponse{}
		err := client.Call("Handler.TryAcquireLock", acquireReq, &acquireResp)
		if err != nil {
			return fmt.Errorf("TryAcquireLock failed: %v", err)
		}
		if acquireResp.IsSuccessful {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	readReq := api.ReadRequest{ClientID: api.ClientID(clientID), Filepath: lockPath}
	readResp := api.ReadResponse{}
	if err := client.Call("Handler.ReadContent", readReq, &readResp); err != nil {
		return fmt.Errorf("ReadContent failed: %v", err)
	}
	releaseReq := api.ReleaseLockRequest{ClientID: api.ClientID(clientID), Filepath: lockPath}
	releaseResp := api.ReleaseLockResponse{}
	return client.Call("Handler.ReleaseLock", releaseReq, &releaseResp)
}

func performWrite(client *rpc.Client, clientID string, lockPath api.FilePath, content string) error {
	openReq := api.OpenLockRequest{ClientID: api.ClientID(clientID), Filepath: lockPath}
	openResp := api.OpenLockResponse{}
	if err := client.Call("Handler.OpenLock", openReq, &openResp); err != nil || openResp.Error != nil {
		return fmt.Errorf("OpenLock failed: %v %v", err, openResp.Error)
	}
	for {
		acquireReq := api.TryAcquireLockRequest{ClientID: api.ClientID(clientID), Filepath: lockPath, Mode: api.EXCLUSIVE}
		acquireResp := api.TryAcquireLockResponse{}
		err := client.Call("Handler.TryAcquireLock", acquireReq, &acquireResp)
		if err != nil {
			return fmt.Errorf("TryAcquireLock failed: %v", err)
		}
		if acquireResp.IsSuccessful {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	writeReq := api.WriteRequest{ClientID: api.ClientID(clientID), Filepath: lockPath, Content: content}
	writeResp := api.WriteResponse{}
	if err := client.Call("Handler.WriteContent", writeReq, &writeResp); err != nil || !writeResp.IsSuccessful {
		return fmt.Errorf("WriteContent failed: %v", err)
	}
	releaseReq := api.ReleaseLockRequest{ClientID: api.ClientID(clientID), Filepath: lockPath}
	releaseResp := api.ReleaseLockResponse{}
	return client.Call("Handler.ReleaseLock", releaseReq, &releaseResp)
}

func prepopulateLocks(servers []string) {
	var leaderHint string
attemptLoop:
	for attempt := 0; attempt < len(servers)+3; attempt++ {
		var tryList []string
		if leaderHint != "" {
			tryList = []string{leaderHint}
		} else {
			tryList = shuffleServers(servers)
		}
		for _, addr := range tryList {
			log.Printf("Trying InitSession on %s", addr)
			client, err := rpc.Dial("tcp", addr)
			if err != nil {
				log.Printf("Dial failed to %s: %v", addr, err)
				continue
			}
			clientID := api.ClientID("prewriter")
			sessReq := api.InitSessionRequest{ClientID: clientID}
			var sessResp api.InitSessionResponse
			err = client.Call("Handler.InitSession", sessReq, &sessResp)
			if err != nil {
				log.Printf("RPC error from %s: %v", addr, err)
				client.Close()
				continue
			}
			if sessResp.Error == nil {
				log.Printf("Connected to leader %s", addr)
				for i := 0; i < lockPoolSize; i++ {
					path := api.FilePath(fmt.Sprintf("/file%d", i))
					_ = client.Call("Handler.OpenLock", api.OpenLockRequest{ClientID: clientID, Filepath: path}, &api.OpenLockResponse{})
					_ = client.Call("Handler.TryAcquireLock", api.TryAcquireLockRequest{ClientID: clientID, Filepath: path, Mode: api.EXCLUSIVE}, &api.TryAcquireLockResponse{})
					_ = client.Call("Handler.WriteContent", api.WriteRequest{ClientID: clientID, Filepath: path, Content: "init"}, &api.WriteResponse{})
					_ = client.Call("Handler.ReleaseLock", api.ReleaseLockRequest{ClientID: clientID, Filepath: path}, &api.ReleaseLockResponse{})
				}
				log.Printf("Prepopulated locks on leader %s", addr)
				client.Close()
				return
			}
			msg := sessResp.Error.Error()
			if strings.Contains(msg, "Leader: ") {
				parts := strings.Split(msg, "Leader: ")
				if len(parts) == 2 {
					leaderHint = strings.TrimSpace(parts[1])
					client.Close()
					continue attemptLoop
				}
			}
			client.Close()
		}
	}
	log.Fatalf("Failed to find a leader to prepopulate locks")
}

func readReplicaList(filename string) ([]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			servers := strings.Split(line, " ")
			for _, server := range servers {
				if trimmed := strings.TrimSpace(server); trimmed != "" {
					lines = append(lines, trimmed)
				}
			}
		}
	}
	return lines, scanner.Err()
}

func shuffleServers(servers []string) []string {
	shuffled := make([]string, len(servers))
	copy(shuffled, servers)
	rand.Shuffle(len(shuffled), func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})
	return shuffled
}

