package raft

import (
	"encoding/json"
	"fmt"
	"io/ioutil"

	//"log"
	//"math/rand"
	"os"
	"strconv"
	"sync"
	"time"
)

// func init() {
// 	log.SetFlags(log.Ltime | log.Lmicroseconds)
// 	seed := time.Now().UnixNano()
// 	rand.Seed(seed)
// }

type LogEntryTH struct {
	Command int
	Term    int
}

type LogDataTH struct {
	CurrentTerm int
	VotedFor    int
	Logs        []LogEntryTH
}

type Test struct {
	mu sync.Mutex

	cluster         []*Server
	file_name       []string
	commit_channels []chan CommitRecord
	commits         [][]CommitRecord
	connected       []bool
	alive           []bool
	nodes           int
}

func Simulation(nodes int) *Test {
	h := new(Test)
	h.cluster = make([]*Server, nodes)
	h.connected = make([]bool, nodes)
	h.alive = make([]bool, nodes)
	h.commit_channels = make([]chan CommitRecord, nodes)
	h.commits = make([][]CommitRecord, nodes)
	ready := make(chan interface{})
	h.file_name = make([]string, nodes)
	h.nodes = nodes

	data := LogDataTH{
		CurrentTerm: 0,
		VotedFor:    -1,
		Logs:        []LogEntryTH{},
	}

	file, _ := json.MarshalIndent(data, "", " ")

	f, err := os.Create("logfile")
	if err != nil {
		fmt.Println("Error creating log file!")
		os.Exit(1)
	}

	for i := 0; i < nodes; i++ {
		peerIds := make([]int, 0)
		for p := 0; p < nodes; p++ {
			if p != i {
				peerIds = append(peerIds, p)
			}
		}

		h.file_name[i] = "Log_" + strconv.Itoa(i) + ".json"

		_ = ioutil.WriteFile(h.file_name[i], file, 0644)

		h.commit_channels[i] = make(chan CommitRecord)
		h.cluster[i] = NewServer(i, peerIds, h.file_name[i], ready, h.commit_channels[i])
		h.cluster[i].Serve()
		h.alive[i] = true
	}
	for i := 0; i < nodes; i++ {
		for j := 0; j < nodes; j++ {
			if i != j {
				h.cluster[i].connect_to_peer(j, h.cluster[j].get_listen_addr())
			}
		}
		h.connected[i] = true
	}
	close(ready)
	f.Close()

	for i := 0; i < nodes; i++ {
		go h.collect_commits(i)
	}
	return h
}

func (h *Test) IsConnectedPeer(id int) bool {
	return h.connected[id]
}

func (h *Test) DisconnectPeer(id int) {
	h.cluster[id].DisconnectAll()
	for j := 0; j < h.nodes; j++ {
		if j != id {
			h.cluster[j].disconnect_peer(id)
		}
	}
	h.connected[id] = false
}

func (h *Test) ReconnectPeer(id int) {
	for j := 0; j < h.nodes; j++ {
		if j != id && h.alive[j] {
			if err := h.cluster[id].connect_to_peer(j, h.cluster[j].get_listen_addr()); err != nil {
				fmt.Printf("Error occurred while reconnecting peer %d\n\n", id)
			}
			if err := h.cluster[j].connect_to_peer(id, h.cluster[id].get_listen_addr()); err != nil {
				fmt.Printf("Error occurred while reconnecting peer %d\n\n", id)
			}
		}
	}
	h.connected[id] = true
}

func (h *Test) GetLeader() (int, int) {
	for r := 0; r < 10; r++ {
		leaderId := -1
		leaderTerm := -1
		for i := 0; i < h.nodes; i++ {
			if h.connected[i] {
				_, term, isLeader := h.cluster[i].rf.Report()
				if isLeader {
					if leaderId < 0 {
						leaderId = i
						leaderTerm = term
					} else {
						fmt.Printf("Error occurred while checking leader.\n\n")
					}
				}
			}
		}
		if leaderId >= 0 {
			return leaderId, leaderTerm
		}
		time.Sleep(150 * time.Millisecond)
	}

	fmt.Printf("Error! No leader found!")
	return -1, -1
}

func (h *Test) PrintCommits() {
	fmt.Printf("                             \tCommand\t\tIndex\t\tTerm\n\n")
	for i := 0; i < len(h.commits); i++ {
		if h.connected[i] {
			fmt.Printf("Commit values for node %d:-\n", i)
			for j := 0; j < len(h.commits[i]); j++ {
				fmt.Printf("\t\t\t\t  %d\t\t  %d\t\t  %d\n", h.commits[i][j].Command, h.commits[i][j].Index, h.commits[i][j].Term)
			}
		} else {
			fmt.Printf("Node %d not connected to server currently.\n", i)
		}
		fmt.Println("")
	}
	fmt.Println("")
}

func (h *Test) SubmitToServer(serverId int, cmd int) bool {
	return h.cluster[serverId].rf.Submit(cmd)
}

func (h *Test) collect_commits(i int) {
	for c := range h.commit_channels[i] {
		h.mu.Lock()
		h.commits[i] = append(h.commits[i], c)
		h.mu.Unlock()
	}
}
