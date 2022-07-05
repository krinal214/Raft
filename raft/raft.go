package raft

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	logger "log"
	"math/rand"
	"os"
	"sync"
	"time"
)

func init() {
	logger.SetFlags(logger.Ltime | logger.Lmicroseconds)
	seed := time.Now().UnixNano()
	rand.Seed(seed)
}

type CommitRecord struct {
	Command int
	Index   int
	Term    int
}

type LogRecord struct {
	Command int
	Term    int
}

type RaftModule struct {
	mu                 sync.Mutex
	fmu                sync.Mutex
	node_id            int
	peer_node_ids      []int
	server             *Server
	file_name          string
	commit_channel     chan<- CommitRecord
	commit_ready_channel chan struct{}
	trigger_channel    chan struct{}
	cur_term           int
	voted_to           int
	log                []LogRecord
	commit_indx        int
	last_log_indx      int
	state              string
	election_reset     time.Time
	next_indx          map[int]int
	match_indx         map[int]int
}

func (rf *RaftModule) restore_from_file(storage string) {
	rf.fmu.Lock()

	type LogDataR struct {
		Term  int        `json:"CurrentTerm"`
		Voted int        `json:"VotedFor"`
		Logs  []LogRecord `json:"Logs"`
	}

	jsonFile, err := os.Open(storage)
	if err != nil {
		fmt.Println(err)
	}

	byteValue, _ := ioutil.ReadAll(jsonFile)
	var logD LogDataR
	json.Unmarshal(byteValue, &logD)

	rf.cur_term = logD.Term
	rf.voted_to = logD.Voted
	rf.log = logD.Logs
	jsonFile.Close()
	rf.fmu.Unlock()
}

func (rf *RaftModule) save_to_file() {
	rf.fmu.Lock()
	type LogDataP struct {
		CurrentTerm int
		VotedFor    int
		Logs        []LogRecord
	}

	data := LogDataP{
		CurrentTerm: rf.cur_term,
		VotedFor:    rf.voted_to,
		Logs:        rf.log,
	}

	file, _ := json.MarshalIndent(data, "", " ")
	_ = ioutil.WriteFile(rf.file_name, file, 0644)
	rf.fmu.Unlock()
}

func (rf *RaftModule) Stop() {
	rf.mu.Lock()
	rf.state = "DeadNode"
	logger.Printf(fmt.Sprintf("Node %d: ", rf.node_id) + " disconnected")
	close(rf.commit_ready_channel)
	rf.mu.Unlock()
}

func NewRaftModule(id int, peerIds []int, server *Server, storage string, ready <-chan interface{}, commitChan chan<- CommitRecord) *RaftModule {
	rf := new(RaftModule)
	rf.node_id = id
	rf.peer_node_ids = peerIds
	rf.server = server
	rf.file_name = storage
	rf.commit_channel = commitChan
	rf.commit_ready_channel = make(chan struct{}, 16)
	rf.trigger_channel = make(chan struct{}, 1)
	rf.state = "FollowerNode"
	rf.voted_to = -1
	rf.commit_indx = -1
	rf.last_log_indx = -1
	rf.next_indx = make(map[int]int)
	rf.match_indx = make(map[int]int)

	f, err := os.OpenFile("logfile", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		logger.Fatalf("error opening file: %v", err)
	}

	logger.SetOutput(f)

	rf.restore_from_file(rf.file_name)

	go func() {
		<-ready
		rf.mu.Lock()
		rf.election_reset = time.Now()
		rf.mu.Unlock()
		rf.run_election_timer()
	}()

	go rf.commit_channel_sender()
	return rf
}

func (rf *RaftModule) Report() (id int, term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.node_id, rf.cur_term, rf.state == "LeaderNode"
}

func (rf *RaftModule) Submit(command int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	logger.Printf(fmt.Sprintf("Node %d: ", rf.node_id)+" Command %v received by node %d", command, rf.node_id)

	if rf.state == "LeaderNode" {
		rf.log = append(rf.log, LogRecord{Command: command, Term: rf.cur_term})
		rf.save_to_file()
		logger.Printf(fmt.Sprintf("Node %d: ", rf.node_id)+" log content=%v", rf.log)
		rf.trigger_channel <- struct{}{}
		return true
	}

	return false
}




type RequestVoteArgs struct {
	Term            int
	CandidateNodeId int
	LastLogIndex    int
	LastLogTerm     int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *RaftModule) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	rf.mu.Lock()
	if rf.state == "DeadNode" {
		rf.mu.Unlock()
		return nil
	}

	lastLogIndex := rf.lastLogIndex()
	lastLogTerm := rf.lastLogTerm(lastLogIndex)
	logger.Printf(fmt.Sprintf("Node %d: ", rf.node_id)+" RequestVote: %+v {currentTerm=%d, last log entry (index/term)=(%d, %d), votedFor=%d}", args, rf.cur_term, lastLogIndex, lastLogTerm, rf.voted_to)

	if args.Term > rf.cur_term {
		logger.Printf(fmt.Sprintf("Node %d: ", rf.node_id) + " RequestVote received outdated term")
		rf.become_follower_node(args.Term)
	}

	if rf.cur_term == args.Term && rf.voted_to == -1 &&
		(args.LastLogTerm > lastLogTerm ||
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
		reply.VoteGranted = true
		rf.voted_to = args.CandidateNodeId
		rf.election_reset = time.Now()
	} else if rf.cur_term == args.Term && rf.voted_to == args.CandidateNodeId &&
		(args.LastLogTerm > lastLogTerm ||
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
		reply.VoteGranted = true
		rf.voted_to = args.CandidateNodeId
		rf.election_reset = time.Now()
	} else {
		reply.VoteGranted = false
	}

	reply.Term = rf.cur_term
	rf.save_to_file()
	logger.Printf(fmt.Sprintf("Node %d: ", rf.node_id)+" RequestVote reply Term: %d, VoteGranted: %t", reply.Term, reply.VoteGranted)

	rf.mu.Unlock()
	return nil
}

type AppendEntriesArgs struct {
	Term         int
	LeaderNodeId int

	PrevLogIndex     int
	PrevLogTerm      int
	Entries          []LogRecord
	LeaderNodeCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	ConflictIndex int
	ConflictTerm  int
}

func (rf *RaftModule) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == "DeadNode" {
		return nil
	}
	logger.Printf(fmt.Sprintf("Node %d: ", rf.node_id)+" AppendEntries: %+v", args)

	if args.Term > rf.cur_term {
		logger.Printf(fmt.Sprintf("Node %d: ", rf.node_id) + " outdated term in AppendEntries")
		rf.become_follower_node(args.Term)
	}

	reply.Success = false
	if args.Term == rf.cur_term {
		if rf.state != "FollowerNode" {
			rf.become_follower_node(args.Term)
		}
		rf.election_reset = time.Now()
		if args.PrevLogIndex == -1 ||
			(args.PrevLogIndex < len(rf.log) && args.PrevLogTerm == rf.log[args.PrevLogIndex].Term) {
			reply.Success = true

			logInsertIndex := args.PrevLogIndex + 1
			newEntriesIndex := 0

			for {
				if logInsertIndex >= len(rf.log) || newEntriesIndex >= len(args.Entries) {
					break
				}
				if rf.log[logInsertIndex].Term != args.Entries[newEntriesIndex].Term {
					break
				}
				logInsertIndex++
				newEntriesIndex++
			}
			if newEntriesIndex < len(args.Entries) {
				rf.log = append(rf.log[:logInsertIndex], args.Entries[newEntriesIndex:]...)
				logger.Printf(fmt.Sprintf("Node %d: ", rf.node_id)+" inserting entries %v from index %d. Current log %v", args.Entries[newEntriesIndex:], logInsertIndex, rf.log)
			}
			if args.LeaderNodeCommit > rf.commit_indx {
				if args.LeaderNodeCommit < len(rf.log)-1 {
					rf.commit_indx = args.LeaderNodeCommit
				} else {
					rf.commit_indx = len(rf.log) - 1
				}
				logger.Printf(fmt.Sprintf("Node %d: ", rf.node_id)+" commitIndex updated to %d", rf.commit_indx)
				rf.commit_ready_channel <- struct{}{}
			}
		} else {
			if args.PrevLogIndex >= len(rf.log) {
				reply.ConflictIndex = len(rf.log)
				reply.ConflictTerm = -1
			} else {
				reply.ConflictTerm = rf.log[args.PrevLogIndex].Term

				var i int
				for i = args.PrevLogIndex - 1; i >= 0; i-- {
					if rf.log[i].Term != reply.ConflictTerm {
						break
					}
				}
				reply.ConflictIndex = i + 1
			}
		}
	}

	reply.Term = rf.cur_term
	rf.save_to_file()
	logger.Printf(fmt.Sprintf("Node %d: ", rf.node_id)+" AppendEntries reply: %+v", *reply)
	return nil
}

func (rf *RaftModule) election_timeout() time.Duration {
	timer := time.Duration(150) * time.Millisecond
	if rand.Intn(3) != 0 || len(os.Getenv("RAFT_FORCE_MORE_REELECTION")) == 0 {
		timer = time.Duration(150+rand.Intn(150)) * time.Millisecond
	}
	return timer
}

func (rf *RaftModule) run_election_timer() {
	timeoutDuration := rf.election_timeout()
	rf.mu.Lock()
	termStarted := rf.cur_term
	rf.mu.Unlock()
	logger.Printf(fmt.Sprintf("Node %d: ", rf.node_id)+" Election timer for term=%d and timeout=%v", termStarted, timeoutDuration)

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		<-ticker.C

		rf.mu.Lock()
		if rf.state == "DeadNode" {
			rf.mu.Unlock()
			return
		} else if rf.state == "LeaderNode" {
			logger.Printf(fmt.Sprintf("Node %d: ", rf.node_id)+" Current state = %s", rf.state)
			rf.mu.Unlock()
			return
		} else if termStarted < rf.cur_term {
			logger.Printf(fmt.Sprintf("Node %d: ", rf.node_id)+" Term changed: %d -> %d", termStarted, rf.cur_term)
			rf.mu.Unlock()
			return
		} else {
			elapsed := time.Since(rf.election_reset)
			if elapsed >= timeoutDuration {
				rf.start_election()
				rf.mu.Unlock()
				return
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *RaftModule) sendRequestVoteRPC(peerId int, savedCurrentTerm int, votesReceived *int) {
	rf.mu.Lock()
	savedLastLogIndex := rf.lastLogIndex()
	savedLastLogTerm := rf.lastLogTerm(savedLastLogIndex)
	rf.mu.Unlock()

	args := RequestVoteArgs{
		Term:            savedCurrentTerm,
		CandidateNodeId: rf.node_id,
		LastLogIndex:    savedLastLogIndex,
		LastLogTerm:     savedLastLogTerm,
	}

	logger.Printf(fmt.Sprintf("Node %d: ", rf.node_id)+" RequestVote sent to node %d: %+v", peerId, args)
	var reply RequestVoteReply
	err := rf.server.Call(peerId, "RaftModule.RequestVote", args, &reply)

	if err == nil {
		rf.mu.Lock()
		logger.Printf(fmt.Sprintf("Node %d: ", rf.node_id)+" RequestVoteReply %+v", reply)

		if rf.state != "CandidateNode" {
			logger.Printf(fmt.Sprintf("Node %d: ", rf.node_id)+" Waiting for reply, state = %v", rf.state)
			rf.mu.Unlock()
			return
		} else if reply.Term > savedCurrentTerm {
			logger.Printf(fmt.Sprintf("Node %d: ", rf.node_id) + " outdated term in RequestVoteReply")
			rf.become_follower_node(reply.Term)
			rf.mu.Unlock()
			return
		} else if reply.Term == savedCurrentTerm && reply.VoteGranted {
			*votesReceived = *votesReceived + 1
			if (*votesReceived)*2 > len(rf.peer_node_ids)+1 {
				logger.Printf(fmt.Sprintf("Node %d: ", rf.node_id)+" wins election with votes = %d", *votesReceived)
				rf.start_leader_node()
				rf.mu.Unlock()
				return
			}
		}

		rf.mu.Unlock()
	}
}

func (rf *RaftModule) start_election() {
	rf.state = "CandidateNode"
	rf.cur_term += 1
	savedCurrentTerm := rf.cur_term
	rf.election_reset = time.Now()
	rf.voted_to = rf.node_id
	logger.Printf(fmt.Sprintf("Node %d: ", rf.node_id)+" is now a CandidateNode with current Term=%d and log=%v", savedCurrentTerm, rf.log)

	votesReceived := 1

	for i := 0; i < len(rf.peer_node_ids); i++ {
		go rf.sendRequestVoteRPC(rf.peer_node_ids[i], savedCurrentTerm, &votesReceived)
	}

	go rf.run_election_timer()
}

func (rf *RaftModule) become_follower_node(term int) {
	logger.Printf(fmt.Sprintf("Node %d: ", rf.node_id)+" is now a follower with term=%d and log=%v", term, rf.log)
	rf.state = "FollowerNode"
	rf.cur_term = term
	rf.voted_to = -1
	rf.election_reset = time.Now()

	go rf.run_election_timer()
}

func (rf *RaftModule) send_to_peer(heartbeatTimeout time.Duration) {
	rf.leader_send_AE()

	t := time.NewTimer(heartbeatTimeout)
	defer t.Stop()

	for {
		doSend := false
		select {
		case <-t.C:
			doSend = true
			t.Stop()
			t.Reset(heartbeatTimeout)
		case _, ok := <-rf.trigger_channel:
			if ok {
				doSend = true
			} else {
				return
			}
			if !t.Stop() {
				<-t.C
			}
			t.Reset(heartbeatTimeout)
		}

		if doSend {
			rf.mu.Lock()
			if rf.state != "LeaderNode" {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			rf.leader_send_AE()
		}
	}
}

func (rf *RaftModule) start_leader_node() {
	rf.state = "LeaderNode"

	for i := 0; i < len(rf.peer_node_ids); i++ {
		peerId := rf.peer_node_ids[i]
		rf.next_indx[peerId] = len(rf.log)
		rf.match_indx[peerId] = -1
	}
	logger.Printf(fmt.Sprintf("Node %d: ", rf.node_id)+" is now a LeaderNode with term=%d, nextIndex=%v, matchIndex=%v; log=%v", rf.cur_term, rf.next_indx, rf.match_indx, rf.log)

	go rf.send_to_peer(50 * time.Millisecond)
}

func (rf *RaftModule) send_receive_AE(peerId int, savedCurrentTerm int) {
	rf.mu.Lock()
	ni := rf.next_indx[peerId]
	prevLogIndex := ni - 1
	prevLogTerm := rf.lastLogTerm(prevLogIndex)

	entries := rf.log[ni:]

	args := AppendEntriesArgs{
		Term:             savedCurrentTerm,
		LeaderNodeId:     rf.node_id,
		PrevLogIndex:     prevLogIndex,
		PrevLogTerm:      prevLogTerm,
		Entries:          entries,
		LeaderNodeCommit: rf.commit_indx,
	}

	rf.mu.Unlock()
	logger.Printf(fmt.Sprintf("Node %d: ", rf.node_id)+" sent AppendEntries to %v: ni=%d, args=%+v", peerId, ni, args)

	var reply AppendEntriesReply
	err := rf.server.Call(peerId, "RaftModule.AppendEntries", args, &reply)

	if err == nil {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if reply.Term > savedCurrentTerm {
			logger.Printf(fmt.Sprintf("Node %d: ", rf.node_id) + " outdated term in heartbeat reply")
			rf.become_follower_node(reply.Term)
			return
		} else if rf.state == "LeaderNode" && savedCurrentTerm == reply.Term && reply.Success {
			rf.next_indx[peerId] = ni + len(entries)
			rf.match_indx[peerId] = rf.next_indx[peerId] - 1

			savedCommitIndex := rf.commit_indx
			for i := rf.commit_indx + 1; i < len(rf.log); i++ {
				if rf.log[i].Term == rf.cur_term {
					matchCount := 1
					for j := 0; j < len(rf.peer_node_ids); j++ {
						peerId = rf.peer_node_ids[j]
						if rf.match_indx[peerId] >= i {
							matchCount++
						}
					}

					if matchCount*2 > len(rf.peer_node_ids)+1 {
						rf.commit_indx = i
					}
				}
			}
			logger.Printf(fmt.Sprintf("Node %d: ", rf.node_id)+" AppendEntries reply from %d success: nextIndex := %v, matchIndex := %v; commitIndex := %d", peerId, rf.next_indx, rf.match_indx, rf.commit_indx)

			if rf.commit_indx != savedCommitIndex {
				logger.Printf(fmt.Sprintf("Node %d: ", rf.node_id)+" leader sets commitIndex := %d", rf.commit_indx)

				rf.commit_ready_channel <- struct{}{}
				rf.trigger_channel <- struct{}{}
			}
		} else if rf.state == "LeaderNode" && savedCurrentTerm == reply.Term {
			if reply.ConflictTerm >= 0 {
				lastIndexOfTerm := -1

				for i := len(rf.log) - 1; i >= 0; i-- {
					if rf.log[i].Term == reply.ConflictTerm {
						lastIndexOfTerm = i
						break
					}
				}

				if lastIndexOfTerm >= 0 {
					rf.next_indx[peerId] = lastIndexOfTerm + 1
				} else {
					rf.next_indx[peerId] = reply.ConflictIndex
				}
			} else {
				rf.next_indx[peerId] = reply.ConflictIndex
			}

			logger.Printf(fmt.Sprintf("Node %d: ", rf.node_id)+" AppendEntries reply from %d !success: nextIndex := %d", peerId, ni-1)
		}
	}
}

func (rf *RaftModule) leader_send_AE() {
	rf.mu.Lock()
	savedCurrentTerm := rf.cur_term
	rf.mu.Unlock()

	for i := 0; i < len(rf.peer_node_ids); i++ {
		peerId := rf.peer_node_ids[i]

		go rf.send_receive_AE(peerId, savedCurrentTerm)
	}
}

func (rf *RaftModule) lastLogIndex() int {
	if len(rf.log) > 0 {
		return len(rf.log) - 1
	} else {
		return -1
	}
}

func (rf *RaftModule) lastLogTerm(index int) int {
	if index >= 0 {
		return rf.log[index].Term
	} else {
		return -1
	}
}

func (rf *RaftModule) commit_channel_sender() {
	for range rf.commit_ready_channel {
		rf.mu.Lock()
		savedTerm := rf.cur_term
		savedLastApplied := rf.last_log_indx
		var log_entries []LogRecord

		if rf.commit_indx > rf.last_log_indx {
			log_entries = rf.log[rf.last_log_indx+1 : rf.commit_indx+1]
			rf.last_log_indx = rf.commit_indx
		}

		rf.mu.Unlock()
		logger.Printf(fmt.Sprintf("Node %d: ", rf.node_id)+" commit_channel_sender entries=%v, savedLastApplied=%d", log_entries, savedLastApplied)

		for i := 0; i < len(log_entries); i++ {
			entry := log_entries[i]
			rf.commit_channel <- CommitRecord{
				Command: entry.Command,
				Index:   savedLastApplied + i + 1,
				Term:    savedTerm,
			}
		}
	}
	logger.Printf(fmt.Sprintf("Node %d: ", rf.node_id) + " commit_channel_sender done")
}
