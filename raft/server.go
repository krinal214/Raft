package raft

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"sync"
	"time"
)

type RPCProxy struct {
	rf *RaftModule
}

type Server struct {
	mu sync.Mutex

	serverId int
	peer_node_ids  []int

	rf       *RaftModule
	file_name  string
	rpcProxy *RPCProxy

	rpcServer *rpc.Server
	listener  net.Listener

	commit_channel  chan<- CommitRecord
	peerClients map[int]*rpc.Client

	ready <-chan interface{}
	quit  chan interface{}
	wg    sync.WaitGroup
}

// DisconnectAll closes all the client connections to peers for this server.
func (s *Server) DisconnectAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id := range s.peerClients {
		if s.peerClients[id] != nil {
			s.peerClients[id].Close()
			s.peerClients[id] = nil
		}
	}
}

func (s *Server) Call(id int, serviceMethod string, args interface{}, reply interface{}) error {
	s.mu.Lock()
	peer := s.peerClients[id]
	s.mu.Unlock()
	if peer == nil {
		return fmt.Errorf("call client %d after it's closed", id)
	} else {
		return peer.Call(serviceMethod, args, reply)
	}
}

func (s *Server) Shutdown() {
	s.rf.Stop()
	close(s.quit)
	s.listener.Close()
	s.wg.Wait()
}

func (s *Server) get_listen_addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listener.Addr()
}

func NewServer(serverId int, peerIds []int, storage string, ready <-chan interface{}, commitChan chan<- CommitRecord) *Server {
	s := new(Server)
	s.serverId = serverId
	s.peer_node_ids = peerIds
	s.peerClients = make(map[int]*rpc.Client)
	s.file_name = storage
	s.ready = ready
	s.commit_channel = commitChan
	s.quit = make(chan interface{})
	return s
}

func (s *Server) Serve() {
	s.mu.Lock()
	s.rf = NewRaftModule(s.serverId, s.peer_node_ids, s, s.file_name, s.ready, s.commit_channel)
	s.rpcServer = rpc.NewServer()
	s.rpcProxy = &RPCProxy{rf: s.rf}
	s.rpcServer.RegisterName("RaftModule", s.rpcProxy)

	var err error
	s.listener, err = net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Node %d: port no: %s", s.serverId, s.listener.Addr())
	s.mu.Unlock()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		for {
			conn, err := s.listener.Accept()
			if err != nil {
				select {
				case <-s.quit:
					return
				default:
					log.Fatal("accept error:", err)
				}
			}
			s.wg.Add(1)
			go func() {
				s.rpcServer.ServeConn(conn)
				s.wg.Done()
			}()
		}
	}()
}

func (s *Server) connect_to_peer(peerId int, addr net.Addr) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.peerClients[peerId] == nil {
		client, err := rpc.Dial(addr.Network(), addr.String())
		if err != nil {
			return err
		}
		s.peerClients[peerId] = client
	}
	return nil
}

func (s *Server) disconnect_peer(peerId int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.peerClients[peerId] != nil {
		err := s.peerClients[peerId].Close()
		s.peerClients[peerId] = nil
		return err
	}
	return nil
}

func (rpp *RPCProxy) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	time.Sleep(time.Duration(1+rand.Intn(7)) * time.Millisecond)
	return rpp.rf.RequestVote(args, reply)
}

func (rpp *RPCProxy) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	time.Sleep(time.Duration(1+rand.Intn(7)) * time.Millisecond)
	return rpp.rf.AppendEntries(args, reply)
}
