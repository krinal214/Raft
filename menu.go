package main

import (
	"fmt"
	r "project/raft"
	"time"
)

func main() {
	fmt.Println("===== Welcome to Raft Consensus =====")
	fmt.Println("")

	var nodes int
	fmt.Print("Enter the no of nodes: ")
	fmt.Scanf("%d", &nodes)

	h := r.Simulation(nodes)

	for {
		fmt.Printf("========== MENU ==========\n\n")
		for i := 0; i < nodes; i++ {
			if h.IsConnectedPeer(i) {
				fmt.Printf("Enter %d to disconnect node %d\n", i, i)
			} else {
				fmt.Printf("Enter %d to reconnect node %d\n", i, i)
			}
		}
		fmt.Printf("Enter %d to show current leader node and term id\n", nodes)
		fmt.Printf("Enter %d to submit a value to leader\n", nodes+1)
		fmt.Printf("Enter %d to show committed values\n", nodes+2)
		fmt.Printf("Enter -1 to EXIT\n\n")

		var inp int
		fmt.Printf("Enter a value: ")
		fmt.Scan(&inp)

		if inp >= 0 && inp < nodes {
			if h.IsConnectedPeer(inp) {
				h.DisconnectPeer(inp)
				fmt.Printf("Node %d disconnected.\n\n", inp)
			} else {
				h.ReconnectPeer(inp)
				fmt.Printf("Node %d reconnected.\n\n", inp)
			}

		} else if inp == nodes {
			server_id, server_term_no := h.GetLeader()
			fmt.Printf("Leader node is %d with term id %d\n\n", server_id, server_term_no)

		} else if inp == nodes+1 {
			var value int
			fmt.Println("Enter a value to submit to the server: ")
			fmt.Scan(&value)

			server_id, _ := h.GetLeader()

			if server_id == -1 {
				fmt.Printf("No leader is currently available!\n\n")
				continue
			}

			fmt.Printf("Submitting %d to node %d\n\n", value, server_id)
			isLeader := h.SubmitToServer(server_id, value)
			if !isLeader {
				fmt.Printf("Want id=%d as leader, but it's not\n\n", server_id)
				continue
			}

			time.Sleep(time.Duration(250) * time.Millisecond)

			fmt.Printf("Value submitted to nodes. Please check the committed values using command %d\n\n", nodes+2)

		} else if inp == nodes+2 {
			h.PrintCommits()

		} else if inp == -1 {
			fmt.Println("Exiting...")
			break

		} else {
			fmt.Printf("Invalid input!\n\n")
		}
	}
}
