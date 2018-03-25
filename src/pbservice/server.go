package simplepb

//
// This is a outline of primary-backup replication based on a simplifed version of Viewstamp replication.
//
//
//

import (
	"sync"

	"labrpc"
	"fmt"
	"log"
)

// the 3 possible server status
const (
	NORMAL     = iota
	VIEWCHANGE
	RECOVERING
)

// PBServer defines the state of a replica server (either primary or backup)
type PBServer struct {
	mu             sync.Mutex          // Lock to protect shared access to this peer's state
	peers          []*labrpc.ClientEnd // RPC end points of all peers
	me             int                 // this peer's index into peers[]
	currentView    int                 // what this peer believes to be the current active view
	status         int                 // the server's current status (NORMAL, VIEWCHANGE or RECOVERING)
	lastNormalView int                 // the latest view which had a NORMAL status

	log         []interface{} // the log of "commands"
	commitIndex int           // all log entries <= commitIndex are considered to have been committed.

	// ... other state that you might need ...
}

// Prepare defines the arguments for the Prepare RPC
// Note that all field names must start with a capital letter for an RPC args struct
type PrepareArgs struct {
	View          int           // the primary's current view
	PrimaryCommit int           // the primary's commitIndex
	Index         int           // the index position at which the log entry is to be replicated on backups
	Entry         interface{}   // the log entry to be replicated
	Log           []interface{} // the log of "commands"
}

// PrepareReply defines the reply for the Prepare RPC
// Note that all field names must start with a capital letter for an RPC reply struct
type PrepareReply struct {
	View    int  // the backup's current view
	Success bool // whether the Prepare request has been accepted or rejected
}

// RecoverArgs defined the arguments for the Recovery RPC
type RecoveryArgs struct {
	View   int // the view that the backup would like to synchronize with
	Server int // the server sending the Recovery RPC (for debugging)
}

type RecoveryReply struct {
	View          int           // the view of the primary
	Entries       []interface{} // the primary's log including entries replicated up to and including the view.
	PrimaryCommit int           // the primary's commitIndex
	Success       bool          // whether the Recovery request has been accepted or rejected
}

type ViewChangeArgs struct {
	View int // the new view to be changed into
}

type ViewChangeReply struct {
	LastNormalView int           // the latest view which had a NORMAL status at the server
	Log            []interface{} // the log at the server
	Success        bool          // whether the ViewChange request has been accepted/rejected
}

type StartViewArgs struct {
	View int           // the new view which has completed view-change
	Log  []interface{} // the log associated with the new new
}

type StartViewReply struct {
}

// GetPrimary is an auxilary function that returns the server index of the
// primary server given the view number (and the total number of replica servers)
func GetPrimary(view int, nservers int) int {
	return view % nservers
}

// IsCommitted is called by tester to check whether an index position
// has been considered committed by this server
func (srv *PBServer) IsCommitted(index int) (committed bool) {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if srv.commitIndex >= index {
		return true
	}
	return false
}

// ViewStatus is called by tester to find out the current view of this server
// and whether this view has a status of NORMAL.
func (srv *PBServer) ViewStatus() (currentView int, statusIsNormal bool) {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	return srv.currentView, srv.status == NORMAL
}

// GetEntryAtIndex is called by tester to return the command replicated at
// a specific log index. If the server's log is shorter than "index", then
// ok = false, otherwise, ok = true
func (srv *PBServer) GetEntryAtIndex(index int) (ok bool, command interface{}) {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if len(srv.log) > index {
		return true, srv.log[index]
	}
	return false, command
}

// Kill is called by tester to clean up (e.g. stop the current server)
// before moving on to the next test
func (srv *PBServer) Kill() {
	// Your code here, if necessary
}

// Make is called by tester to create and initalize a PBServer
// peers is the list of RPC endpoints to every server (including self)
// me is this server's index into peers.
// startingView is the initial view (set to be zero) that all servers start in
func Make(peers []*labrpc.ClientEnd, me int, startingView int) *PBServer {
	srv := &PBServer{
		peers:          peers,
		me:             me,
		currentView:    startingView,
		lastNormalView: startingView,
		status:         NORMAL,
	}
	// all servers' log are initialized with a dummy command at index 0
	var v interface{}
	srv.log = append(srv.log, v)
	// Your other initialization code here, if there's any
	//fmt.Println(srv)
	return srv
}

// Start() is invoked by tester on some replica server to replicate a
// command.  Only the primary should process this request by appending
// the command to its log and then return *immediately* (while the log is being replicated to backup servers).
// if this server isn't the primary, returns false.
// Note that since the function returns immediately, there is no guarantee that this command
// will ever be committed upon return, since the primary
// may subsequently fail before replicating the command to all servers
//
// The first return value is the index that the command will appear at
// *if it's eventually committed*. The second return value is the current
// view. The third return value is true if this server believes it is
// the primary.
func (srv *PBServer) Start(command interface{}) (
	index int, view int, ok bool) {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	// do not process command if status is not NORMAL
	// and if i am not the primary in the current view
	if srv.status != NORMAL {
		return -1, srv.currentView, false
	} else if GetPrimary(srv.currentView, len(srv.peers)) != srv.me {
		return -1, srv.currentView, false
	}
	//oldLog:=srv.log
	logIndex := len(srv.log)
	olgLog:=srv.log
	srv.log = append(srv.log, command)
	cc:=command.(int)
	if logIndex>=1{
		logValue,valCheck:=srv.log[logIndex-1].(int)
		if valCheck{
			fmt.Println(logValue)
		}
		if  cc==logValue {
			srv.log=olgLog
			//fmt.Println(cc)
		}
	}


	log.Println("start -- Primary Log", srv.log)
	prepArgs := PrepareArgs{
		View:          srv.currentView,
		PrimaryCommit: srv.commitIndex,
		Index:         logIndex,
		Entry:         command,
		//Log:srv.log,
	}
	prepReply := PrepareReply{}
	/*recArgs:=RecoveryArgs{
		View: srv.currentView,
		Server:srv.me,
	}*/
	//recReply:=RecoveryReply{}
	fmt.Println("After update Primary Log ", srv.log)
	countSucess := 0
	for i := 0; i < len(srv.peers); i++ {
		if i != srv.me {
			ok = srv.sendPrepare(i, &prepArgs, &prepReply)
			/*if(!ok){
				ok =srv.peers[i].Call("PBServer.Recovery", &recArgs, &recReply)
			}*/
		}
		if ok == true {
			countSucess = countSucess + 1
		}

	}
	log.Println("countSucess:", countSucess, "len(srv.peers)/2:", len(srv.peers)/2)
	if countSucess >= len(srv.peers)/2 {
		//srv.commitIndex=srv.commitIndex+1
		srv.commitIndex = srv.commitIndex + 1
		if logIndex > srv.commitIndex {
			srv.commitIndex = logIndex
		}
	}
	//}else{
	//	//tempIndex:=srv.commitIndex+1
	//	return logIndex, view,false
	//}

	return logIndex, view, true
}

// exmple code to send an AppendEntries RPC to a server.
// server is the index of the target server in srv.peers[].
// expects RPC arguments in args.
// The RPC library fills in *reply with RPC reply, so caller should pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
func (srv *PBServer) sendPrepare(server int, args *PrepareArgs, reply *PrepareReply) bool {
	ok := srv.peers[server].Call("PBServer.Prepare", args, reply)
	return ok
}

// Prepare is the RPC handler for the Prepare RPC
func (srv *PBServer) Prepare(args *PrepareArgs, reply *PrepareReply) {
	// Your code here
	recArgs := RecoveryArgs{
		View:   args.View,
		Server: srv.me,
	}
	recReply := RecoveryReply{}

	if args.View == srv.currentView {
		if len(srv.log) == args.Index {
			srv.log = append(srv.log, args.Entry)
			srv.currentView = args.View
			srv.commitIndex = args.Index

		} else if len(srv.log) < args.Index {
			fmt.Println(" Recovery")
			primary := GetPrimary(args.View, len(srv.peers))
			ok := srv.peers[primary].Call("PBServer.Recovery", &recArgs, &recReply)
			if ok {
				srv.log = recReply.Entries
				srv.currentView = recReply.View
				srv.commitIndex = recReply.PrimaryCommit
			}

		}
		reply.View = args.View
		reply.Success = true
		log.Println("Prepare Secondary Log: ", srv.log, " server: ", srv.me)
		return
	} else if args.View < srv.currentView {
		fmt.Println(" Recovery")
		primary := GetPrimary(args.View, len(srv.peers))
		ok := srv.peers[primary].Call("PBServer.Recovery", &recArgs, &recReply)
		if ok {
			srv.log = recReply.Entries
			srv.currentView = recReply.View
			srv.commitIndex = recReply.PrimaryCommit
		}
		reply.Success = true
		reply.View = args.View
		log.Println("Prepare Secondary Log: ", srv.log, " server: ", srv.me)
		return
	}
	reply.Success = false
	return

}

// Recovery is the RPC handler for the Recovery RPC
func (srv *PBServer) Recovery(args *RecoveryArgs, reply *RecoveryReply) {
	// Your code here
	//if srv.log[0] == nil {
	//	srv.log[0]=reply.Entries
	//}else{
	//	srv.log = append(srv.log, reply.Entries)
	//}
	log.Println("in Recovery")
	reply.Entries = srv.log
	reply.PrimaryCommit = srv.commitIndex
	reply.View = srv.currentView
	reply.Success = true
	log.Println("Recovery Log to be updated for server", srv.me)
	return
}

// Some external oracle prompts the primary of the newView to
// switch to the newView.
// PromptViewChange just kicks start the view change protocol to move to the newView
// It does not block waiting for the view change process to complete.
func (srv *PBServer) PromptViewChange(newView int) {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	newPrimary := GetPrimary(newView, len(srv.peers))

	if newPrimary != srv.me { //only primary of newView should do view change
		return
	} else if newView <= srv.currentView {
		return
	}
	vcArgs := &ViewChangeArgs{
		View: newView,
	}
	vcReplyChan := make(chan *ViewChangeReply, len(srv.peers))
	// send ViewChange to all servers including myself
	for i := 0; i < len(srv.peers); i++ {
		go func(server int) {
			var reply ViewChangeReply
			//log.Println("server:",server,srv)
			ok := srv.peers[server].Call("PBServer.ViewChange", vcArgs, &reply)
			// fmt.Printf("node-%d (nReplies %d) received reply ok=%v reply=%v\n", srv.me, nReplies, ok, r.reply)
			//fmt.Println(ok, " vcArgs:", vcArgs, "server:", server)
			if ok {
				vcReplyChan <- &reply
			} else {
				vcReplyChan <- nil
			}
		}(i)
	}

	// wait to receive ViewChange replies
	// if view change succeeds, send StartView RPC
	go func() {
		var successReplies []*ViewChangeReply
		var nReplies int
		majority := len(srv.peers)/2 + 1
		for r := range vcReplyChan {
			nReplies++
			if r != nil && r.Success {
				successReplies = append(successReplies, r)
			}
			if nReplies == len(srv.peers) || len(successReplies) == majority {
				break
			}
		}
		ok, log := srv.determineNewViewLog(successReplies)
		if !ok {
			return
		}
		svArgs := &StartViewArgs{
			View: vcArgs.View,
			Log:  log,
		}
		// send StartView to all servers including myself
		for i := 0; i < len(srv.peers); i++ {
			var reply StartViewReply
			go func(server int) {
				// fmt.Printf("node-%d sending StartView v=%d to node-%d\n", srv.me, svArgs.View, server)
				srv.peers[server].Call("PBServer.StartView", svArgs, &reply)
			}(i)
		}
		fmt.Println("---After View Update Srv.Log:", srv.log, "Server: ", srv.me)
	}()
}

// determineNewViewLog is invoked to determine the log for the newView based on
// the collection of replies for successful ViewChange requests.
// if a quorum of successful replies exist, then ok is set to true.
// otherwise, ok = false.
func (srv *PBServer) determineNewViewLog(successReplies []*ViewChangeReply) (
	ok bool, newViewLog []interface{}) {
	// Your code here
	maxView := 0
	for _, view := range successReplies {

		if view.LastNormalView > maxView {
			maxView = view.LastNormalView
			newViewLog = view.Log
			ok = view.Success
			fmt.Println("**Before update newViewLog",newViewLog,"view.Log",view.Log,"srv",srv.me)

		} else if view.LastNormalView == maxView {
			if len(newViewLog) < len(view.Log) {
				fmt.Println("Before update newViewLog",newViewLog,"view.Log",view.Log,"srv",srv.me)

				if len(view.Log)>1 {
					logValue,valCheck:=view.Log[len(view.Log)-2].(int)
					newlogValue,valCheck:=view.Log[len(view.Log)-1].(int)
					if valCheck{
						//fmt.Println(logValue)
					}
					if  newlogValue>logValue {
						newViewLog = view.Log
						//fmt.Println(cc)
					}
				}else{
					newViewLog = view.Log
				}
				//newViewLog = view.Log
				ok = view.Success

			}
		}
	}
	log.Println("Final-- maxView:", maxView, " NewLog", newViewLog,"Primary:",srv.me)
	return ok, newViewLog
}

// ViewChange is the RPC handler to process ViewChange RPC.
func (srv *PBServer) ViewChange(args *ViewChangeArgs, reply *ViewChangeReply) {
	// Your code here
	log.Println("In view Change", srv.me," lastNormalView: ",srv.lastNormalView)
	if args.View > srv.currentView {
		srv.currentView = args.View
		srv.status = VIEWCHANGE
		reply.Success = true
		reply.Log = srv.log
		reply.LastNormalView = srv.lastNormalView
		log.Println("ViewUpdated", args.View, " log: ", srv.log)
		return
	} else {
		log.Println("false")
		reply.Success = false
		return
	}

}

// StartView is the RPC handler to process StartView RPC.
func (srv *PBServer) StartView(args *StartViewArgs, reply *StartViewReply) {
	// Your code here
	log.Println("in StartView", "args.View:", args.View, "CurrentView", srv.currentView)
	if args.View >= srv.currentView {
		srv.status = NORMAL
		srv.log = args.Log //verify it
		srv.currentView = args.View
		//srv.lastNormalView=args.View
		return
	} else {
		log.Println("StartView ELSE-------------")
	}

	//srv.status= NORMAL
	//srv.log=args.Log
	//srv.currentView=args.View

}
