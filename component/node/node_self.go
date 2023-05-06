package node

import (
	"errors"
	"log"
	"math"
	"net/http"
	"sync"

	NodeInfo "github.com/rhosocial/go-rush-producer/models/node_info"
)

type PoolSelf struct {
	Identity    uint8
	Node        *NodeInfo.NodeInfo
	Alive       uint8
	AliveRWLock sync.RWMutex
}

func (ps *PoolSelf) SetLevel(level uint8) {
	ps.Node.Level = level
}

func (ps *PoolSelf) Upgrade() {
	ps.Node.Level -= 1
}

func (ps *PoolSelf) Downgrade() {
	ps.Node.Level += 1
}

var ErrNodeMasterInvalid = errors.New("master node invalid")
var ErrNodeMasterValidButRefused = errors.New("master is valid but refuse to communicate")

var ErrNodeMasterIsSelf = errors.New("master node is self")
var ErrNodeMasterExisted = errors.New("a valid master node with the same socket already exists")
var ErrNodeExisted = errors.New(" a valid node with same socket already exists")

// CheckMaster 检查主节点有效性。如果有效，则返回 nil。
// 如果指定主节点不存在，则报 ErrNodeMasterInvalid。
//
// 判断 master 的套接字是否与自己相同。
//
// 1. 如果相同，则认为是自己，报 ErrNodeMasterIsSelf。
//
// 2. 如果不同，则认为主节点是另一个进程。尝试与其沟通，参见 CheckMasterWithRequest。
func (n *Pool) CheckMaster(master *NodeInfo.NodeInfo) error {
	if master == nil {
		log.Println("Master not specified")
		return ErrNodeMasterInvalid
	}
	if n.Self.Node.IsSocketEqual(master) {
		return ErrNodeMasterIsSelf
	}
	return n.CheckMasterWithRequest(master)
}

// CheckMasterWithRequest 发送请求查询主节点状态。
//
// 如果指定主节点不存在，则报 ErrNodeMasterInvalid。
//
// 1. 如果请求构造出错，则报 ErrNodeRequestInvalid。
//
// 2. 如果请求构造成功，请求响应失败，则报 ErrNodeRequestResponseError。
//
// 3. 如果 Socket 相同，则认为主节点已存在，报 ErrNodeMasterExisted。
//
// 4. 如果状态码不是 200 OK，则认为主节点有效，但拒绝。
//
// 其它情况没有任何错误。
func (n *Pool) CheckMasterWithRequest(master *NodeInfo.NodeInfo) error {
	if master == nil {
		log.Println("Master not specified")
		return ErrNodeMasterInvalid
	}
	log.Printf("Checking Master [ID: %d - %s]...\n", master.ID, master.Socket())
	resp, err := n.SendRequestMasterStatus(master)
	if errors.Is(err, ErrNodeRequestInvalid) {
		log.Println(err)
		return ErrNodeRequestInvalid
	}
	if err != nil {
		log.Println(err)
		return ErrNodeRequestResponseError
	}
	// 此时目标主节点网络正常。
	// 若与自己套接字相同，则视为已存在。
	if n.Self.Node.IsSocketEqual(master) {
		return ErrNodeMasterExisted
	}
	if resp.StatusCode != http.StatusOK {
		var body = make([]byte, resp.ContentLength)
		if _, err := resp.Body.Read(body); err != nil {
			return ErrNodeRequestResponseError
		}
		log.Println(string(body))
		return ErrNodeMasterValidButRefused
	}
	return nil
}

// AliveUpAndClearIf 活跃次数调升，并在达到阈值时清零。也即调用后返回值不会大于 threshold。
// 注意，本次调用不能被其它操作 Alive 的方法嵌套调用，以防死锁。
func (ps *PoolSelf) AliveUpAndClearIf(threshold uint8) uint8 {
	ps.AliveRWLock.Lock()
	defer ps.AliveRWLock.Unlock()
	if ps.Alive < math.MaxUint8 {
		ps.Alive += 1
	}
	if ps.Alive >= threshold {
		ps.Alive = 0
	}
	return ps.Alive
}
