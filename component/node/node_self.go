package node

import (
	"errors"
	models "github.com/rhosocial/go-rush-producer/models/node_info"
	"log"
	"net/http"
)

type PoolSelf struct {
	Identity uint8
	Node     *models.NodeInfo
}

func (ps *PoolSelf) SetLevel(level uint8) {
	ps.Node.Level = level
}

func (ps *PoolSelf) Downgrade() {
	ps.Node.Level -= 1
}

func (ps *PoolSelf) Upgrade() {
	ps.Node.Level += 1
}

var ErrNodeMasterInvalid = errors.New("master node invalid")
var ErrNodeMasterValidButRefused = errors.New("master is valid but refuse to communicate")

var ErrNodeMasterIsSelf = errors.New("master node is self")
var ErrNodeMasterExisted = errors.New("a valid master node with the same socket already exists")

// CheckMaster 检查主节点有效性。如果有效，则返回 nil。
// 如果指定主节点不存在，则报 ErrNodeMasterInvalid。
//
// 判断 master 的套接字是否与自己相同。
//
// 1. 如果相同，则认为是自己，报 ErrNodeMasterIsSelf。
//
// 2. 如果不同，则认为主节点是另一个进程。尝试与其沟通，参见 CheckMasterWithRequest。
func (ps *PoolSelf) CheckMaster(master *models.NodeInfo) error {
	if master == nil {
		log.Println("Master not specified")
		return ErrNodeMasterInvalid
	}
	if ps.Node.IsSocketEqual(master) {
		return ErrNodeMasterIsSelf
	}
	return ps.CheckMasterWithRequest(master)
}

// CheckMasterWithRequest 发送请求查询主节点状态。
//
// 如果指定主节点不存在，则报 ErrNodeMasterInvalid。
//
// 1. 如果请求构造出错，则报 ErrNodeRequestInvalid。
//
// 2. 如果 Socket 相同，则认为主节点已存在，报 ErrNodeMasterExisted。
//
// 3. 如果状态码不是 200 OK，则认为主节点有效，但拒绝。
//
// 其它情况没有任何错误。
func (ps *PoolSelf) CheckMasterWithRequest(master *models.NodeInfo) error {
	if master == nil {
		log.Println("Master not specified")
		return ErrNodeMasterInvalid
	}
	log.Printf("Checking Master [ID: %d - %s]...\n", master.ID, master.Socket())
	resp, err := SendRequestMasterStatus(master)
	if err != nil {
		log.Println(err)
		return ErrNodeRequestInvalid
	}
	// 此时目标主节点网络正常。
	// 若与自己套接字相同，则视为已存在。
	if ps.Node.IsSocketEqual(master) {
		return ErrNodeMasterExisted
	}
	if resp.StatusCode != http.StatusOK {
		var body = make([]byte, resp.ContentLength)
		resp.Body.Read(body)
		log.Println(string(body))
		return ErrNodeMasterValidButRefused
	}
	return nil
}
