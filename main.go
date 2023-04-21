package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/gin-gonic/gin"
	"github.com/rhosocial/go-rush-common/component/auth"
	error2 "github.com/rhosocial/go-rush-common/component/error"
	"github.com/rhosocial/go-rush-common/component/logger"
	"github.com/rhosocial/go-rush-producer/component"
	"github.com/rhosocial/go-rush-producer/component/node"
	controllerSystem "github.com/rhosocial/go-rush-producer/controllers/server"
	models "github.com/rhosocial/go-rush-producer/models/node_info"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

var r *gin.Engine
var db *gorm.DB
var dsn = "root:Python4096@tcp(1.n.rho.im:13406)/node_demo?charset=utf8mb4&parseTime=true&loc=Local"

func main() {
	log.Println("Hello, World!")
	SetupCloseHandler()
	var err error
	// 最初初始化所有配置参数为默认值。
	err = component.LoadEnvDefault()
	if err != nil {
		println(err.Error())
		return
	}
	// 再从环境变量中加载配置信息。
	if err := component.LoadEnvFromSystemEnvVar(); err != nil {
		println(err.Error())
	}
	db, err = gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalln(err)
	}
	models.NodeInfoDB = db
	self := models.NodeInfo{
		Name:        "GO-RUSH-PRODUCER",
		NodeVersion: "0.0.1",
		Port:        *(*(*component.GlobalEnv).Net).ListenPort,
		Level:       1,
	}
	node.Nodes = node.NewNodePool(&self)
	masterNode, err := node.Nodes.DiscoverMasterNode()
	if errors.Is(err, models.ErrNodeSuperiorNotExist) {
		// Switch identity to master.
		node.Nodes.CommitSelfAsMasterNode()
	} else if err != nil {
		log.Fatalln(err)
	}
	if masterNode == nil {
	} else {
		node.Nodes.Master = masterNode
		// Check `masterNode` if valid:
		err = node.Nodes.CheckMaster(masterNode)
		if err == nil {
			// Regard `masterNode` as my master:
			node.Nodes.AcceptMaster(masterNode)
			// Notify master to add this node as its slave:
			_, err := node.Nodes.NotifyMasterToAddSelfAsSlave()
			if err != nil {
				log.Fatalln(err)
			}
		} else if errors.Is(err, node.ErrNodeMasterIsSelf) {
			// I am already a master node.
			// Switch identity to master.
			node.Nodes.Self = masterNode
			node.Nodes.Master = node.Nodes.Self
			node.Nodes.SwitchIdentityMasterOn()
		} else if errors.Is(err, node.ErrNodeMasterExisted) {
			// A valid master node with the same socket already exists. Exiting.
			log.Fatalln(err)
		} else if errors.Is(err, node.ErrNodeMasterInvalid) {
			log.Fatalln(err)
		} else if errors.Is(err, node.ErrNodeRequestInvalid) {
			log.Fatalln(err)
		}
	}
	// For-loop
	if node.Nodes.Identity == node.IdentityNotDetermined {
		// Wait for a minute, and retry to determine the identity.
		log.Println("Identity: Not determined.")
	}
	if node.Nodes.Identity == node.IdentityMaster {
		// Start a goroutine to monitor its master.
		log.Println("Identity: Master")
		log.Printf("Self  : %s\n", node.Nodes.Self.Log())
	}
	if node.Nodes.Identity == node.IdentitySlave {
		// Start a goroutine to monitor its slaves.
		log.Println("Identity: Slave.")
		log.Printf("Master: %s", node.Nodes.Master.Log())
		log.Printf("Self  : %s", node.Nodes.Self.Log())
	}
	r = gin.New()
	if !configEngine(r) {
		return
	}
	r.Run(fmt.Sprintf(":%d", *(*(*component.GlobalEnv).Net).ListenPort))
}

func configEngine(r *gin.Engine) bool {
	r.Use(
		logger.AppendRequestID(),
		gin.LoggerWithFormatter(logger.LogFormatter),
		auth.AuthRequired(),
		gin.Recovery(),
		error2.ErrorHandler(),
	)
	var ca controllerSystem.ControllerServer
	ca.RegisterActions(r)
	return true
}

// SetupCloseHandler creates a 'listener' on a new goroutine which will notify the
// program if it receives an interrupt from the OS. We then handle this by calling
// our cleaning-up procedure and exiting the program.
func SetupCloseHandler() {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, os.Kill, syscall.SIGTERM, syscall.SIGKILL)
	go func() {
		<-c
		log.Println("\r- Ctrl+C pressed in Terminal")
		node.Nodes.Stop()
		os.Exit(0)
	}()
}
