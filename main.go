package main

import (
	"fmt"
	"github.com/gin-gonic/gin"
	commonComponent "github.com/rhosocial/go-rush-common/component"
	"github.com/rhosocial/go-rush-producer/component"
	controllerSystem "github.com/rhosocial/go-rush-producer/controllers/system"
	models "github.com/rhosocial/go-rush-producer/models/node_info"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"log"
)

var r *gin.Engine
var db *gorm.DB
var dsn = "root:Python4096@tcp(1.n.rho.im:13406)/node_demo?charset=utf8mb4&parseTime=true&loc=Local"
var ListenPort = uint16(38082)

func main() {
	log.Println("Hello, World!")
	var err error
	db, err = gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalln(err)
	}
	models.NodeInfoDB = db
	self := models.NodeInfo{
		Name:        "GO-RUSH_PRODUCER",
		NodeVersion: "0.0.1",
		Port:        ListenPort,
		Level:       1,
	}
	component.Nodes = component.NewNodePool(&self)
	masterNode, err := component.Nodes.DiscoverMasterNode()
	if err == models.ErrNodeSuperiorNotExist {
		// Switch identity to master.
		component.Nodes.CommitSelfAsMasterNode()
	} else if err != nil {
		log.Fatalln(err)
	}
	if masterNode == nil {
	} else {
		component.Nodes.Master = masterNode
		// Check `masterNode` if valid:
		err = component.Nodes.CheckMaster(masterNode)
		if err == nil {
			// Regard `masterNode` as my master:
			component.Nodes.AcceptMaster(masterNode)
			// Notify master to add this node as its slave:
			_, err := component.Nodes.NotifyMasterToAddSelfAsSlave()
			if err != nil {
				log.Fatalln(err)
			}
		} else if err == component.ErrNodeMasterIsSelf {
			// I am already a master node.
			// Switch identity to master.
			component.Nodes.Self = masterNode
			component.Nodes.SwitchIdentityToMaster()
		} else if err == component.ErrNodeMasterExisted {
			// A valid master node with the same socket already exists. Exiting.
			log.Fatalln(err)
		}
	}
	// For-loop
	if component.Nodes.Identity == component.NodeIdentityNotDetermined {
		// Wait for a minute, and retry to determine the identity.
		log.Println("Identity: Not determined.")
	}
	if component.Nodes.Identity == component.NodeIdentityMaster {
		// Start a goroutine to monitor its master.
		log.Println("Identity: Master")
		log.Printf("Self  : %s\n", component.Nodes.Self.Log())
	}
	if component.Nodes.Identity == component.NodeIdentitySlave {
		// Start a goroutine to monitor its slaves.
		log.Println("Identity: Slave.")
		log.Printf("Self  : %s", component.Nodes.Self.Log())
		log.Printf("Master: %s", component.Nodes.Master.Log())
	}
	r = gin.New()
	if !configEngine(r) {
		return
	}
	r.Run(fmt.Sprintf(":%d", ListenPort))
}

func configEngine(r *gin.Engine) bool {
	r.Use(
		commonComponent.AppendRequestID(),
		gin.LoggerWithFormatter(commonComponent.LogFormatter),
		commonComponent.AuthRequired(),
		gin.Recovery(),
		commonComponent.ErrorHandler(),
	)
	var ca controllerSystem.ControllerServer
	ca.RegisterActions(r)
	return true
}
