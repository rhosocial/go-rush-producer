package main

import (
	"fmt"
	"log"
	"net"
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

func tryBindListenPort(addr string) error {
	if listen, err := net.Listen("tcp", addr); err != nil {
		return err
	} else {
		listen.Close()
	}
	return nil
}

func main() {
	log.Println("Hello, World!")
	SetupCloseHandler()
	// 最初初始化所有配置参数为默认值。
	if err := component.LoadEnvDefault(); err != nil {
		log.Println(err.Error())
	}
	if err := component.LoadEnvFromYaml("default.yaml"); err != nil {
		log.Println(err.Error())
	}
	// 再从环境变量中加载配置信息。
	if err := component.LoadEnvFromSystemEnvVar(); err != nil {
		log.Println(err.Error())
	}
	// 尝试监听端口。
	if tryBindListenPort(fmt.Sprintf(":%d", *(*(*component.GlobalEnv).Net).ListenPort)) != nil {
		log.Fatalf("Cannot bind the listening port: %d\n", *(*(*component.GlobalEnv).Net).ListenPort)
		return
	}
	db, err := gorm.Open(mysql.Open((*(*component.GlobalEnv).MySQLServers)[0].GetDSN()), &gorm.Config{})
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
	node.Nodes.Start((*component.GlobalEnv).Identity)
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
