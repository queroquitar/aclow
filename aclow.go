package aclow

import (
	"fmt"
	"log"
	"net/url"
	"runtime/debug"
	"sync"

	"github.com/google/uuid"
)

type StartOptions struct {
	Local         bool
	Debug         bool
	Host          string
	Port          int
	ClusterPort   int
	ClusterRoutes []*url.URL
}

type Caller = func(a string, d Message) (Message, error)

type Node interface {
	Address() []string
	Start(app *App)
	Execute(msg Message, call Caller) (Message, error)
}

type App struct {
	opt StartOptions
	// Deprecated: remote execution was removed; kept only for compatibility.
	Conn          interface{}
	Config        map[string]interface{}
	Resources     map[string]interface{}
	SyncResources *sync.Map
	NodeMap       map[string]Node
	Logger        Logger
	OnError       func(address string, msg Message, err error)
	resourcesMu   sync.RWMutex
}

type Message struct {
	Header map[string]interface{}
	Body   interface{}
}

type ReplyMessage struct {
	Message
	Err error
}

func (a *App) Start(opt StartOptions) {
	a.opt = opt
	a.opt.Local = true
	a.Config = make(map[string]interface{})
	a.Resources = make(map[string]interface{})
	a.SyncResources = &sync.Map{}
	a.NodeMap = make(map[string]Node)

	a.Logger.start()
}

func (a *App) Wait() {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	wg.Wait()
}

func (a *App) Publish(address string, msg Message) {
	localNode := a.NodeMap[address]

	if localNode == nil {
		err := fmt.Errorf("Address '%s' not found!", address)
		log.Println(err)
		return
	}

	if _, err := a.executeLocalNode(address, msg, localNode, false); err != nil {
		log.Println("Error executing:", address)
		log.Println(string(debug.Stack()))
	}
}

func (a *App) Call(address string, msg Message) (r Message, err error) {
	localNode := a.NodeMap[address]
	if localNode == nil {
		err := fmt.Errorf("Address '%s' not found!", address)
		if a.OnError != nil {
			go func() { a.OnError(address, msg, err) }()
		}
		return Message{}, err
	}

	reply, err := a.executeLocalNode(address, msg, localNode, true)
	if err != nil {
		log.Println("Error executing:", address, " => ", err.Error())
		log.Println(string(debug.Stack()))
	}

	return reply, err
}

func (a *App) RegisterModule(moduleName string, nodes []Node) {
	for _, n := range nodes {
		for _, addr := range n.Address() {
			nodeAddress := moduleName + "@" + addr

			a.NodeMap[nodeAddress] = n
		}

		go n.Start(a)
	}
}

func (a *App) SetResource(key string, value interface{}) {
	a.resourcesMu.Lock()
	defer a.resourcesMu.Unlock()

	if a.Resources == nil {
		a.Resources = make(map[string]interface{})
	}
	if a.SyncResources == nil {
		a.SyncResources = &sync.Map{}
	}

	a.Resources[key] = value
	a.SyncResources.Store(key, value)
}

func (a *App) GetResource(key string) (interface{}, bool) {
	if a.SyncResources != nil {
		if value, ok := a.SyncResources.Load(key); ok {
			return value, true
		}
	}

	a.resourcesMu.RLock()
	defer a.resourcesMu.RUnlock()

	value, ok := a.Resources[key]
	return value, ok
}

func (a *App) DeleteResource(key string) {
	a.resourcesMu.Lock()
	defer a.resourcesMu.Unlock()

	if a.Resources != nil {
		delete(a.Resources, key)
	}
	if a.SyncResources != nil {
		a.SyncResources.Delete(key)
	}
}

func (a *App) executeLocalNode(address string, msg Message, localNode Node, notifyOnError bool) (reply Message, err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Println("Recovered:", r)
			log.Println(string(debug.Stack()))
			err = fmt.Errorf("Panic error on call: %v %v \n %v", localNode.Address(), r, string(debug.Stack()))
			if a.OnError != nil {
				go func() { a.OnError(address, msg, err) }()
			}
		}
	}()

	executionID := uuid.New().String()
	a.logIt(Log{
		logType:     "starting-execution",
		executionID: executionID,
		address:     address,
		message:     msg,
	})

	reply, err = localNode.Execute(msg, a.makeCaller(address, executionID))

	a.logIt(Log{
		logType:     "ending-execution",
		executionID: executionID,
		address:     address,
		message:     reply,
		err:         err,
	})

	if err != nil && notifyOnError && a.OnError != nil {
		go func() { a.OnError(address, msg, err) }()
	}

	return reply, err
}

func (a *App) makeCaller(fromAddress string, executionID string) Caller {
	return func(address string, m Message) (Message, error) {
		a.logIt(Log{
			logType:          "starting-call",
			executionID:      executionID,
			executionAddress: fromAddress,
			address:          address,
			message:          m,
		})
		reply, err := a.Call(address, m)
		//log.Println(err)
		a.logIt(Log{
			logType:          "receiving-call-response",
			executionID:      executionID,
			executionAddress: fromAddress,
			address:          address,
			message:          reply,
			err:              err,
		})
		return reply, err
	}
}

func (a *App) logIt(logMsg Log) {
	if a.opt.Debug {
		a.Logger.logIt(logMsg)
	}
}

func BodyAsString(r Message, err error) string {
	check(err)
	return r.Body.(string)
}

func BodyAsFloat64(r Message, err error) float64 {
	check(err)
	return r.Body.(float64)
}

func BodyAsInt64(r Message, err error) int64 {
	check(err)
	return r.Body.(int64)
}

func BodyAsBool(r Message, err error) bool {
	check(err)
	return r.Body.(bool)
}

func Body(r Message, err error) interface{} {
	check(err)
	return r.Body
}

func check(err error) {
	if err != nil {
		log.Panic(err)
	}
}

type Tuple []interface{}
