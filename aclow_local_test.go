package aclow

import "testing"

type echoNode struct{}

func (n *echoNode) Address() []string { return []string{"echo"} }

func (n *echoNode) Start(app *App) {}

func (n *echoNode) Execute(msg Message, call Caller) (Message, error) {
	return Message{Header: msg.Header, Body: msg.Body}, nil
}

func TestStartInitializesSyncResources(t *testing.T) {
	app := &App{}
	app.Start(StartOptions{})

	if app.Resources == nil {
		t.Fatal("Resources should be initialized")
	}

	if app.SyncResources == nil {
		t.Fatal("SyncResources should be initialized")
	}

	app.SetResource("answer", 42)

	if got, ok := app.GetResource("answer"); !ok || got != 42 {
		t.Fatalf("GetResource() = (%v, %v), want (42, true)", got, ok)
	}

	if got := app.Resources["answer"]; got != 42 {
		t.Fatalf("Resources[answer] = %v, want 42", got)
	}

	if got, ok := app.SyncResources.Load("answer"); !ok || got != 42 {
		t.Fatalf("SyncResources.Load(answer) = (%v, %v), want (42, true)", got, ok)
	}

	app.DeleteResource("answer")

	if _, ok := app.GetResource("answer"); ok {
		t.Fatal("resource should have been deleted")
	}
}

func TestCallUsesLocalExecutionEvenWhenLocalFlagIsFalse(t *testing.T) {
	app := &App{}
	app.Start(StartOptions{Local: false})
	app.RegisterModule("module", []Node{&echoNode{}})

	reply, err := app.Call("module@echo", Message{Body: "ok"})
	if err != nil {
		t.Fatalf("Call() error = %v", err)
	}

	if reply.Body != "ok" {
		t.Fatalf("Call() body = %v, want ok", reply.Body)
	}
}
