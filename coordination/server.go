package coordination

type CoordinationServer struct {
	Address string
	Servers []string
}

func (coordinator *CoordinationServer) NewCoordinator() *CoordinationServer {
	return &CoordinationServer{
		Address: "localhost:8880",
		Servers: make([]string, 0),
	}
}

// create a new http server
// start the HTTP server in a separate goroutine
// should have the /register endpoint for servers to ask for Node Address
// should have a /healthcheck endpoint for servers to do a healthcheck
