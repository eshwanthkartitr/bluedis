package main

import (
	"log/slog"
	"net"
)

const defaultListenAddr = ":5301"

type Config struct {
    ListenAddr string
}

type Server struct {
    Config 
    peers map[*Peer]bool
    ln net.Listener
}

func NewServer(cfg Config) *Server {
    if len(cfg.ListenAddr) == 0 {
        cfg.ListenAddr = defaultListenAddr
    }
    return &Server{
        Config: cfg,
        peers: make(map[*Peer]bool),
    }
}

func (s *Server) Start() error {
    ln, err := net.Listen("tcp", s.Config.ListenAddr)
    if err != nil {
        return err
    }
    s.ln = ln
    return s.acceptLoop()
}

func (s *Server) acceptLoop() error {
    for {
        conn, err := s.ln.Accept() 
        if err != nil {
            slog.Error("Accept error", "err", err)
            continue
        }
        go s.handleConn()
    }
}

func (s *Server) handleConn(conn net.Conn) {

}

func main() {

}
