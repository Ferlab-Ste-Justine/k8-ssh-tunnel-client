package ssh

import (
	"errors" 
    "fmt"
    "io"
    "net"
    "golang.org/x/crypto/ssh"
	"encoding/hex"
)

type SshTunnel struct {
    LocalFrontendUrl  string
    SshServerUrl      string
    RemoteBackendUrl  string
    Config            *ssh.ClientConfig
    frontendListener  net.Listener
    sshConn           *ssh.Client
    listening         bool
}

func (tunnel *SshTunnel) IsClosed() bool {
    return !tunnel.listening
}

func (tunnel *SshTunnel) Close() {
    tunnel.listening = false
    tunnel.frontendListener.Close()
    tunnel.sshConn.Close()
}

func (tunnel *SshTunnel) Init() error {
    var err error

    fmt.Printf("Listening on local frontend URL: %s\n", tunnel.LocalFrontendUrl)
    tunnel.frontendListener, err = net.Listen("tcp", tunnel.LocalFrontendUrl)
    if err != nil {
        return err
    }

    fmt.Printf("Connecting to SSH server: %s\n", tunnel.SshServerUrl)
    tunnel.sshConn, err = ssh.Dial("tcp", tunnel.SshServerUrl, tunnel.Config)
    if err != nil {
        tunnel.frontendListener.Close()
        return err
    }
    tunnel.listening = true

    return nil
}

func (tunnel *SshTunnel) Listen() error {
    for {
        frontendConn, err := tunnel.frontendListener.Accept()
        if err != nil {
            if tunnel.listening {
                return err
            }
            return nil
        }
        go tunnel.forwardFrontendToBackend(frontendConn)
    }
}

func pipeConn(writer net.Conn, reader net.Conn, c chan error, direction string) {
    defer writer.Close()
    defer reader.Close()

    buf := make([]byte, 1024) // Adjust buffer size as needed
    for {
        n, err := reader.Read(buf)
        if err != nil {
            if err != io.EOF {
                fmt.Printf("Error reading data (%s): %v\n", direction, err)
                c <- err
            }
            break
        }

        // Log the amount of data and first few bytes (in hex) for inspection
        fmt.Printf("Transferring %d bytes (%s): %s...\n", n, direction, hex.EncodeToString(buf[:min(n, 10)]))

        // Write the data to the other connection
        _, err = writer.Write(buf[:n])
        if err != nil {
            fmt.Printf("Error writing data (%s): %v\n", direction, err)
            c <- err
            break
        }
    }

    c <- nil
}

func min(a, b int) int {
    if a < b {
        return a
    }
    return b
}

func (tunnel *SshTunnel) forwardFrontendToBackend(frontendConn net.Conn) {
    fmt.Printf("Connecting to remote backend URL: %s\n", tunnel.RemoteBackendUrl)
    backendConn, err := tunnel.sshConn.Dial("tcp", tunnel.RemoteBackendUrl)
    if err != nil {
        fmt.Printf("Backend dial error: %s\n", err)
        fmt.Printf("Failed to connect to backend URL: %s\n", tunnel.RemoteBackendUrl)
        fmt.Printf("Error details: %v\n", err)
        frontendConn.Close()
        return
    }

    fmt.Println("Successfully connected to backend.")
    pipeErrChan := make(chan error)

    go pipeConn(frontendConn, backendConn, pipeErrChan, "frontend to backend")
    go pipeConn(backendConn, frontendConn, pipeErrChan, "backend to frontend")

    err = <-pipeErrChan
    if err != nil && !errors.Is(err, net.ErrClosed) {
        fmt.Printf("Error in pipeConn (first goroutine): %s\n", err)
    }

    err = <-pipeErrChan
    if err != nil && !errors.Is(err, net.ErrClosed) {
        fmt.Printf("Error in pipeConn (second goroutine): %s\n", err)
    }
}
