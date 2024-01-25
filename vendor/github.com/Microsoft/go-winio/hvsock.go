// +build windows

package winio

import (
	"fmt"
	"io"
	"net"
	"os"
	"syscall"
	"time"
	"unsafe"

	"github.com/Microsoft/go-winio/pkg/guid"
)

//sys bind(s syscall.Handle, name unsafe.Pointer, namelen int32) (err error) [failretval==socketError] = ws2_32.bind

<<<<<<< HEAD
const (
	afHvSock = 34 // AF_HYPERV

	socketError = ^uintptr(0)
)
=======
// Well known Service and VM IDs
// https://docs.microsoft.com/en-us/virtualization/hyper-v-on-windows/user-guide/make-integration-service#vmid-wildcards

// HvsockGUIDWildcard is the wildcard VmId for accepting connections from all partitions.
func HvsockGUIDWildcard() guid.GUID { // 00000000-0000-0000-0000-000000000000
	return guid.GUID{}
}

// HvsockGUIDBroadcast is the wildcard VmId for broadcasting sends to all partitions.
func HvsockGUIDBroadcast() guid.GUID { // ffffffff-ffff-ffff-ffff-ffffffffffff
	return guid.GUID{
		Data1: 0xffffffff,
		Data2: 0xffff,
		Data3: 0xffff,
		Data4: [8]uint8{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
	}
}

// HvsockGUIDLoopback is the Loopback VmId for accepting connections to the same partition as the connector.
func HvsockGUIDLoopback() guid.GUID { // e0e16197-dd56-4a10-9195-5ee7a155a838
	return guid.GUID{
		Data1: 0xe0e16197,
		Data2: 0xdd56,
		Data3: 0x4a10,
		Data4: [8]uint8{0x91, 0x95, 0x5e, 0xe7, 0xa1, 0x55, 0xa8, 0x38},
	}
}

// HvsockGUIDSiloHost is the address of a silo's host partition:
//   - The silo host of a hosted silo is the utility VM.
//   - The silo host of a silo on a physical host is the physical host.
func HvsockGUIDSiloHost() guid.GUID { // 36bd0c5c-7276-4223-88ba-7d03b654c568
	return guid.GUID{
		Data1: 0x36bd0c5c,
		Data2: 0x7276,
		Data3: 0x4223,
		Data4: [8]byte{0x88, 0xba, 0x7d, 0x03, 0xb6, 0x54, 0xc5, 0x68},
	}
}

// HvsockGUIDChildren is the wildcard VmId for accepting connections from the connector's child partitions.
func HvsockGUIDChildren() guid.GUID { // 90db8b89-0d35-4f79-8ce9-49ea0ac8b7cd
	return guid.GUID{
		Data1: 0x90db8b89,
		Data2: 0xd35,
		Data3: 0x4f79,
		Data4: [8]uint8{0x8c, 0xe9, 0x49, 0xea, 0xa, 0xc8, 0xb7, 0xcd},
	}
}

// HvsockGUIDParent is the wildcard VmId for accepting connections from the connector's parent partition.
// Listening on this VmId accepts connection from:
//   - Inside silos: silo host partition.
//   - Inside hosted silo: host of the VM.
//   - Inside VM: VM host.
//   - Physical host: Not supported.
func HvsockGUIDParent() guid.GUID { // a42e7cda-d03f-480c-9cc2-a4de20abb878
	return guid.GUID{
		Data1: 0xa42e7cda,
		Data2: 0xd03f,
		Data3: 0x480c,
		Data4: [8]uint8{0x9c, 0xc2, 0xa4, 0xde, 0x20, 0xab, 0xb8, 0x78},
	}
}

// hvsockVsockServiceTemplate is the Service GUID used for the VSOCK protocol.
func hvsockVsockServiceTemplate() guid.GUID { // 00000000-facb-11e6-bd58-64006a7986d3
	return guid.GUID{
		Data2: 0xfacb,
		Data3: 0x11e6,
		Data4: [8]uint8{0xbd, 0x58, 0x64, 0x00, 0x6a, 0x79, 0x86, 0xd3},
	}
}
>>>>>>> 181c414a (Support proxy connections over TLS)

// An HvsockAddr is an address for a AF_HYPERV socket.
type HvsockAddr struct {
	VMID      guid.GUID
	ServiceID guid.GUID
}

type rawHvsockAddr struct {
	Family    uint16
	_         uint16
	VMID      guid.GUID
	ServiceID guid.GUID
}

// Network returns the address's network name, "hvsock".
func (addr *HvsockAddr) Network() string {
	return "hvsock"
}

func (addr *HvsockAddr) String() string {
	return fmt.Sprintf("%s:%s", &addr.VMID, &addr.ServiceID)
}

// VsockServiceID returns an hvsock service ID corresponding to the specified AF_VSOCK port.
func VsockServiceID(port uint32) guid.GUID {
	g, _ := guid.FromString("00000000-facb-11e6-bd58-64006a7986d3")
	g.Data1 = port
	return g
}

func (addr *HvsockAddr) raw() rawHvsockAddr {
	return rawHvsockAddr{
		Family:    afHvSock,
		VMID:      addr.VMID,
		ServiceID: addr.ServiceID,
	}
}

func (addr *HvsockAddr) fromRaw(raw *rawHvsockAddr) {
	addr.VMID = raw.VMID
	addr.ServiceID = raw.ServiceID
}

// HvsockListener is a socket listener for the AF_HYPERV address family.
type HvsockListener struct {
	sock *win32File
	addr HvsockAddr
}

// HvsockConn is a connected socket of the AF_HYPERV address family.
type HvsockConn struct {
	sock          *win32File
	local, remote HvsockAddr
}

func newHvSocket() (*win32File, error) {
	fd, err := syscall.Socket(afHvSock, syscall.SOCK_STREAM, 1)
	if err != nil {
		return nil, os.NewSyscallError("socket", err)
	}
	f, err := makeWin32File(fd)
	if err != nil {
		syscall.Close(fd)
		return nil, err
	}
	f.socket = true
	return f, nil
}

// ListenHvsock listens for connections on the specified hvsock address.
func ListenHvsock(addr *HvsockAddr) (_ *HvsockListener, err error) {
	l := &HvsockListener{addr: *addr}
	sock, err := newHvSocket()
	if err != nil {
		return nil, l.opErr("listen", err)
	}
	sa := addr.raw()
	err = bind(sock.handle, unsafe.Pointer(&sa), int32(unsafe.Sizeof(sa)))
	if err != nil {
		return nil, l.opErr("listen", os.NewSyscallError("socket", err))
	}
	err = syscall.Listen(sock.handle, 16)
	if err != nil {
		return nil, l.opErr("listen", os.NewSyscallError("listen", err))
	}
	return &HvsockListener{sock: sock, addr: *addr}, nil
}

func (l *HvsockListener) opErr(op string, err error) error {
	return &net.OpError{Op: op, Net: "hvsock", Addr: &l.addr, Err: err}
}

// Addr returns the listener's network address.
func (l *HvsockListener) Addr() net.Addr {
	return &l.addr
}

// Accept waits for the next connection and returns it.
func (l *HvsockListener) Accept() (_ net.Conn, err error) {
	sock, err := newHvSocket()
	if err != nil {
		return nil, l.opErr("accept", err)
	}
	defer func() {
		if sock != nil {
			sock.Close()
		}
	}()
	c, err := l.sock.prepareIo()
	if err != nil {
		return nil, l.opErr("accept", err)
	}
	defer l.sock.wg.Done()

	// AcceptEx, per documentation, requires an extra 16 bytes per address.
	const addrlen = uint32(16 + unsafe.Sizeof(rawHvsockAddr{}))
	var addrbuf [addrlen * 2]byte

	var bytes uint32
<<<<<<< HEAD
	err = syscall.AcceptEx(l.sock.handle, sock.handle, &addrbuf[0], 0, addrlen, addrlen, &bytes, &c.o)
	_, err = l.sock.asyncIo(c, nil, bytes, err)
	if err != nil {
=======
	err = syscall.AcceptEx(l.sock.handle, sock.handle, &addrbuf[0], 0 /* rxdatalen */, addrlen, addrlen, &bytes, &c.o)
	if _, err = l.sock.asyncIO(c, nil, bytes, err); err != nil {
>>>>>>> 181c414a (Support proxy connections over TLS)
		return nil, l.opErr("accept", os.NewSyscallError("acceptex", err))
	}
	conn := &HvsockConn{
		sock: sock,
	}
	conn.local.fromRaw((*rawHvsockAddr)(unsafe.Pointer(&addrbuf[0])))
	conn.remote.fromRaw((*rawHvsockAddr)(unsafe.Pointer(&addrbuf[addrlen])))
	sock = nil
	return conn, nil
}

// Close closes the listener, causing any pending Accept calls to fail.
func (l *HvsockListener) Close() error {
	return l.sock.Close()
}

/* Need to finish ConnectEx handling
func DialHvsock(ctx context.Context, addr *HvsockAddr) (*HvsockConn, error) {
	sock, err := newHvSocket()
	if err != nil {
		return nil, err
	}
	defer func() {
		if sock != nil {
			sock.Close()
		}
	}()
	c, err := sock.prepareIo()
	if err != nil {
		return nil, err
	}
	defer sock.wg.Done()
	var bytes uint32
	err = windows.ConnectEx(windows.Handle(sock.handle), sa, nil, 0, &bytes, &c.o)
	_, err = sock.asyncIo(ctx, c, nil, bytes, err)
	if err != nil {
		return nil, err
	}
	conn := &HvsockConn{
		sock:   sock,
		remote: *addr,
	}
	sock = nil
	return conn, nil
}
*/

func (conn *HvsockConn) opErr(op string, err error) error {
	return &net.OpError{Op: op, Net: "hvsock", Source: &conn.local, Addr: &conn.remote, Err: err}
}

func (conn *HvsockConn) Read(b []byte) (int, error) {
	c, err := conn.sock.prepareIo()
	if err != nil {
		return 0, conn.opErr("read", err)
	}
	defer conn.sock.wg.Done()
	buf := syscall.WSABuf{Buf: &b[0], Len: uint32(len(b))}
	var flags, bytes uint32
	err = syscall.WSARecv(conn.sock.handle, &buf, 1, &bytes, &flags, &c.o, nil)
	n, err := conn.sock.asyncIo(c, &conn.sock.readDeadline, bytes, err)
	if err != nil {
		if _, ok := err.(syscall.Errno); ok {
			err = os.NewSyscallError("wsarecv", err)
		}
		return 0, conn.opErr("read", err)
	} else if n == 0 {
		err = io.EOF
	}
	return n, err
}

func (conn *HvsockConn) Write(b []byte) (int, error) {
	t := 0
	for len(b) != 0 {
		n, err := conn.write(b)
		if err != nil {
			return t + n, err
		}
		t += n
		b = b[n:]
	}
	return t, nil
}

func (conn *HvsockConn) write(b []byte) (int, error) {
	c, err := conn.sock.prepareIo()
	if err != nil {
		return 0, conn.opErr("write", err)
	}
	defer conn.sock.wg.Done()
	buf := syscall.WSABuf{Buf: &b[0], Len: uint32(len(b))}
	var bytes uint32
	err = syscall.WSASend(conn.sock.handle, &buf, 1, &bytes, 0, &c.o, nil)
	n, err := conn.sock.asyncIo(c, &conn.sock.writeDeadline, bytes, err)
	if err != nil {
		if _, ok := err.(syscall.Errno); ok {
			err = os.NewSyscallError("wsasend", err)
		}
		return 0, conn.opErr("write", err)
	}
	return n, err
}

// Close closes the socket connection, failing any pending read or write calls.
func (conn *HvsockConn) Close() error {
	return conn.sock.Close()
}

func (conn *HvsockConn) shutdown(how int) error {
	err := syscall.Shutdown(conn.sock.handle, syscall.SHUT_RD)
	if err != nil {
		return os.NewSyscallError("shutdown", err)
	}
	return nil
}

// CloseRead shuts down the read end of the socket.
func (conn *HvsockConn) CloseRead() error {
	err := conn.shutdown(syscall.SHUT_RD)
	if err != nil {
		return conn.opErr("close", err)
	}
	return nil
}

// CloseWrite shuts down the write end of the socket, notifying the other endpoint that
// no more data will be written.
func (conn *HvsockConn) CloseWrite() error {
	err := conn.shutdown(syscall.SHUT_WR)
	if err != nil {
		return conn.opErr("close", err)
	}
	return nil
}

// LocalAddr returns the local address of the connection.
func (conn *HvsockConn) LocalAddr() net.Addr {
	return &conn.local
}

// RemoteAddr returns the remote address of the connection.
func (conn *HvsockConn) RemoteAddr() net.Addr {
	return &conn.remote
}

// SetDeadline implements the net.Conn SetDeadline method.
func (conn *HvsockConn) SetDeadline(t time.Time) error {
	conn.SetReadDeadline(t)
	conn.SetWriteDeadline(t)
	return nil
}

// SetReadDeadline implements the net.Conn SetReadDeadline method.
func (conn *HvsockConn) SetReadDeadline(t time.Time) error {
	return conn.sock.SetReadDeadline(t)
}

// SetWriteDeadline implements the net.Conn SetWriteDeadline method.
func (conn *HvsockConn) SetWriteDeadline(t time.Time) error {
	return conn.sock.SetWriteDeadline(t)
}
