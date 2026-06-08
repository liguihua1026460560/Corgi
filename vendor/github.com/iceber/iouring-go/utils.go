//go:build linux
// +build linux

package iouring

import (
	"syscall"
	"unsafe"
)

var zero uintptr

func bytes2iovec(bs [][]byte) []syscall.Iovec {
	iovecs := make([]syscall.Iovec, len(bs))
	for i, b := range bs {
		iovecs[i].SetLen(len(b))
		if len(b) > 0 {
			iovecs[i].Base = &b[0]
		} else {
			iovecs[i].Base = (*byte)(unsafe.Pointer(&zero))
		}
	}
	return iovecs
}

// sockaddr converts a syscall.Sockaddr to a raw pointer and length.
// This replaces the //go:linkname approach (incompatible with Go 1.21+).
func sockaddr(sa syscall.Sockaddr) (unsafe.Pointer, uint32, error) {
	switch v := sa.(type) {
	case *syscall.SockaddrInet4:
		rsa := &syscall.RawSockaddrInet4{Family: syscall.AF_INET}
		p := (*[2]byte)(unsafe.Pointer(&rsa.Port))
		p[0] = byte(v.Port >> 8)
		p[1] = byte(v.Port)
		rsa.Addr = v.Addr
		return unsafe.Pointer(rsa), syscall.SizeofSockaddrInet4, nil
	case *syscall.SockaddrInet6:
		rsa := &syscall.RawSockaddrInet6{Family: syscall.AF_INET6}
		p := (*[2]byte)(unsafe.Pointer(&rsa.Port))
		p[0] = byte(v.Port >> 8)
		p[1] = byte(v.Port)
		rsa.Scope_id = v.ZoneId
		rsa.Addr = v.Addr
		return unsafe.Pointer(rsa), syscall.SizeofSockaddrInet6, nil
	case *syscall.SockaddrUnix:
		name := v.Name
		n := len(name)
		rsa := &syscall.RawSockaddrUnix{Family: syscall.AF_UNIX}
		if n > len(rsa.Path) {
			return nil, 0, syscall.EINVAL
		}
		for i := 0; i < n; i++ {
			rsa.Path[i] = int8(name[i])
		}
		sl := uint32(2)
		if n > 0 {
			sl += uint32(n) + 1
		}
		return unsafe.Pointer(rsa), sl, nil
	default:
		return nil, 0, syscall.EAFNOSUPPORT
	}
}

// anyToSockaddr converts a RawSockaddrAny to a Sockaddr.
func anyToSockaddr(rsa *syscall.RawSockaddrAny) (syscall.Sockaddr, error) {
	switch rsa.Addr.Family {
	case syscall.AF_INET:
		pp := (*syscall.RawSockaddrInet4)(unsafe.Pointer(rsa))
		sa := &syscall.SockaddrInet4{}
		p := (*[2]byte)(unsafe.Pointer(&pp.Port))
		sa.Port = int(p[0])<<8 + int(p[1])
		sa.Addr = pp.Addr
		return sa, nil
	case syscall.AF_INET6:
		pp := (*syscall.RawSockaddrInet6)(unsafe.Pointer(rsa))
		sa := &syscall.SockaddrInet6{}
		p := (*[2]byte)(unsafe.Pointer(&pp.Port))
		sa.Port = int(p[0])<<8 + int(p[1])
		sa.ZoneId = pp.Scope_id
		sa.Addr = pp.Addr
		return sa, nil
	case syscall.AF_UNIX:
		pp := (*syscall.RawSockaddrUnix)(unsafe.Pointer(rsa))
		sa := &syscall.SockaddrUnix{}
		if pp.Path[0] == 0 {
			return sa, nil
		}
		i := 0
		for i < len(pp.Path) && pp.Path[i] != 0 {
			i++
		}
		bytes := (*[len(pp.Path)]byte)(unsafe.Pointer(&pp.Path[0]))
		sa.Name = string(bytes[:i])
		return sa, nil
	default:
		return nil, syscall.EAFNOSUPPORT
	}
}
