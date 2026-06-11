package goaio

import (
	"syscall"
	"unsafe"
)

// 阻塞等待任意一个请求完成，返回完成的请求数量和可能的错误。如果没有未完成的请求，则返回0和nil。
func (a *AIO) WaitAnyBlock(completed []RequestId) (int, error) {
	waitBlock := func(to timespec, completed []RequestId) (int, error) {
		var compLen int

		toPtr := uintptr(unsafe.Pointer(&to))
		if to == zeroTime {
			toPtr = 0
		}

		for {
			x, _, errno := syscall.Syscall6(
				syscall.SYS_IO_GETEVENTS,
				uintptr(a.ctx),
				uintptr(1),
				uintptr(len(a.evt)),
				uintptr(unsafe.Pointer(&a.evt[0])),
				toPtr,
				uintptr(0),
			)

			if errno == syscall.EINTR {
				continue
			}

			if errno != 0 {
				return 0, errLookup(errno)
			}

			if x == uintptr(0) {
				return 0, nil
			}

			if x > uintptr(len(a.evt)) {
				return 0, ErrWaitAllFailed
			}

			var err error
			for i := uintptr(0); i < x; i++ {
				if e := a.verifyResult(a.evt[i], &compLen, completed); e != nil {
					if err == nil {
						err = e
					}
				}
			}

			return compLen, err
		}
	}
	a.wmtx.Lock()
	defer a.wmtx.Unlock()

	return waitBlock(zeroTime, completed)
}
