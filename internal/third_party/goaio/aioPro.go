package goaio

import (
	"syscall"
	"unsafe"
)

type notifyResult struct {
	ch  chan<- Completion
	res Completion
}

func (a *AIO) finishEventLocked(ae *activeEvent, cb *aiocb, err error) notifyResult {
	ch := ae.doneCh

	ae.data = nil
	delete(a.active, cb)

	cb.buffer = nil // 放回avail前，先清空buffer，使gc回收
	a.avail[cb] = true
	a.releaseSlot()

	return notifyResult{
		ch: ch,
		res: Completion{
			N:   int(ae.written),
			Err: err,
		},
	}
}

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

func (a *AIO) WriteAtNotify(b []byte, offset int64, doneCh chan<- Completion) error {
	if len(b) <= 0 {
		return ErrInvalidBuffer
	}

	cbp, err := a.getNextReady()
	if err != nil {
		return err
	}

	cbp.offset = offset
	cbp.buffer = unsafe.Pointer(&b[0])
	cbp.nbytes = uint64(len(b))
	cbp.opcode = iocb_cmd_pwrite

	a.dmtx.Lock()
	defer a.dmtx.Unlock()
	if err := a.submit(cbp); err != nil {
		a.avail[cbp] = true
		a.releaseSlot()
		return err
	}

	a.active[cbp] = &activeEvent{
		data:   b, // 保证 buffer 不被 GC
		cb:     cbp,
		doneCh: doneCh,
	}

	if newEnd := offset + int64(len(b)); a.end < newEnd {
		a.end = newEnd
	}

	return nil
}

func (a *AIO) verifyResultPro(evnt event) (notifyResult, error) {
	a.dmtx.Lock()
	defer a.dmtx.Unlock()

	if evnt.cb == nil { //出现的话，不需要处理doneCh，上游应该要中断进程或者者记录日志，至少不应该继续往下走了
		return notifyResult{}, ErrNilCallback
	}

	ae, ok := a.active[evnt.cb]
	if !ok {
		return notifyResult{}, ErrUntrackedEventKey
	}

	if ae.cb != evnt.cb { //出现的话，不需要处理doneCh，上游应该要中断进程或者者记录日志，至少不应该继续往下走了
		return notifyResult{}, ErrInvalidEventPtr
	}

	if evnt.res < 0 {
		return a.finishEventLocked(ae, evnt.cb, lookupErrNo(evnt.res)), nil
	}

	ae.written += uint(evnt.res)

	if ae.written != uint(len(ae.data)) {
		return a.finishEventLocked(ae, evnt.cb, ErrShortWrite), nil
	}

	return a.finishEventLocked(ae, evnt.cb, nil), nil
}

func (a *AIO) WaitAnyNotify() (int, error) {
	a.wmtx.Lock()
	defer a.wmtx.Unlock()

	shortTimeout := timespec{
		sec:  1, // 1s
		nsec: 0, //100ms=100*1000*1000,
	}
	toPtr := uintptr(unsafe.Pointer(&shortTimeout))

	for {
		x, _, errno := syscall.Syscall6(
			syscall.SYS_IO_GETEVENTS,
			uintptr(a.ctx),
			uintptr(1),
			uintptr(len(a.evt)),
			uintptr(unsafe.Pointer(&a.evt[0])),
			toPtr, //等待1s
			uintptr(0),
		)

		if errno == syscall.EINTR {
			continue
		}
		if errno != 0 {
			return 0, errLookup(errno)
		}
		if x == uintptr(0) { //没有完成的请求，继续等待
			return 0, nil
		}

		notifies := make([]notifyResult, 0, int(x))

		for i := uintptr(0); i < x; i++ {
			nr, err := a.verifyResultPro(a.evt[i])
			if err != nil {
				return len(notifies), err
			}
			if nr.ch != nil {
				notifies = append(notifies, nr)
			}
		}

		// 重点：锁外发送，避免死锁
		for _, nr := range notifies {
			nr.ch <- nr.res
		}

		return len(notifies), nil
	}
}
