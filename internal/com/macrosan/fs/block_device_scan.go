package fs

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unsafe"

	"golang.org/x/sys/unix"
)

const blockDeviceMountPrefix = "fs-SP0-"

var deviceMap sync.Map

type BlockDeviceCandidate struct {
	Lun           string
	MountDir      string
	MetaPartition string
	DataPartition string
	Disk          string
	Size          int64
}

func Init(ctx context.Context, testMountdir string) error {
	candidates, err := ScanBlockDeviceCandidates()
	if err != nil {
		return err
	}

	var errs []error
	for _, c := range candidates {
		if testMountdir != "" && c.MountDir != testMountdir {
			continue
		}
		if _, ok := deviceMap.Load(c.Lun); ok {
			continue
		}
		device, openErr := OpenPartitionBlockDevice(ctx, c.Lun, c.MountDir, c.DataPartition)
		if openErr != nil {
			errs = append(errs, fmt.Errorf("open block device %s (%s): %w", c.Lun, c.DataPartition, openErr))
			continue
		}
		actual, loaded := deviceMap.LoadOrStore(c.Lun, device)
		if loaded {
			_ = device.Close()
			device = actual.(*BlockDevice)
		}
		_ = device
	}
	return errors.Join(errs...)
}

func Devices() map[string]*BlockDevice {
	out := make(map[string]*BlockDevice)
	deviceMap.Range(func(key, value any) bool {
		out[key.(string)] = value.(*BlockDevice)
		return true
	})
	return out
}

func Get(lun string) *BlockDevice {
	if v, ok := deviceMap.Load(lun); ok {
		return v.(*BlockDevice)
	}
	return nil
}

func Remove(lun string) error {
	v, ok := deviceMap.LoadAndDelete(lun)
	if !ok {
		return nil
	}
	return v.(*BlockDevice).Close()
}

func ScanBlockDeviceCandidates() ([]BlockDeviceCandidate, error) {
	mounts, err := readMountInfo()
	if err != nil {
		return nil, err
	}

	seen := make(map[string]struct{})
	candidates := make([]BlockDeviceCandidate, 0)
	for _, m := range mounts {
		if !isFSBlockMount(m.mountPoint) {
			continue
		}

		part, err := blockPartition(m.source)
		if err != nil || part.number != 1 {
			continue
		}

		parts, err := diskPartitions(part.disk)
		if err != nil || len(parts) != 2 {
			continue
		}

		var p1, p2 partition
		for _, p := range parts {
			if p.number == 1 {
				p1 = p
			}
			if p.number == 2 {
				p2 = p
			}
		}
		if p1.name == "" || p2.name == "" || p1.name != part.name {
			continue
		}

		lun := filepath.Base(m.mountPoint)
		if _, ok := seen[lun]; ok {
			continue
		}

		dataPath := filepath.Join("/dev", p2.name)
		size, err := blockDeviceSize(dataPath)
		if err != nil {
			return nil, err
		}

		candidates = append(candidates, BlockDeviceCandidate{
			Lun:           lun,
			MountDir:      m.mountPoint,
			MetaPartition: filepath.Join("/dev", p1.name),
			DataPartition: dataPath,
			Disk:          part.disk,
			Size:          size / BlockSize * BlockSize,
		})
		seen[lun] = struct{}{}
	}
	return candidates, nil
}

type mountInfo struct {
	mountPoint string
	source     string
}

func readMountInfo() ([]mountInfo, error) {
	f, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var mounts []mountInfo
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, " - ", 2)
		if len(parts) != 2 {
			continue
		}
		left := strings.Fields(parts[0])
		right := strings.Fields(parts[1])
		if len(left) < 5 || len(right) < 2 {
			continue
		}
		mounts = append(mounts, mountInfo{
			mountPoint: unescapeMountField(left[4]),
			source:     unescapeMountField(right[1]),
		})
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return mounts, nil
}

func isFSBlockMount(mountPoint string) bool {
	clean := strings.TrimPrefix(filepath.Clean(mountPoint), string(os.PathSeparator))
	return strings.HasPrefix(clean, blockDeviceMountPrefix) ||
		strings.HasPrefix(filepath.Base(mountPoint), blockDeviceMountPrefix)
}

type partition struct {
	name   string
	disk   string
	number int
}

func blockPartition(source string) (partition, error) {
	if !strings.HasPrefix(source, "/dev/") {
		return partition{}, fmt.Errorf("not a /dev block source: %s", source)
	}

	realPath, err := filepath.EvalSymlinks(source)
	if err != nil {
		realPath = source
	}
	//e.g. "sda1" for "/dev/sda1".
	name := filepath.Base(realPath)

	numberBytes, err := os.ReadFile(filepath.Join("/sys/class/block", name, "partition"))
	if err != nil {
		return partition{}, err
	}
	// sda1 -> 1, sda2 -> 2
	number, err := strconv.Atoi(strings.TrimSpace(string(numberBytes)))
	if err != nil {
		return partition{}, err
	}

	link, err := os.Readlink(filepath.Join("/sys/class/block", name))
	if err != nil {
		return partition{}, err
	}
	//partition{name: "sda1", disk: "sda", number: 1}
	return partition{name: name, disk: filepath.Base(filepath.Dir(link)), number: number}, nil
}

func diskPartitions(disk string) ([]partition, error) {
	entries, err := os.ReadDir("/sys/class/block")
	if err != nil {
		return nil, err
	}

	parts := make([]partition, 0, 2)
	for _, entry := range entries {
		name := entry.Name()
		numberBytes, err := os.ReadFile(filepath.Join("/sys/class/block", name, "partition"))
		if err != nil {
			continue
		}
		link, err := os.Readlink(filepath.Join("/sys/class/block", name))
		if err != nil || filepath.Base(filepath.Dir(link)) != disk {
			continue
		}
		number, err := strconv.Atoi(strings.TrimSpace(string(numberBytes)))
		if err != nil {
			continue
		}
		parts = append(parts, partition{name: name, disk: disk, number: number})
	}

	sort.Slice(parts, func(i, j int) bool {
		return parts[i].number < parts[j].number
	})
	return parts, nil
}

// 返回块设备的总容量大小，单位是字节
func blockDeviceSize(path string) (int64, error) {
	realPath, err := filepath.EvalSymlinks(path)
	if err != nil {
		realPath = path
	}
	name := filepath.Base(realPath)
	if sectorsBytes, err := os.ReadFile(filepath.Join("/sys/class/block", name, "size")); err == nil {
		sectors, parseErr := strconv.ParseInt(strings.TrimSpace(string(sectorsBytes)), 10, 64)
		if parseErr != nil {
			return 0, parseErr
		}
		return sectors * 512, nil
	}

	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	if info.Mode().IsRegular() { //这种不是块设备，是普通文件，直接用文件大小
		return info.Size(), nil
	}

	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	var size uint64
	//向内核询问这个块设备的真实容量，返回单位是字节
	_, _, errno := unix.Syscall(unix.SYS_IOCTL, f.Fd(), unix.BLKGETSIZE64, uintptr(unsafe.Pointer(&size)))
	if errno != 0 {
		return 0, errno
	}
	return int64(size), nil
}

func unescapeMountField(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+3 < len(s) {
			if v, err := strconv.ParseUint(s[i+1:i+4], 8, 8); err == nil {
				b.WriteByte(byte(v))
				i += 3
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}
