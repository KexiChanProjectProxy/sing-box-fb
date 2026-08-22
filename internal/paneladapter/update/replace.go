package update

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	E "github.com/sagernet/sing/common/exceptions"
)

func writeFileAtomic(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	tmpPath := filepath.Join(dir, fmt.Sprintf(".bin.tmp.%d.%d", os.Getpid(), time.Now().UnixNano()))
	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, perm)
	if err != nil {
		return E.Cause(err, "create temp binary")
	}
	cleanup := func() {
		f.Close()
		os.Remove(tmpPath)
	}
	if _, err := f.Write(data); err != nil {
		cleanup()
		return E.Cause(err, "write temp binary")
	}
	if err := f.Sync(); err != nil {
		cleanup()
		return E.Cause(err, "fsync temp binary")
	}
	if err := f.Close(); err != nil {
		os.Remove(tmpPath)
		return E.Cause(err, "close temp binary")
	}
	if err := os.Chmod(tmpPath, perm); err != nil {
		os.Remove(tmpPath)
		return E.Cause(err, "chmod temp binary")
	}
	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return E.Cause(err, "rename temp binary")
	}
	return nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	info, err := in.Stat()
	if err != nil {
		return err
	}
	perm := info.Mode().Perm()
	if perm == 0 {
		perm = 0o755
	}
	tmp := dst + ".copytmp"
	out, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		os.Remove(tmp)
		return err
	}
	if err := out.Sync(); err != nil {
		out.Close()
		os.Remove(tmp)
		return err
	}
	if err := out.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, dst); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Chmod(dst, perm)
}
