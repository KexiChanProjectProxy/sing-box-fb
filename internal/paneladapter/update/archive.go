package update

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"io"
	"path"
	"strings"

	E "github.com/sagernet/sing/common/exceptions"
)

func unpackExecutable(payload []byte, preferName string) ([]byte, error) {
	if isGzip(payload) {
		gr, err := gzip.NewReader(bytes.NewReader(payload))
		if err != nil {
			return nil, E.Cause(err, "gunzip binary")
		}
		defer gr.Close()
		payload, err = io.ReadAll(io.LimitReader(gr, maxBinaryBytes+1))
		if err != nil {
			return nil, E.Cause(err, "gunzip binary")
		}
		if len(payload) > maxBinaryBytes {
			return nil, E.New("decompressed binary exceeds ", maxBinaryBytes, " bytes")
		}
	}
	if isTar(payload) {
		return extractFromTar(payload, preferName)
	}
	if len(payload) == 0 {
		return nil, E.New("empty binary payload")
	}
	return payload, nil
}

func isGzip(payload []byte) bool {
	return len(payload) >= 2 && payload[0] == 0x1f && payload[1] == 0x8b
}

func isTar(payload []byte) bool {
	return len(payload) >= 262 && string(payload[257:262]) == "ustar"
}

func extractFromTar(payload []byte, preferName string) ([]byte, error) {
	tr := tar.NewReader(bytes.NewReader(payload))
	preferName = path.Base(preferName)
	var fallback []byte
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, E.Cause(err, "read tar")
		}
		if hdr.Typeflag != tar.TypeReg {
			continue
		}
		name := path.Base(hdr.Name)
		if name == "" || name == "." || strings.HasPrefix(name, ".") {
			continue
		}
		data, err := io.ReadAll(io.LimitReader(tr, maxBinaryBytes+1))
		if err != nil {
			return nil, E.Cause(err, "read tar member ", hdr.Name)
		}
		if len(data) == 0 || len(data) > maxBinaryBytes {
			continue
		}
		if preferName != "" && name == preferName {
			return data, nil
		}
		if fallback == nil {
			fallback = data
		}
	}
	if fallback != nil {
		return fallback, nil
	}
	return nil, E.New("tar archive contains no executable")
}
