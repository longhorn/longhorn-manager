package util

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	DownloadBufferSize = 1 << 12
	HTTPTimeout        = 10 * time.Second
)

type ProgressUpdater interface {
	UpdateProgress(size int64)
}

type Handler interface {
	GetSizeFromURL(url string) (fileSize int64, err error)
	DownloadFromURL(ctx context.Context, url, outFh *os.File, updater ProgressUpdater) (written int64, err error)
}

type HTTPHandler struct{}

func (h *HTTPHandler) GetSizeFromURL(url string) (size int64, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), HTTPTimeout)
	defer cancel()

	rr, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
	if err != nil {
		return 0, err
	}

	client := NewDownloadHttpClient()
	resp, err := client.Do(rr)
	if err != nil {
		return 0, err
	}
	defer func() {
		if errClose := resp.Body.Close(); errClose != nil {
			logrus.WithError(errClose).Errorf("Failed to close response body for %s", url)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("expected status code 200 from %s, got %s", url, resp.Status)
	}

	contentLength := resp.Header.Get("Content-Length")
	if contentLength == "" {
		// -1 indicates unknown size
		size = -1
	} else {
		size, err = strconv.ParseInt(contentLength, 10, 64)
		if err != nil {
			return 0, err
		}
	}

	return size, nil
}

func (h *HTTPHandler) DownloadFromURL(ctx context.Context, url string, outFh *os.File, updater ProgressUpdater) (written int64, err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	rr, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, err
	}

	client := NewDownloadHttpClient()
	resp, err := client.Do(rr)
	if err != nil {
		return 0, err
	}
	defer func() {
		if errClose := resp.Body.Close(); errClose != nil {
			logrus.WithError(errClose).Errorf("Failed to close response body for %s", url)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("expected status code 200 from %s, got %s", url, resp.Status)
	}

	copied, err := IdleTimeoutCopy(ctx, cancel, resp.Body, outFh, updater, false)
	if err != nil {
		return 0, err
	}

	return copied, nil
}

// IdleTimeoutCopy relies on ctx of the reader/src or a separate timer to interrupt the processing.
func IdleTimeoutCopy(ctx context.Context, cancel context.CancelFunc, src io.ReadCloser, dst io.WriteSeeker, updater ProgressUpdater, writeZero bool) (copied int64, err error) {
	writeSeekCh := make(chan int64, 100)
	defer close(writeSeekCh)

	go func() {
		t := time.NewTimer(HTTPTimeout)
		done := false
		for !done {
			select {
			case <-ctx.Done():
				done = true
			case <-t.C:
				cancel()
				done = true
			case _, writeChOpen := <-writeSeekCh:
				if !writeChOpen {
					done = true
					break
				}
				if !t.Stop() {
					<-t.C
				}
				t.Reset(HTTPTimeout)
			}
		}

		// Still need to make sure to clean up the signals in writeSeekCh
		// so that they won't block the below sender.
		for writeChOpen := true; writeChOpen; {
			_, writeChOpen = <-writeSeekCh
		}
	}()

	var nr, nw int
	var nws int64
	var rErr, handleErr error
	buf := make([]byte, DownloadBufferSize)
	zeroByteArray := make([]byte, DownloadBufferSize)
	for rErr == nil && err == nil {
		select {
		case <-ctx.Done():
			err = fmt.Errorf("context cancelled during the copy")
		default:
			// Read will error out once the context is cancelled.
			nr, rErr = src.Read(buf)
			if nr > 0 {
				// Skip writing zero data
				if !writeZero && bytes.Equal(buf[0:nr], zeroByteArray[0:nr]) {
					_, handleErr = dst.Seek(int64(nr), io.SeekCurrent)
					nws = int64(nr)
				} else {
					nw, handleErr = dst.Write(buf[0:nr])
					nws = int64(nw)
				}
				if handleErr != nil {
					err = handleErr
					break
				}
				writeSeekCh <- nws
				copied += nws
				updater.UpdateProgress(nws)
			}
			if rErr != nil {
				if rErr != io.EOF {
					err = rErr
				}
				break // nolint: staticcheck
			}
		}
	}

	return copied, err
}

func removeReferer(req *http.Request) {
	for k := range req.Header {
		if strings.ToLower(k) == "referer" {
			delete(req.Header, k)
		}
	}
}

func NewDownloadHttpClient() http.Client {
	return http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			// Remove the "Referer" header to enable downloads of files
			// that are delivered via CDN and therefore may be redirected
			// several times. This is the same behaviour of curl or wget
			// in their default configuration.
			removeReferer(req)
			return nil
		},
	}
}
