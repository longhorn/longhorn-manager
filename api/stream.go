package api

import (
	"math/rand"
	"net/http"
	"reflect"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"

	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"

	"github.com/longhorn/longhorn-manager/controller"
)

const (
	keepAlivePeriod = 15 * time.Second

	writeWait = 10 * time.Second
)

var (
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}
	randFunc *rand.Rand
)

func init() {
	randFunc = rand.New(rand.NewSource(time.Now().UnixNano()))
}

func NewStreamHandlerFunc(streamType string, watcher *controller.Watcher, listFunc func(ctx *api.ApiContext) (*client.GenericCollection, error)) func(w http.ResponseWriter, r *http.Request) error {
	return func(w http.ResponseWriter, r *http.Request) error {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return err
		}
		fields := logrus.Fields{
			"id":   strconv.Itoa(randFunc.Int()),
			"type": streamType,
		}
		logrus.WithFields(fields).Debug("websocket: open")

		done := make(chan struct{})
		go func() {
			defer close(done)
			for {
				_, _, err := conn.ReadMessage()
				if err != nil {
					logrus.WithFields(fields).Warn(err.Error())
					return
				}
			}
		}()

		apiContext := api.GetApiContext(r)

		resp, err := writeList(conn, nil, listFunc, apiContext)
		if err != nil {
			return err
		}

		rateLimitTicker := maybeNewTicker(getPeriod(r))
		if rateLimitTicker != nil {
			defer rateLimitTicker.Stop()
		}
		keepAliveTicker := time.NewTicker(keepAlivePeriod)
		defer keepAliveTicker.Stop()
		for {
			if rateLimitTicker != nil {
				<-rateLimitTicker.C
			}
			select {
			case <-done:
				return nil
			case <-watcher.Events():
				resp, err = writeList(conn, resp, listFunc, apiContext)
			case <-keepAliveTicker.C:
				err = conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(writeWait))
				// WebsocketController doesn't include eventInformer so it only
				// gets triggered here.
				if streamType == "events" {
					resp, err = writeList(conn, resp, listFunc, apiContext)
				}
			}
			if err != nil {
				return err
			}
		}
	}
}

func writeList(conn *websocket.Conn, oldResp *client.GenericCollection, listFunc func(ctx *api.ApiContext) (*client.GenericCollection, error), apiContext *api.ApiContext) (*client.GenericCollection, error) {
	newResp, err := listFunc(apiContext)
	if err != nil {
		return oldResp, err
	}

	if oldResp != nil && reflect.DeepEqual(oldResp, newResp) {
		return oldResp, nil
	}
	data, err := apiContext.PopulateCollection(newResp)
	if err != nil {
		return oldResp, err
	}

	err = conn.SetWriteDeadline(time.Now().Add(writeWait))
	if err != nil {
		return oldResp, err
	}
	err = conn.WriteJSON(data)
	if err != nil {
		return oldResp, err
	}

	return newResp, nil
}

func maybeNewTicker(d time.Duration) *time.Ticker {
	var ticker *time.Ticker
	if d > 0*time.Second {
		ticker = time.NewTicker(d)
	}
	return ticker
}

func getPeriod(r *http.Request) time.Duration {
	period := 0 * time.Second
	periodString := mux.Vars(r)["period"]
	if periodString != "" {
		period, _ = time.ParseDuration(periodString)
	}
	switch {
	case period < 0*time.Second:
		period = 0 * time.Second
	case period > 15*time.Second:
		period = 15 * time.Second
	}
	return period
}
