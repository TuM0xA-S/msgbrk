package main

import (
	"container/list"
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

// !!!DISCLAIMER!!!
// третий вариант с использованием контекста на ожидании (решает проблему когда ожидающий запрос отменяется)
// убирание буферизации с канала отправки на войтере чтобы когда ожидание закончилось не было варианта послать туда когда сработал таймер
// max queue size
// billion is ok and pretty practical
// unsafe.Sizeof([]byte{}) == 24
const queueSize = 1e6 // 24 megs for empty queue
const maxWaiters = 1e5

var (
	ErrorQueueOverflow   = errors.New("queue overflow")
	ErrorWaitersOverflow = errors.New("waiters overflow")
	ErrorNoMessage       = errors.New("no message")
)

type MessageBroker struct {
	ts map[string]Topic
	mu sync.Mutex
}

// only problem with using just channel is
// rule that if you first start waiting you first get value
// when multiple goroutines waiting on channel there no difference in them
// so we creating another queue for waiters
type Topic struct {
	queue   chan []byte
	waiters *list.List
}

func NewTopic() Topic {
	return Topic{queue: make(chan []byte, queueSize), waiters: list.New()}
}

type Waiter struct {
	ch  chan []byte
	ctx context.Context
}

func NewWaiter(ctx context.Context) Waiter {
	return Waiter{ch: make(chan []byte), ctx: ctx}
}

func NewMessageBroker() *MessageBroker {
	return &MessageBroker{ts: map[string]Topic{}}
}

func (mb *MessageBroker) Publish(topic string, msg []byte) error {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	// if no topic then create
	if _, ok := mb.ts[topic]; !ok {
		mb.ts[topic] = NewTopic()
	}
	t := mb.ts[topic]
	for t.waiters.Len() > 0 {
		w := t.waiters.Remove(t.waiters.Front()).(Waiter)
		// if waiter timeouted try next waiter
		// if we have timeout waiter have timeout too
		// if we sended message waiter get message because channel is unbuffered
		// sender and waiter literally waiting on same select
		// гонки данных нет твердо и четко (?)
		select {
		case w.ch <- msg:
			return nil
		case <-w.ctx.Done():
		}
	}
	// if no waiter push to queue
	// if queue overflowed then return false
	select {
	case t.queue <- msg:
		return nil
	default:
		return ErrorQueueOverflow
	}
}

// Consume with timeout, return msg and if consumed ok
// timeout <= 0 means do no wait for value
func (mb *MessageBroker) Consume(ctx context.Context, topic string, timeout time.Duration) ([]byte, error) {
	mb.mu.Lock()
	// if no topic then create
	if _, ok := mb.ts[topic]; !ok {
		mb.ts[topic] = NewTopic()
	}
	t := mb.ts[topic]

	select {
	case msg := <-t.queue:
		mb.mu.Unlock()
		return msg, nil
	default:
		if timeout <= 0 {
			mb.mu.Unlock()
			return nil, ErrorNoMessage
		}
	}
	// if have timeout do blocking select with timer
	if t.waiters.Len() >= maxWaiters {
		mb.mu.Unlock()
		return nil, ErrorWaitersOverflow
	}
	ctxTimeout, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	w := NewWaiter(ctxTimeout)
	t.waiters.PushBack(w)
	mb.mu.Unlock()

	select {
	case msg := <-w.ch:
		return msg, nil
	case <-w.ctx.Done():
		return nil, ErrorNoMessage
	}
}

// Service implements http.Handler with message broker rest api
type Service struct {
	mb *MessageBroker
}

func (s *Service) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()

	path := req.URL.Path
	if len(path) < 2 { // to handle /
		rw.WriteHeader(http.StatusBadRequest)
		return
	}
	if req.Method == http.MethodPut {
		s.publish(rw, req)
	} else if req.Method == http.MethodGet {
		s.consume(rw, req)
	} else {
		rw.WriteHeader(http.StatusBadRequest)
	}
}

func (s *Service) publish(rw http.ResponseWriter, req *http.Request) {
	queries := req.URL.Query()
	msg := queries.Get("v")
	if msg == "" {
		rw.WriteHeader(http.StatusBadRequest)
		return
	}
	topic := req.URL.Path
	if err := s.mb.Publish(topic, []byte(msg)); err != nil {
		rw.WriteHeader(http.StatusInternalServerError)
	}
}

func (s *Service) consume(rw http.ResponseWriter, req *http.Request) {
	timeout := -1
	timeoutRaw := req.URL.Query().Get("timeout")
	if timeoutRaw != "" {
		var err error
		timeout, err = strconv.Atoi(timeoutRaw)
		if err != nil {
			rw.WriteHeader(http.StatusBadRequest)
			return
		}
	}
	topic := req.URL.Path
	msg, err := s.mb.Consume(req.Context(), topic, time.Duration(timeout)*time.Second)
	if err == ErrorNoMessage {
		rw.WriteHeader(http.StatusNotFound)
		return
	}
	if err != nil {
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}
	rw.Write(msg)
}
func NewService() *Service {
	return &Service{NewMessageBroker()}
}

func main() {
	host := ":8080"
	if len(os.Args) > 1 {
		host = os.Args[1]
	}
	// gogogogo
	if err := http.ListenAndServe(host, NewService()); err != nil {
		log.Fatal(err)
	}
}
