package main

import (
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestMessageBroker(t *testing.T) {
	mb := NewMessageBroker()
	// basic case
	mb.Publish("test", []byte("lol"))
	msg, err := mb.Consume("test", 0)
	if err != nil || string(msg) != "lol" {
		t.Fatal("fail to pop basic value")
	}
	// basic case timeout
	mb.Publish("test", []byte("lol"))
	msg, err = mb.Consume("test", time.Second)
	if err != nil || string(msg) != "lol" {
		t.Fatal("fail to pop basic value")
	}
	// fail without timeout
	_, err = mb.Consume("test", 0)
	if err == nil {
		t.Fatal("wtf ok from empty queue")
	}
	// blocking pull value with timeout after ghost promise
	go func() {
		time.Sleep(time.Millisecond * 100)
		mb.Publish("test", []byte("wtf"))
	}()
	msg, err = mb.Consume("test", time.Millisecond*200)
	if err != nil || string(msg) != "wtf" {
		t.Fatal("fail to get value with block and timeout")
	}
	mb.Consume("test", 0)
	mb.Consume("test", 0)
	mb.Consume("test", 0)
	mb.Publish("test", []byte("1"))
	// time.Sleep(time.Millisecond * 100)
	msg, err = mb.Consume("test", 0)
	if err != nil || string(msg) != "1" {
		t.Fatal("fail to get value after ghost promises")
	}
	mb.Publish("test", []byte("2"))
	// time.Sleep(time.Millisecond * 100)
	msg, err = mb.Consume("test", 0)
	if err != nil || string(msg) != "2" {
		t.Fatal("fail to get value after ghost promises")
	}
	// check wait order
	var wg sync.WaitGroup
	var values [101]string
	for i := 1; i <= 100; i++ {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			msg, err := mb.Consume("test", 10*time.Second)
			if err == nil {
				values[i] = string(msg)
			}
		}()
		time.Sleep(10 * time.Millisecond)
	}
	for i := 1; i <= 100; i++ {
		mb.Publish("test", []byte(strconv.Itoa(i)))
	}
	wg.Wait()
	for i := 1; i <= 100; i++ {
		if values[i] != strconv.Itoa(i) {
			t.Fatal("check wait order failed")
		}
	}

	// check overdue waiters
	_, err = mb.Consume("test", time.Millisecond)
	if err == nil {
		t.Fatal("wrong consume")
	}
	_, err = mb.Consume("test", time.Millisecond)
	if err == nil {
		t.Fatal("wrong consume")
	}

	time.Sleep(time.Millisecond * 100)
	mb.Publish("test", []byte("wow"))
	msg, err = mb.Consume("test", time.Millisecond)
	if err != nil || string(msg) != "wow" {
		t.Fatal("wrong consume after overdue waiters")
	}

}
