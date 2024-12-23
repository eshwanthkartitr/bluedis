package main

import (
	"fmt"
	"sync"
	"time"
)


type Node struct {
	value interface{}
	prev  *Node
	next  *Node
}


type DoublyLinkedList struct {
	head   *Node
	tail   *Node
	length int
	mu     sync.Mutex
	cond   *sync.Cond
}

func NewDoublyLinkedList() *DoublyLinkedList {
	dll := &DoublyLinkedList{}
	dll.cond = sync.NewCond(&dll.mu)
	return dll
}

func (dll *DoublyLinkedList) PushLeft(value interface{}) {
	dll.mu.Lock()
	defer dll.mu.Unlock()

	newNode := &Node{value: value}
	if dll.head == nil {
		dll.head = newNode
		dll.tail = newNode
	} else {
		newNode.next = dll.head
		dll.head.prev = newNode
		dll.head = newNode
	}
	dll.length++
	dll.cond.Signal() // Notify any waiting consumers
}

func (dll *DoublyLinkedList) PushRight(value interface{}) {
	dll.mu.Lock()
	defer dll.mu.Unlock()

	newNode := &Node{value: value}
	if dll.tail == nil {
		dll.head = newNode
		dll.tail = newNode
	} else {
		newNode.prev = dll.tail
		dll.tail.next = newNode
		dll.tail = newNode
	}
	dll.length++
	dll.cond.Signal() // Notify any waiting consumers
}

func (dll *DoublyLinkedList) PopLeft() (interface{}, bool) {
	dll.mu.Lock()
	defer dll.mu.Unlock()

	if dll.head == nil {
		return nil, false 
	}

	value := dll.head.value
	dll.head = dll.head.next
	if dll.head != nil {
		dll.head.prev = nil
	} else {
		dll.tail = nil
	}
	dll.length--
	return value, true
}

func (dll *DoublyLinkedList) PopRight() (interface{}, bool) {
	dll.mu.Lock()
	defer dll.mu.Unlock()

	if dll.tail == nil {
		return nil, false 
	}

	value := dll.tail.value
	dll.tail = dll.tail.prev
	if dll.tail != nil {
		dll.tail.next = nil
	} else {
		dll.head = nil 
	}
	dll.length--
	return value, true
}

func (dll *DoublyLinkedList) Length() int {
	dll.mu.Lock()
	defer dll.mu.Unlock()
	return dll.length
}

// BlockingPopLeft removes and returns the value from the head of the list, blocking if empty
func (dll *DoublyLinkedList) BlockingPopLeft() interface{} {
	dll.mu.Lock()
	defer dll.mu.Unlock()

	for dll.head == nil {
		dll.cond.Wait() // Wait until an item is available
	}

	value := dll.head.value
	dll.head = dll.head.next
	if dll.head != nil {
		dll.head.prev = nil
	} else {
		dll.tail = nil 
	}
	dll.length--
	return value
}


// ExtractRange returns a slice of values from the list within the specified range
func (dll *DoublyLinkedList) ExtractRange(start, end int) []interface{} {
    dll.mu.Lock()
    defer dll.mu.Unlock()

    // Check if start is out of bounds
    if start < 0 || start >= dll.length {
        return nil
    }

    // If end is -1, set it to the last index ,also adjusts the end if larger than the length of list
    if end == -1 || end >= dll.length {
        end = dll.length - 1
    }

    // Check if end is less than start after adjustments
    if end < start {
        return nil 
    }

    var result []interface{}
    current := dll.head
    for i := 0; i < start; i++ {
        current = current.next
    }

    for i := start; i <= end && current != nil; i++ {
        result = append(result, current.value)
        current = current.next
    }
    return result
}
