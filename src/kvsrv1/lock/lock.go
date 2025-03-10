package lock

import (
	"log"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

const LOCK_FREE = "FREE" // No client has lock

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	id string
	l  string // where to store lock state
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.id = kvtest.RandValue(8)
	lk.l = l
	lk.ck.Put(lk.l, LOCK_FREE, 0)
	return lk
}

func (lk *Lock) Acquire() {
	for {
		value, version, err := lk.ck.Get(lk.l)
		if err == rpc.OK && (value == LOCK_FREE || value == lk.id) {
			if value == lk.id {
				log.Printf("%v already held the lock", lk.id)
				return
			}
			log.Printf("%v try to acquire lock with version %v", lk.id, version)
			err := lk.ck.Put(lk.l, lk.id, version)
			if err == rpc.OK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	for {
		value, version, err := lk.ck.Get(lk.l)
		if err == rpc.OK && (value == LOCK_FREE || value == lk.id) {
			if value == LOCK_FREE {
				log.Printf("%v already released lock", lk.id)
				return
			}
			log.Printf("%v try to release lock with version %v", lk.id, version)
			err := lk.ck.Put(lk.l, LOCK_FREE, version)
			if err == rpc.OK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
