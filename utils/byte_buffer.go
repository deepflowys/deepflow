package utils

import (
	"sync"
	"sync/atomic"
)

type ByteBuffer struct {
	buf    []byte
	offset int
	quota  int

	ExtraRefCount int32 // for PseudoClone
}

// 返回下一个可用的[]byte，自动增长空间
func (b *ByteBuffer) Use(n int) []byte {
	b.offset += n
	if b.offset > b.quota {
		panic("Quota limit exceeded!") // 对忘记调用Reset的保护
	}
	if b.offset > len(b.buf) {
		b.buf = append(b.buf, make([]byte, (b.offset-len(b.buf))*2)...)
	}
	return b.buf[b.offset-n : b.offset]
}

// 返回所有Use调用过的[]byte
func (b *ByteBuffer) Bytes() []byte {
	return b.buf[:b.offset]
}

func (b *ByteBuffer) Reset() {
	b.offset = 0
	b.ExtraRefCount = 0
}

func (b *ByteBuffer) SetQuota(n int) {
	b.quota = n
}

var pool = sync.Pool{
	New: func() interface{} {
		return &ByteBuffer{quota: 1 << 16}
	},
}

func AcquireByteBuffer() *ByteBuffer {
	return pool.Get().(*ByteBuffer)
}

func CloneByteBuffer(bytes *ByteBuffer) *ByteBuffer {
	clone := AcquireByteBuffer()
	clone.Use(len(bytes.Bytes()))
	copy(clone.Bytes(), bytes.Bytes())
	return clone
}

func PseudoCloneByteBuffer(bytes *ByteBuffer) {
	atomic.AddInt32(&bytes.ExtraRefCount, 1)
}

func ReleaseByteBuffer(bytes *ByteBuffer) {
	if atomic.AddInt32(&bytes.ExtraRefCount, -1) >= 0 {
		return
	}

	bytes.Reset()
	pool.Put(bytes)
}
