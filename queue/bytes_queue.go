package queue

import (
	"encoding/binary"
	"log"
	"time"
)

const (
	// Number of bytes to encode 0 in uvarint format
	minimumHeaderSize = 17 // 1 byte blobsize + timestampSizeInBytes + hashSizeInBytes
	// Bytes before left margin are not used. Zero index means element does not exist in queue, useful while reading slice from index
	leftMarginIndex = 1
)

var (
	errEmptyQueue       = &queueError{"Empty queue"}
	errInvalidIndex     = &queueError{"Index must be greater than zero. Invalid index."}
	errIndexOutOfBounds = &queueError{"Index out of range"}
)

// BytesQueue is a non-thread safe queue type of fifo based on bytes array.
// For every push operation index of entry is returned. It can be used to read the entry later
// BytesQueue 是一种基于字节数组的非线程安全队列类型的 fifo。
// 每次推送操作都会返回 entry 的索引。可用于稍后阅读条目
type BytesQueue struct {
	full         bool   // 是否已满
	array        []byte // 实际存储字节切片
	capacity     int    // array 容量
	maxCapacity  int    // 切片最大容量
	head         int    // head索引位置
	tail         int    // tail索引位置
	count        int    // 存储条数
	rightMargin  int    // 左边距 TODO
	headerBuffer []byte // 序列化 头复用切片
	verbose      bool   // 是否开启打印详情
}

type queueError struct {
	message string
}

// getNeededSize returns the number of bytes an entry of length need in the queue
// 存储entry条目length需要的字节大小
// |  1-5 bytes  |  8 bytes  |  8 bytes  |  2 bytes  | n byte | m bytes |
// | entryLength | timestamp | hashValue | KeyLength |   Key  |  entry  |
// |     √       |           |           |           |        |         |
func getNeededSize(length int) int {
	var header int
	switch {
	case length < 127: // 1<<7-1
		header = 1
	case length < 16382: // 1<<14-2
		header = 2
	case length < 2097149: // 1<<21 -3
		header = 3
	case length < 268435452: // 1<<28 -4
		header = 4
	default:
		header = 5
	}

	return length + header
}

// NewBytesQueue initialize new bytes queue.
// capacity is used in bytes array allocation
// When verbose flag is set then information about memory allocation are printed
func NewBytesQueue(capacity int, maxCapacity int, verbose bool) *BytesQueue {
	return &BytesQueue{
		array:        make([]byte, capacity),
		capacity:     capacity,
		maxCapacity:  maxCapacity,
		headerBuffer: make([]byte, binary.MaxVarintLen32),
		tail:         leftMarginIndex,
		head:         leftMarginIndex,
		rightMargin:  leftMarginIndex,
		verbose:      verbose,
	}
}

// Reset removes all entries from queue
func (q *BytesQueue) Reset() {
	// Just reset indexes
	q.tail = leftMarginIndex
	q.head = leftMarginIndex
	q.rightMargin = leftMarginIndex
	q.count = 0
	q.full = false
}

// Push copies entry at the end of queue and moves tail pointer. Allocates more space if needed.
// Returns index for pushed data or error if maximum size queue limit is reached.
// Push 复制队列末尾的条目并移动尾指针。 如果需要，分配更多空间。 如果达到最大队列限制，则返回推送数据的索引或错误
func (q *BytesQueue) Push(data []byte) (int, error) {
	neededSize := getNeededSize(len(data)) // 条目长度：header+len(data)

	// 1.|---head****tail---| √
	// 2.|---head*******tail| ×
	// 3.|-head*********tail| ×（当tail后的空间不够存储一条条目时）
	// 4.|tail-----head*****| √
	// 5.|***tail-----head**| √
	// 6.|*******tail-head**| ×
	if !q.canInsertAfterTail(neededSize) { // 判断能否在tail标志位后添加条目（右边界）
		// 1.|head**********tail| ×
		// 2.|-head*********tail| ×
		// 3.|--—-head******tail| √
		// 4.|*******tail-head**| ×
		if q.canInsertBeforeHead(neededSize) {
			// |tail---head*******|
			q.tail = leftMarginIndex
		} else if q.capacity+neededSize >= q.maxCapacity && q.maxCapacity > 0 { // 超过最大容量
			return -1, &queueError{"Full queue. Maximum size limit reached."}
		} else {
			q.allocateAdditionalMemory(neededSize) // 扩容
		}
	}

	index := q.tail

	q.push(data, neededSize)

	return index, nil
}

// 扩容，原来2倍大的slice然后将旧数据迁移到新slice上
func (q *BytesQueue) allocateAdditionalMemory(minimum int) {
	start := time.Now()
	if q.capacity < minimum {
		q.capacity += minimum
	}
	q.capacity = q.capacity * 2
	if q.capacity > q.maxCapacity && q.maxCapacity > 0 {
		q.capacity = q.maxCapacity
	}

	oldArray := q.array
	q.array = make([]byte, q.capacity)

	if leftMarginIndex != q.rightMargin {
		copy(q.array, oldArray[:q.rightMargin])

		if q.tail <= q.head {
			if q.tail != q.head {
				// created slice is slightly larger then need but this is fine after only the needed bytes are copied
				q.push(make([]byte, q.head-q.tail), q.head-q.tail)
			}

			q.head = leftMarginIndex
			q.tail = q.rightMargin
		}
	}

	q.full = false

	if q.verbose {
		log.Printf("Allocated new queue in %s; Capacity: %d \n", time.Since(start), q.capacity)
	}
}

// |  1-5 bytes  |  8 bytes  |  8 bytes  |  2 bytes  | n byte | m bytes |
// | entryLength | timestamp | hashValue | KeyLength |   Key  |  entry  |
func (q *BytesQueue) push(data []byte, len int) {
	headerEntrySize := binary.PutUvarint(q.headerBuffer, uint64(len)) // 将header长度写到q.headerBuffer中
	q.copy(q.headerBuffer, headerEntrySize)                           // q.headerBuffer写到q.array

	q.copy(data, len-headerEntrySize) // wrapEntry写到q.array

	if q.tail > q.head {
		q.rightMargin = q.tail
	}
	if q.tail == q.head {
		q.full = true
	}

	q.count++
}

func (q *BytesQueue) copy(data []byte, len int) {
	q.tail += copy(q.array[q.tail:], data[:len])
}

// Pop reads the oldest entry from queue and moves head pointer to the next one
func (q *BytesQueue) Pop() ([]byte, error) {
	data, blockSize, err := q.peek(q.head)
	if err != nil {
		return nil, err
	}

	q.head += blockSize
	q.count--

	if q.head == q.rightMargin {
		q.head = leftMarginIndex
		if q.tail == q.rightMargin {
			q.tail = leftMarginIndex
		}
		q.rightMargin = q.tail
	}

	q.full = false

	return data, nil
}

// Peek reads the oldest entry from list without moving head pointer
func (q *BytesQueue) Peek() ([]byte, error) {
	data, _, err := q.peek(q.head)
	return data, err
}

// Get reads entry from index
func (q *BytesQueue) Get(index int) ([]byte, error) {
	data, _, err := q.peek(index)
	return data, err
}

// CheckGet checks if an entry can be read from index
func (q *BytesQueue) CheckGet(index int) error {
	return q.peekCheckErr(index)
}

// Capacity returns number of allocated bytes for queue
func (q *BytesQueue) Capacity() int {
	return q.capacity
}

// Len returns number of entries kept in queue
func (q *BytesQueue) Len() int {
	return q.count
}

// Error returns error message
func (e *queueError) Error() string {
	return e.message
}

// peekCheckErr is identical to peek, but does not actually return any data
func (q *BytesQueue) peekCheckErr(index int) error {

	if q.count == 0 {
		return errEmptyQueue
	}

	if index <= 0 {
		return errInvalidIndex
	}

	if index >= len(q.array) {
		return errIndexOutOfBounds
	}
	return nil
}

// peek returns the data from index and the number of bytes to encode the length of the data in uvarint format
func (q *BytesQueue) peek(index int) ([]byte, int, error) {
	err := q.peekCheckErr(index)
	if err != nil {
		return nil, 0, err
	}

	blockSize, n := binary.Uvarint(q.array[index:])
	return q.array[index+n : index+int(blockSize)], int(blockSize), nil
}

// canInsertAfterTail returns true if it's possible to insert an entry of size of need after the tail of the queue
// 如果可以在队列尾部之后插入一个需要大小的条目，则返回 true
func (q *BytesQueue) canInsertAfterTail(need int) bool {
	if q.full {
		return false
	}

	// 1.|---head****tail---| √
	// 2.|---head*******tail| ×
	// 3.|---head******tail*| ×
	if q.tail >= q.head {
		return q.capacity-q.tail >= need
	}
	// 1. there is exactly need bytes between head and tail, so we do not need
	// to reserve extra space for a potential empty entry when realloc this queue
	// 2. still have unused space between tail and head, then we must reserve
	// at least headerEntrySize bytes so we can put an empty entry
	// 4.|tail-----head*****| √
	// 5.|***tail-----head**| √
	return q.head-q.tail == need || q.head-q.tail >= need+minimumHeaderSize
}

// canInsertBeforeHead returns true if it's possible to insert an entry of size of need before the head of the queue
func (q *BytesQueue) canInsertBeforeHead(need int) bool {
	// |head**********tail| ×
	if q.full {
		return false
	}
	// |---head*******tail| ×
	// |---head******tail*| ×
	// |---head*******tail| √
	// |-head*********tail| ×
	if q.tail >= q.head {
		return q.head-leftMarginIndex == need || q.head-leftMarginIndex >= need+minimumHeaderSize
	}
	return q.head-q.tail == need || q.head-q.tail >= need+minimumHeaderSize
}
