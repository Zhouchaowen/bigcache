package bigcache

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/allegro/bigcache/v3/queue"
)

type onRemoveCallback func(wrappedEntry []byte, reason RemoveReason)

// Metadata contains information of a specific entry
type Metadata struct {
	RequestCount uint32
}

type cacheShard struct {
	hashmap     map[uint64]uint32 // 存储索引的哈希表 (0 GC)
	entries     queue.BytesQueue  // 存储数据的具体数据结构
	lock        sync.RWMutex
	entryBuffer []byte           // 复用切片，用于减少分配
	onRemove    onRemoveCallback // 删除回调

	isVerbose    bool
	statsEnabled bool
	logger       Logger
	clock        clock  // 生成时间戳 时钟
	lifeWindow   uint64 // 条目驱除全局时间

	hashmapStats map[uint64]uint32 // 存储状态的哈希表
	stats        Stats             // 当前分片的缓存状态
}

func (s *cacheShard) getWithInfo(key string, hashedKey uint64) (entry []byte, resp Response, err error) {
	currentTime := uint64(s.clock.Epoch())
	s.lock.RLock()
	wrappedEntry, err := s.getWrappedEntry(hashedKey)
	if err != nil {
		s.lock.RUnlock()
		return nil, resp, err
	}
	if entryKey := readKeyFromEntry(wrappedEntry); key != entryKey {
		s.lock.RUnlock()
		s.collision()
		if s.isVerbose {
			s.logger.Printf("Collision detected. Both %q and %q have the same hash %x", key, entryKey, hashedKey)
		}
		return nil, resp, ErrEntryNotFound
	}

	entry = readEntry(wrappedEntry)
	oldestTimeStamp := readTimestampFromEntry(wrappedEntry)
	s.lock.RUnlock()
	s.hit(hashedKey)
	if currentTime-oldestTimeStamp >= s.lifeWindow {
		resp.EntryStatus = Expired
	}
	return entry, resp, nil
}

func (s *cacheShard) get(key string, hashedKey uint64) ([]byte, error) {
	s.lock.RLock()                                    // 加锁
	wrappedEntry, err := s.getWrappedEntry(hashedKey) // 获取包裹的条目
	if err != nil {
		s.lock.RUnlock()
		return nil, err
	}
	if entryKey := readKeyFromEntry(wrappedEntry); key != entryKey {
		s.lock.RUnlock()
		s.collision()
		if s.isVerbose {
			s.logger.Printf("Collision detected. Both %q and %q have the same hash %x", key, entryKey, hashedKey)
		}
		return nil, ErrEntryNotFound
	}
	entry := readEntry(wrappedEntry)
	s.lock.RUnlock()
	s.hit(hashedKey)

	return entry, nil
}

func (s *cacheShard) getWrappedEntry(hashedKey uint64) ([]byte, error) {
	itemIndex := s.hashmap[hashedKey]

	if itemIndex == 0 {
		s.miss()
		return nil, ErrEntryNotFound
	}

	wrappedEntry, err := s.entries.Get(int(itemIndex))
	if err != nil {
		s.miss()
		return nil, err
	}

	return wrappedEntry, err
}

// 获取有效的包裹条目
func (s *cacheShard) getValidWrapEntry(key string, hashedKey uint64) ([]byte, error) {
	wrappedEntry, err := s.getWrappedEntry(hashedKey)
	if err != nil {
		return nil, err
	}

	// 比较条目中的key
	if !compareKeyFromEntry(wrappedEntry, key) {
		s.collision()
		if s.isVerbose {
			s.logger.Printf("Collision detected. Both %q and %q have the same hash %x", key, readKeyFromEntry(wrappedEntry), hashedKey)
		}

		return nil, ErrEntryNotFound
	}
	s.hitWithoutLock(hashedKey) // 缓存命中+1

	return wrappedEntry, nil
}

func (s *cacheShard) set(key string, hashedKey uint64, entry []byte) error {
	currentTimestamp := uint64(s.clock.Epoch()) // 产生一个时间戳

	s.lock.Lock()

	if previousIndex := s.hashmap[hashedKey]; previousIndex != 0 { // 重置条目为0并删除s.hashmap的索引
		if previousEntry, err := s.entries.Get(int(previousIndex)); err == nil {
			resetKeyFromEntry(previousEntry) // 这个用到了slice的特性，切片持有源数据地址，改切片会导致改源数组
			//remove hashkey
			delete(s.hashmap, hashedKey)
		}
	}

	if oldestEntry, err := s.entries.Peek(); err == nil { // 尝试驱逐过期条目，触发删除回调
		s.onEvict(oldestEntry, currentTimestamp, s.removeOldestEntry)
	}

	w := wrapEntry(currentTimestamp, hashedKey, key, entry, &s.entryBuffer)

	for { // 循环尝试添加条目到切片末尾，添加失败尝试驱逐
		if index, err := s.entries.Push(w); err == nil {
			s.hashmap[hashedKey] = uint32(index)
			s.lock.Unlock()
			return nil
		}
		if s.removeOldestEntry(NoSpace) != nil { // 尝试驱逐过期条目
			s.lock.Unlock()
			return fmt.Errorf("entry is bigger than max shard size")
		}
	}
}

// 无锁添加
func (s *cacheShard) addNewWithoutLock(key string, hashedKey uint64, entry []byte) error {
	currentTimestamp := uint64(s.clock.Epoch())

	if oldestEntry, err := s.entries.Peek(); err == nil {
		s.onEvict(oldestEntry, currentTimestamp, s.removeOldestEntry)
	}

	w := wrapEntry(currentTimestamp, hashedKey, key, entry, &s.entryBuffer)

	for {
		if index, err := s.entries.Push(w); err == nil {
			s.hashmap[hashedKey] = uint32(index)
			return nil
		}
		if s.removeOldestEntry(NoSpace) != nil {
			return fmt.Errorf("entry is bigger than max shard size")
		}
	}
}

// 将包含多个entry的wrapper添加到切片末尾
func (s *cacheShard) setWrappedEntryWithoutLock(currentTimestamp uint64, w []byte, hashedKey uint64) error {
	if previousIndex := s.hashmap[hashedKey]; previousIndex != 0 { // 重置条目为0并删除s.hashmap的索引
		if previousEntry, err := s.entries.Get(int(previousIndex)); err == nil {
			resetKeyFromEntry(previousEntry)
		}
	}

	if oldestEntry, err := s.entries.Peek(); err == nil {
		s.onEvict(oldestEntry, currentTimestamp, s.removeOldestEntry)
	}

	for {
		if index, err := s.entries.Push(w); err == nil {
			s.hashmap[hashedKey] = uint32(index)
			return nil
		}
		if s.removeOldestEntry(NoSpace) != nil {
			return fmt.Errorf("entry is bigger than max shard size")
		}
	}
}

func (s *cacheShard) append(key string, hashedKey uint64, entry []byte) error {
	s.lock.Lock()
	wrappedEntry, err := s.getValidWrapEntry(key, hashedKey)

	if err == ErrEntryNotFound { // 没找直接添加
		err = s.addNewWithoutLock(key, hashedKey, entry)
		s.lock.Unlock()
		return err
	}
	if err != nil {
		s.lock.Unlock()
		return err
	}

	currentTimestamp := uint64(s.clock.Epoch())

	// 重新封装
	w := appendToWrappedEntry(currentTimestamp, wrappedEntry, entry, &s.entryBuffer)

	// 重新添加到末尾
	err = s.setWrappedEntryWithoutLock(currentTimestamp, w, hashedKey)
	s.lock.Unlock()

	return err
}

func (s *cacheShard) del(hashedKey uint64) error {
	// 先使用乐观锁，检查是否有该hashedKey的条目
	// Optimistic pre-check using only readlock
	s.lock.RLock()
	{
		itemIndex := s.hashmap[hashedKey]

		if itemIndex == 0 {
			s.lock.RUnlock()
			s.delmiss()
			return ErrEntryNotFound
		}

		if err := s.entries.CheckGet(int(itemIndex)); err != nil {
			s.lock.RUnlock()
			s.delmiss()
			return err
		}
	}
	s.lock.RUnlock()

	s.lock.Lock()
	{
		// After obtaining the writelock, we need to read the same again,
		// since the data delivered earlier may be stale now
		itemIndex := s.hashmap[hashedKey]

		if itemIndex == 0 {
			s.lock.Unlock()
			s.delmiss()
			return ErrEntryNotFound
		}

		wrappedEntry, err := s.entries.Get(int(itemIndex))
		if err != nil {
			s.lock.Unlock()
			s.delmiss()
			return err
		}

		delete(s.hashmap, hashedKey)      // 从s.hashmap删除
		s.onRemove(wrappedEntry, Deleted) // 删除回调
		if s.statsEnabled {
			delete(s.hashmapStats, hashedKey)
		}
		resetKeyFromEntry(wrappedEntry) // 覆盖条目，设置为0
	}
	s.lock.Unlock()

	s.delhit()
	return nil
}

// 驱逐
func (s *cacheShard) onEvict(oldestEntry []byte, currentTimestamp uint64, evict func(reason RemoveReason) error) bool {
	oldestTimestamp := readTimestampFromEntry(oldestEntry)
	if currentTimestamp-oldestTimestamp > s.lifeWindow { // 判断过期窗框
		evict(Expired)
		return true
	}
	return false
}

// 定期清理
func (s *cacheShard) cleanUp(currentTimestamp uint64) {
	s.lock.Lock()
	for {
		if oldestEntry, err := s.entries.Peek(); err != nil {
			break
		} else if evicted := s.onEvict(oldestEntry, currentTimestamp, s.removeOldestEntry); !evicted {
			break
		}
	}
	s.lock.Unlock()
}

// TODO
func (s *cacheShard) getEntry(hashedKey uint64) ([]byte, error) {
	s.lock.RLock()

	entry, err := s.getWrappedEntry(hashedKey)
	// copy entry
	newEntry := make([]byte, len(entry))
	copy(newEntry, entry)

	s.lock.RUnlock()

	return newEntry, err
}

// 获取s.hashmap的key列表
func (s *cacheShard) copyHashedKeys() (keys []uint64, next int) {
	s.lock.RLock()
	keys = make([]uint64, len(s.hashmap))

	for key := range s.hashmap {
		keys[next] = key
		next++
	}

	s.lock.RUnlock()
	return keys, next
}

// 删除最旧的条目，如果条目的hash被清空，则不执行删除回调
func (s *cacheShard) removeOldestEntry(reason RemoveReason) error {
	oldest, err := s.entries.Pop()
	if err == nil {
		hash := readHashFromEntry(oldest)
		if hash == 0 {
			// entry has been explicitly deleted with resetKeyFromEntry, ignore
			return nil
		}
		delete(s.hashmap, hash)
		s.onRemove(oldest, reason)
		if s.statsEnabled {
			delete(s.hashmapStats, hash)
		}
		return nil
	}
	return err
}

func (s *cacheShard) reset(config Config) {
	s.lock.Lock()
	s.hashmap = make(map[uint64]uint32, config.initialShardSize())
	s.entryBuffer = make([]byte, config.MaxEntrySize+headersSizeInBytes)
	s.entries.Reset()
	s.lock.Unlock()
}

func (s *cacheShard) len() int {
	s.lock.RLock()
	res := len(s.hashmap)
	s.lock.RUnlock()
	return res
}

func (s *cacheShard) capacity() int {
	s.lock.RLock()
	res := s.entries.Capacity()
	s.lock.RUnlock()
	return res
}

func (s *cacheShard) getStats() Stats {
	var stats = Stats{
		Hits:       atomic.LoadInt64(&s.stats.Hits),
		Misses:     atomic.LoadInt64(&s.stats.Misses),
		DelHits:    atomic.LoadInt64(&s.stats.DelHits),
		DelMisses:  atomic.LoadInt64(&s.stats.DelMisses),
		Collisions: atomic.LoadInt64(&s.stats.Collisions),
	}
	return stats
}

func (s *cacheShard) getKeyMetadataWithLock(key uint64) Metadata {
	s.lock.RLock()
	c := s.hashmapStats[key]
	s.lock.RUnlock()
	return Metadata{
		RequestCount: c,
	}
}

func (s *cacheShard) getKeyMetadata(key uint64) Metadata {
	return Metadata{
		RequestCount: s.hashmapStats[key],
	}
}

func (s *cacheShard) hit(key uint64) {
	atomic.AddInt64(&s.stats.Hits, 1)
	if s.statsEnabled {
		s.lock.Lock()
		s.hashmapStats[key]++
		s.lock.Unlock()
	}
}

func (s *cacheShard) hitWithoutLock(key uint64) {
	atomic.AddInt64(&s.stats.Hits, 1)
	if s.statsEnabled {
		s.hashmapStats[key]++
	}
}

func (s *cacheShard) miss() {
	atomic.AddInt64(&s.stats.Misses, 1)
}

func (s *cacheShard) delhit() {
	atomic.AddInt64(&s.stats.DelHits, 1)
}

func (s *cacheShard) delmiss() {
	atomic.AddInt64(&s.stats.DelMisses, 1)
}

func (s *cacheShard) collision() {
	atomic.AddInt64(&s.stats.Collisions, 1)
}

// 初始化分配
func initNewShard(config Config, callback onRemoveCallback, clock clock) *cacheShard {
	bytesQueueInitialCapacity := config.initialShardSize() * config.MaxEntrySize // 计算每个分片容量(每个分片条目*每个条目大小)
	maximumShardSizeInBytes := config.maximumShardSizeInBytes()                  // 获取每个分片最大堆内存使用量
	if maximumShardSizeInBytes > 0 && bytesQueueInitialCapacity > maximumShardSizeInBytes {
		bytesQueueInitialCapacity = maximumShardSizeInBytes
	}
	return &cacheShard{
		hashmap:      make(map[uint64]uint32, config.initialShardSize()),
		hashmapStats: make(map[uint64]uint32, config.initialShardSize()),
		entries:      *queue.NewBytesQueue(bytesQueueInitialCapacity, maximumShardSizeInBytes, config.Verbose), // 实际存储数据切片
		entryBuffer:  make([]byte, config.MaxEntrySize+headersSizeInBytes),                                     // 条目缓存结构，避免多次分配
		onRemove:     callback,                                                                                 // 删除回调

		isVerbose:    config.Verbose,
		logger:       newLogger(config.Logger),
		clock:        clock,
		lifeWindow:   uint64(config.LifeWindow.Seconds()), // 条目过期时间
		statsEnabled: config.StatsEnabled,
	}
}
