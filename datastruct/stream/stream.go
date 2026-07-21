package stream

import (
	"fmt"
	"log/slog"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Entry represents a single entry in a stream, containing a unique ID and field-value pairs.
type Entry struct {
	ID     string
	Fields map[string]string
}

// PendingEntry represents a message that has been delivered to a consumer but not yet acknowledged.
type PendingEntry struct {
	ID            string
	ConsumerName  string
	DeliveryTime  time.Time
	DeliveryCount int64
}

// Consumer represents a consumer within a consumer group that reads from the stream.
type Consumer struct {
	Name          string
	Pending       []*PendingEntry
}

// ConsumerGroup represents a group of consumers that cooperatively consume a stream.
type ConsumerGroup struct {
	Name            string
	LastDeliveredID string
	Consumers       map[string]*Consumer
	Pending         []*PendingEntry
}

// Stream implements a Redis-compatible stream data structure with ordered entries and consumer group support.
type Stream struct {
	mu      sync.RWMutex
	entries []*Entry
	// lastID tracks the last entry ID for auto-generation
	lastID       string
	lastTimeMs   int64
	lastSeq      int64

	// consumer groups
	groups map[string]*ConsumerGroup
}

// NewStream creates a new empty stream.
func NewStream() *Stream {
	slog.Debug("NewStream")
	return &Stream{
		entries: make([]*Entry, 0),
		lastID:  "0-0",
		groups:  make(map[string]*ConsumerGroup),
	}
}

// Len returns the number of entries in the stream.
func (s *Stream) Len() int {
	slog.Debug("Stream.Len")
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.entries)
}

// parseID parses a stream entry ID string (format: "timestamp-seq") into its components.
func parseID(id string) (timeMs, seq int64, err error) {
	slog.Debug("parseID", "id", id)
	parts := strings.SplitN(id, "-", 2)
	timeMs, err = strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("ERR Invalid stream ID specified")
	}
	if len(parts) == 2 {
		seq, err = strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return 0, 0, fmt.Errorf("ERR Invalid stream ID specified")
		}
	} else {
		seq = 0
	}
	return timeMs, seq, nil
}

// compareIDs compares two stream entry IDs, returning -1, 0, or 1.
func compareIDs(a, b string) int {
	slog.Debug("compareIDs", "a", a, "b", b)
	ta, sa, err := parseID(a)
	if err != nil {
		return 0
	}
	tb, sb, err := parseID(b)
	if err != nil {
		return 0
	}
	if ta != tb {
		if ta < tb {
			return -1
		}
		return 1
	}
	if sa < sb {
		return -1
	} else if sa > sb {
		return 1
	}
	return 0
}

// generateID auto-generates a stream entry ID based on the current time or a given prefix timestamp.
func (s *Stream) generateID(prefixTimeMs int64) string {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UnixMilli()
	if prefixTimeMs > 0 {
		now = prefixTimeMs
	}

	if now > s.lastTimeMs {
		s.lastTimeMs = now
		s.lastSeq = 0
		s.lastID = fmt.Sprintf("%d-0", now)
		return s.lastID
	}

	s.lastSeq++
	s.lastID = fmt.Sprintf("%d-%d", s.lastTimeMs, s.lastSeq)
	return s.lastID
}

// Add appends a new entry to the stream with the given ID and field-value pairs. If id is "*", a new ID is auto-generated.
func (s *Stream) Add(id string, fields map[string]string) (string, error) {
	slog.Debug("Stream.Add", "id", id)
	s.mu.Lock()
	defer s.mu.Unlock()

	if id == "*" || id == "" {
		id = s.generateID(0)
	} else if strings.HasSuffix(id, "-*") {
		prefix := strings.TrimSuffix(id, "-*")
		prefixTimeMs, err := strconv.ParseInt(prefix, 10, 64)
		if err != nil {
			return "", fmt.Errorf("ERR Invalid stream ID specified")
		}
		id = s.generateID(prefixTimeMs)
	} else {
		// Validate the provided ID
		if strings.TrimSpace(id) == "" {
			return "", fmt.Errorf("ERR Invalid stream ID specified")
		}
		timeMs, seq, err := parseID(id)
		if err != nil {
			return "", err
		}
		// ID must be > 0-0
		if timeMs < 0 || (timeMs == 0 && seq <= 0) {
			return "", fmt.Errorf("ERR The ID specified must be greater than 0-0")
		}
		// Must be > last entry ID
		if len(s.entries) > 0 {
			lastEntryID := s.entries[len(s.entries)-1].ID
			if compareIDs(id, lastEntryID) <= 0 {
				return "", fmt.Errorf("ERR The ID specified is not greater than the last entry ID")
			}
		}
		if seq > s.lastSeq || timeMs > s.lastTimeMs {
			s.lastTimeMs = timeMs
			s.lastSeq = seq
			s.lastID = id
		}
	}

	entry := &Entry{ID: id, Fields: fields}
	s.entries = append(s.entries, entry)
	return id, nil
}

// Range returns entries with IDs between start and end (inclusive), optionally limited by count.
func (s *Stream) Range(start, end string, count int) []*Entry {
	slog.Debug("Stream.Range", "start", start, "end", end, "count", count)
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]*Entry, 0)
	for _, entry := range s.entries {
		if compareIDs(entry.ID, start) < 0 {
			continue
		}
		if compareIDs(entry.ID, end) > 0 {
			break
		}
		result = append(result, entry)
		if count > 0 && len(result) >= count {
			break
		}
	}
	return result
}

// Read returns entries after the given IDs for each stream key, optionally limited by count.
func (s *Stream) Read(ids map[string]string, count int) map[string][]*Entry {
	slog.Debug("Stream.Read", "count", count)
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make(map[string][]*Entry)

	// For each key (stream), collect entries after the given ID
	for _, entry := range s.entries {
		for key, afterID := range ids {
			if compareIDs(entry.ID, afterID) > 0 {
				result[key] = append(result[key], entry)
			}
		}
	}

	if count > 0 {
		for key := range result {
			if len(result[key]) > count {
				result[key] = result[key][:count]
			}
		}
	}
	return result
}

// ReadAfter returns entries with IDs greater than lastID, optionally limited by count.
func (s *Stream) ReadAfter(lastID string, count int) []*Entry {
	slog.Debug("Stream.ReadAfter", "lastID", lastID, "count", count)
	s.mu.RLock()
	defer s.mu.RUnlock()

	if count <= 0 {
		count = math.MaxInt32
	}

	result := make([]*Entry, 0)
	for _, entry := range s.entries {
		if compareIDs(entry.ID, lastID) > 0 {
			result = append(result, entry)
			if len(result) >= count {
				break
			}
		}
	}
	return result
}

// GetEntry returns the entry with the given ID, or nil if not found.
func (s *Stream) GetEntry(id string) *Entry {
	slog.Debug("Stream.GetEntry", "id", id)
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, entry := range s.entries {
		if entry.ID == id {
			return entry
		}
	}
	return nil
}

// Delete removes entries with the given IDs from the stream and returns the number removed.
func (s *Stream) Delete(ids []string) int {
	slog.Debug("Stream.Delete", "ids", ids)
	s.mu.Lock()
	defer s.mu.Unlock()

	deleted := 0
	newEntries := make([]*Entry, 0, len(s.entries))
	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}
	for _, entry := range s.entries {
		if idSet[entry.ID] {
			deleted++
			continue
		}
		newEntries = append(newEntries, entry)
	}
	s.entries = newEntries
	return deleted
}

// Trim removes oldest entries until the stream length is at most maxLen. Returns the number removed.
func (s *Stream) Trim(maxLen int) int {
	slog.Debug("Stream.Trim", "maxLen", maxLen)
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.entries) <= maxLen {
		return 0
	}
	removed := len(s.entries) - maxLen
	s.entries = s.entries[len(s.entries)-maxLen:]
	return removed
}

// GroupCreate creates a new consumer group for the stream with the given name and last delivered ID.
func (s *Stream) GroupCreate(groupName, lastDeliveredID string) error {
	slog.Debug("Stream.GroupCreate", "groupName", groupName, "lastDeliveredID", lastDeliveredID)
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.groups[groupName]; exists {
		return fmt.Errorf("BUSYGROUP Consumer Group name already exists")
	}

	s.groups[groupName] = &ConsumerGroup{
		Name:            groupName,
		LastDeliveredID: lastDeliveredID,
		Consumers:       make(map[string]*Consumer),
		Pending:         make([]*PendingEntry, 0),
	}
	return nil
}

// GroupDestroy removes a consumer group from the stream. Returns false if the group does not exist.
func (s *Stream) GroupDestroy(groupName string) bool {
	slog.Debug("Stream.GroupDestroy", "groupName", groupName)
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.groups[groupName]; !exists {
		return false
	}
	delete(s.groups, groupName)
	return true
}

// GetOrCreateConsumer returns an existing consumer or creates a new one in the given consumer group.
func (s *Stream) GetOrCreateConsumer(groupName, consumerName string) (*Consumer, error) {
	slog.Debug("Stream.GetOrCreateConsumer", "groupName", groupName, "consumerName", consumerName)
	s.mu.Lock()
	defer s.mu.Unlock()

	group, exists := s.groups[groupName]
	if !exists {
		return nil, fmt.Errorf("NOGROUP No such consumer group '%s'", groupName)
	}

	consumer, exists := group.Consumers[consumerName]
	if !exists {
		consumer = &Consumer{Name: consumerName}
		group.Consumers[consumerName] = consumer
	}
	return consumer, nil
}

// ReadGroup reads entries for a consumer group consumer. If lastID is ">", new entries after the group's last delivered ID are returned; otherwise pending entries are returned.
func (s *Stream) ReadGroup(groupName, consumerName, lastID string, count int) ([]*Entry, string, error) {
	slog.Debug("Stream.ReadGroup", "groupName", groupName, "consumerName", consumerName, "lastID", lastID, "count", count)
	s.mu.Lock()
	defer s.mu.Unlock()

	group, exists := s.groups[groupName]
	if !exists {
		return nil, "", fmt.Errorf("NOGROUP No such consumer group '%s'", groupName)
	}

	consumer, exists := group.Consumers[consumerName]
	if !exists {
		consumer = &Consumer{Name: consumerName}
		group.Consumers[consumerName] = consumer
	}

	deliverID := group.LastDeliveredID
	if lastID != ">" {
		deliverID = lastID
	}

	if count <= 0 {
		count = math.MaxInt32
	}

	result := make([]*Entry, 0)
	for _, entry := range s.entries {
		if compareIDs(entry.ID, deliverID) > 0 {
			result = append(result, entry)
			if len(result) >= count {
				break
			}
		}
	}

	// Update last delivered ID and add to pending for the consumer
	for _, entry := range result {
		if compareIDs(entry.ID, group.LastDeliveredID) > 0 {
			group.LastDeliveredID = entry.ID
		}
		pe := &PendingEntry{
			ID:            entry.ID,
			ConsumerName:  consumerName,
			DeliveryTime:  time.Now(),
			DeliveryCount: 1,
		}
		consumer.Pending = append(consumer.Pending, pe)
		group.Pending = append(group.Pending, pe)
	}

	return result, group.LastDeliveredID, nil
}

// Ack acknowledges delivery of messages with the given IDs in the consumer group, removing them from pending lists.
func (s *Stream) Ack(groupName string, ids []string) int {
	slog.Debug("Stream.Ack", "groupName", groupName)
	s.mu.Lock()
	defer s.mu.Unlock()

	group, exists := s.groups[groupName]
	if !exists {
		return 0
	}

	acked := 0
	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	newPending := make([]*PendingEntry, 0)
	for _, pe := range group.Pending {
		if idSet[pe.ID] {
			acked++
			if consumer, ok := group.Consumers[pe.ConsumerName]; ok {
				newConsumerPending := make([]*PendingEntry, 0)
				for _, cpe := range consumer.Pending {
					if cpe.ID != pe.ID {
						newConsumerPending = append(newConsumerPending, cpe)
					}
				}
				consumer.Pending = newConsumerPending
			}
			continue
		}
		newPending = append(newPending, pe)
	}
	group.Pending = newPending
	return acked
}

// Nack moves messages with the given IDs back to the pending list for re-delivery.
func (s *Stream) Nack(groupName string, ids []string) int {
	slog.Info("Stream.Nack", "groupName", groupName)
	s.mu.Lock()
	defer s.mu.Unlock()

	group, exists := s.groups[groupName]
	if !exists {
		return 0
	}

	// XNACK takes IDs that have already been acknowledged and marks them as pending again.
	// This allows reprocessing messages that were already acknowledged.
	nacked := 0
	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	for _, entry := range s.entries {
		if idSet[entry.ID] {
			pe := &PendingEntry{
				ID:            entry.ID,
				ConsumerName:  "$",
				DeliveryTime:  time.Now(),
				DeliveryCount: 0,
			}
			group.Pending = append(group.Pending, pe)
			nacked++
		}
	}
	return nacked
}

// GroupSetID sets the last delivered ID for a consumer group.
func (s *Stream) GroupSetID(groupName, lastDeliveredID string) error {
	slog.Debug("Stream.GroupSetID", "groupName", groupName, "lastDeliveredID", lastDeliveredID)
	s.mu.Lock()
	defer s.mu.Unlock()

	group, exists := s.groups[groupName]
	if !exists {
		return fmt.Errorf("NOGROUP No such consumer group '%s'", groupName)
	}
	group.LastDeliveredID = lastDeliveredID
	return nil
}

// GroupDelConsumer removes a consumer from a group and returns the number of pending entries it had.
func (s *Stream) GroupDelConsumer(groupName, consumerName string) int {
	slog.Debug("Stream.GroupDelConsumer", "groupName", groupName, "consumerName", consumerName)
	s.mu.Lock()
	defer s.mu.Unlock()

	group, exists := s.groups[groupName]
	if !exists {
		return 0
	}

	consumer, exists := group.Consumers[consumerName]
	if !exists {
		return 0
	}

	pendingCount := len(consumer.Pending)

	newGroupPending := make([]*PendingEntry, 0)
	for _, pe := range group.Pending {
		if pe.ConsumerName == consumerName {
			continue
		}
		newGroupPending = append(newGroupPending, pe)
	}
	group.Pending = newGroupPending

	delete(group.Consumers, consumerName)
	return pendingCount
}

// Info returns stream metadata including length, first/last entry IDs, and group count.
func (s *Stream) Info() map[string]interface{} {
	slog.Debug("Stream.Info")
	s.mu.RLock()
	defer s.mu.RUnlock()

	info := make(map[string]interface{})
	info["length"] = len(s.entries)
	if len(s.entries) > 0 {
		info["first-entry"] = s.entries[0].ID
		info["last-entry"] = s.entries[len(s.entries)-1].ID
	}
	info["groups"] = len(s.groups)
	return info
}

// GroupInfo returns metadata about all consumer groups in the stream.
func (s *Stream) GroupInfo() []map[string]interface{} {
	slog.Debug("Stream.GroupInfo")
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]map[string]interface{}, 0)
	for _, group := range s.groups {
		gInfo := make(map[string]interface{})
		gInfo["name"] = group.Name
		gInfo["consumers"] = len(group.Consumers)
		gInfo["pending"] = len(group.Pending)
		gInfo["last-delivered-id"] = group.LastDeliveredID
		result = append(result, gInfo)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i]["name"].(string) < result[j]["name"].(string)
	})
	return result
}

// ConsumerInfo returns metadata about all consumers in a given consumer group.
func (s *Stream) ConsumerInfo(groupName string) []map[string]interface{} {
	slog.Debug("Stream.ConsumerInfo", "groupName", groupName)
	s.mu.RLock()
	defer s.mu.RUnlock()

	group, exists := s.groups[groupName]
	if !exists {
		return nil
	}

	result := make([]map[string]interface{}, 0)
	for _, consumer := range group.Consumers {
		cInfo := make(map[string]interface{})
		cInfo["name"] = consumer.Name
		cInfo["pending"] = len(consumer.Pending)
		result = append(result, cInfo)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i]["name"].(string) < result[j]["name"].(string)
	})
	return result
}

// PendingInfo returns pending entries for a consumer group within the given ID range and count limit.
func (s *Stream) PendingInfo(groupName string, start, end string, count int) ([]*PendingEntry, error) {
	slog.Debug("Stream.PendingInfo", "groupName", groupName, "start", start, "end", end, "count", count)
	s.mu.RLock()
	defer s.mu.RUnlock()

	group, exists := s.groups[groupName]
	if !exists {
		return nil, fmt.Errorf("NOGROUP No such consumer group '%s'", groupName)
	}

	result := make([]*PendingEntry, 0)
	for _, pe := range group.Pending {
		if count > 0 && len(result) >= count {
			break
		}
		if start != "-" && compareIDs(pe.ID, start) < 0 {
			continue
		}
		if end != "+" && compareIDs(pe.ID, end) > 0 {
			continue
		}
		result = append(result, pe)
	}
	return result, nil
}

// LastID returns the ID of the last entry in the stream, or "0-0" if the stream is empty.
func (s *Stream) LastID() string {
	slog.Debug("Stream.LastID")
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.entries) == 0 {
		return "0-0"
	}
	return s.entries[len(s.entries)-1].ID
}
