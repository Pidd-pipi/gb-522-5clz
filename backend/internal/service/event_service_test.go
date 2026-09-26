package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"fiber-otdr-fault-localization/backend/internal/constants"
	"fiber-otdr-fault-localization/backend/internal/dto"
	"fiber-otdr-fault-localization/backend/internal/model"
	"fiber-otdr-fault-localization/backend/internal/repository"
	"gorm.io/datatypes"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func newEventTestStore(t *testing.T) *repository.Store {
	t.Helper()
	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared", strings.NewReplacer("/", "-", " ", "-").Replace(t.Name()))
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("sqlite handle: %v", err)
	}
	sqlDB.SetMaxOpenConns(1)
	if err := db.AutoMigrate(&model.User{}, &model.FiberRoute{}, &model.TraceCapture{}, &model.EventMarker{}, &model.LocalizationCase{}, &model.AuditLog{}); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return repository.NewStore(db)
}

func syntheticPoints() []float64 {
	points := make([]float64, 240)
	for i := range points {
		value := 28 - float64(i)*0.035 + math.Sin(float64(i)*0.37)*0.08
		if i >= 62 {
			value -= 2.8
		}
		if i >= 154 {
			value -= 4.6
		}
		points[i] = value
	}
	return points
}

func seedEventTrace(t *testing.T, store *repository.Store, points []float64) model.TraceCapture {
	t.Helper()
	route := model.FiberRoute{RouteCode: "RT-MERGE", Name: "合并测试线路", LengthM: 2000, RefractiveIndex: 1.468, LaunchConnector: "SC/UPC", RouteStatus: "active"}
	if err := store.DB.Create(&route).Error; err != nil {
		t.Fatalf("seed route: %v", err)
	}
	raw, _ := json.Marshal(points)
	trace := model.TraceCapture{RouteID: route.ID, WavelengthNM: 1550, PulseWidthNS: 100, SampleIntervalNS: 100, RawPointsJSON: datatypes.JSON(raw), NoiseFloorDB: 0, DenoiseWindow: 5, PeakThresholdDB: 0.8, MergeWindow: 3, CapturedAt: time.Now(), UploadedBy: 1}
	if err := store.DB.Create(&trace).Error; err != nil {
		t.Fatalf("seed trace: %v", err)
	}
	return trace
}

func traceEvents(t *testing.T, store *repository.Store, traceID uint) []model.EventMarker {
	t.Helper()
	events, err := store.Events.ForTrace(traceID)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	return events
}

func auditCount(t *testing.T, store *repository.Store, action string) int64 {
	t.Helper()
	var count int64
	if err := store.DB.Model(&model.AuditLog{}).Where("action = ?", action).Count(&count).Error; err != nil {
		t.Fatalf("count audits: %v", err)
	}
	return count
}

var detectActor = Actor{ID: 9, Username: "maintainer", Role: constants.RoleAdmin, RequestID: "req-merge-test"}

func TestDetectSafelyMergesReviewedEvents(t *testing.T) {
	store := newEventTestStore(t)
	service := NewEventService(store)
	trace := seedEventTrace(t, store, syntheticPoints())
	params := dto.DetectEventsRequest{DenoiseWindow: 5, PeakThresholdDB: 0.8, MergeWindow: 3}

	summary, err := service.Detect(trace.ID, params, detectActor)
	if err != nil {
		t.Fatalf("first detect: %v", err)
	}
	if summary.DetectedCount != 2 || summary.SyncedReviewedCount != 0 || summary.OrphanedReviewedCount != 0 {
		t.Fatalf("unexpected first summary: %+v", summary)
	}
	events := traceEvents(t, store, trace.ID)
	if len(events) != 2 {
		t.Fatalf("expected 2 detected events, got %d", len(events))
	}

	target := events[0]
	humanDistance := target.DistanceM + 3
	if _, err := service.Review(target.ID, dto.ReviewEventRequest{EventType: constants.EventBend, DistanceM: &humanDistance, ReviewNote: "人工确认为弯曲点"}, detectActor); err != nil {
		t.Fatalf("review event: %v", err)
	}

	summary, err = service.Detect(trace.ID, params, detectActor)
	if err != nil {
		t.Fatalf("re-detect with same params: %v", err)
	}
	if summary.SyncedReviewedCount != 1 || summary.OrphanedReviewedCount != 0 {
		t.Fatalf("expected one synced reviewed event, got %+v", summary)
	}
	events = traceEvents(t, store, trace.ID)
	if len(events) != 2 {
		t.Fatalf("safe merge must not duplicate events, got %d", len(events))
	}
	merged, ok := updateFor(eventMergePlan{updates: events}, target.ID)
	if !ok {
		t.Fatalf("reviewed event %d must survive the re-run: %+v", target.ID, events)
	}
	if !merged.Reviewed || !merged.AlgorithmBacked || merged.EventType != constants.EventBend || merged.DistanceM != humanDistance || merged.ReviewNote != "人工确认为弯曲点" {
		t.Fatalf("manual judgment must be preserved: %+v", merged)
	}
	if merged.AlgorithmDistanceM != target.AlgorithmDistanceM || merged.AlgorithmEventType != target.AlgorithmEventType {
		t.Fatalf("algorithm raw values must be synced: %+v", merged)
	}

	summary, err = service.Detect(trace.ID, dto.DetectEventsRequest{DenoiseWindow: 5, PeakThresholdDB: 6, MergeWindow: 3}, detectActor)
	if err != nil {
		t.Fatalf("re-detect with stricter threshold: %v", err)
	}
	if summary.DetectedCount != 0 || summary.OrphanedReviewedCount != 1 {
		t.Fatalf("expected no detections and one orphan, got %+v", summary)
	}
	events = traceEvents(t, store, trace.ID)
	if len(events) != 1 || events[0].ID != target.ID {
		t.Fatalf("orphaned reviewed event must stay in the list: %+v", events)
	}
	if events[0].Reviewed || events[0].AlgorithmBacked || events[0].ReviewNote != "人工确认为弯曲点" || events[0].DistanceM != humanDistance {
		t.Fatalf("orphan must be pending review with judgment intact: %+v", events[0])
	}
	strict, err := store.Traces.Get(trace.ID)
	if err != nil || strict.PeakThresholdDB != 6 {
		t.Fatalf("processing parameters must follow the latest run: %+v", strict)
	}

	summary, err = service.Detect(trace.ID, params, detectActor)
	if err != nil {
		t.Fatalf("re-detect with restored params: %v", err)
	}
	if summary.SyncedReviewedCount != 1 || summary.OrphanedReviewedCount != 0 {
		t.Fatalf("expected the orphan to be restored, got %+v", summary)
	}
	events = traceEvents(t, store, trace.ID)
	if len(events) != 2 {
		t.Fatalf("restored run must yield 2 events, got %d", len(events))
	}
	restored, ok := updateFor(eventMergePlan{updates: events}, target.ID)
	if !ok || !restored.Reviewed || !restored.AlgorithmBacked || restored.ReviewNote != "人工确认为弯曲点" {
		t.Fatalf("re-matched event must regain algorithm basis with judgment intact: %+v", restored)
	}
	if got := auditCount(t, store, "trace.events_detected"); got != 4 {
		t.Fatalf("each successful run must append one audit entry, got %d", got)
	}
}

func TestDetectFailureLeavesEventsCurveAndAuditUntouched(t *testing.T) {
	store := newEventTestStore(t)
	service := NewEventService(store)
	trace := seedEventTrace(t, store, syntheticPoints())
	params := dto.DetectEventsRequest{DenoiseWindow: 5, PeakThresholdDB: 0.8, MergeWindow: 3}
	if _, err := service.Detect(trace.ID, params, detectActor); err != nil {
		t.Fatalf("initial detect: %v", err)
	}
	beforeEvents := traceEvents(t, store, trace.ID)
	beforeTrace, err := store.Traces.Get(trace.ID)
	if err != nil {
		t.Fatalf("reload trace: %v", err)
	}
	beforeAudits := auditCount(t, store, "trace.events_detected")

	if err := store.DB.Exec("DROP TABLE audit_logs").Error; err != nil {
		t.Fatalf("drop audit table: %v", err)
	}
	if _, err := service.Detect(trace.ID, dto.DetectEventsRequest{DenoiseWindow: 7, PeakThresholdDB: 2.5, MergeWindow: 9}, detectActor); err == nil {
		t.Fatal("detect must fail when the batch cannot be committed")
	}

	afterEvents := traceEvents(t, store, trace.ID)
	if len(afterEvents) != len(beforeEvents) {
		t.Fatalf("failed batch must not change events: before=%d after=%d", len(beforeEvents), len(afterEvents))
	}
	for i := range beforeEvents {
		if afterEvents[i].ID != beforeEvents[i].ID || afterEvents[i].DistanceM != beforeEvents[i].DistanceM || afterEvents[i].Reviewed != beforeEvents[i].Reviewed || afterEvents[i].AlgorithmBacked != beforeEvents[i].AlgorithmBacked {
			t.Fatalf("event %d changed despite failed batch: before=%+v after=%+v", i, beforeEvents[i], afterEvents[i])
		}
	}
	afterTrace, err := store.Traces.Get(trace.ID)
	if err != nil {
		t.Fatalf("reload trace after failure: %v", err)
	}
	if afterTrace.PeakThresholdDB != beforeTrace.PeakThresholdDB || afterTrace.DenoiseWindow != beforeTrace.DenoiseWindow || string(afterTrace.ProcessedJSON) != string(beforeTrace.ProcessedJSON) {
		t.Fatalf("failed batch must not change the processing curve or parameters")
	}
	if err := store.DB.AutoMigrate(&model.AuditLog{}); err != nil {
		t.Fatalf("restore audit table: %v", err)
	}
	if got := auditCount(t, store, "trace.events_detected"); got != 0 {
		t.Fatalf("failed batch must not append audit entries, got %d", got)
	}
	if beforeAudits != 1 {
		t.Fatalf("expected exactly one committed audit before the failure, got %d", beforeAudits)
	}
}

func TestTransactionRollsBackEventWritesOnError(t *testing.T) {
	store := newEventTestStore(t)
	trace := seedEventTrace(t, store, syntheticPoints())
	event := model.EventMarker{TraceID: trace.ID, DistanceM: 100, EventType: constants.EventSplice, InsertionLossDB: 1, ReflectanceDB: -40, Confidence: 0.8, AlgorithmEventType: constants.EventSplice, AlgorithmDistanceM: 100, AlgorithmInsertionLossDB: 1, AlgorithmBacked: true}
	if err := store.DB.Create(&event).Error; err != nil {
		t.Fatalf("seed event: %v", err)
	}
	err := store.Transaction(func(tx *repository.Store) error {
		if err := tx.Events.DeleteAlgorithmOnly(trace.ID); err != nil {
			return err
		}
		return errors.New("forced batch failure")
	})
	if err == nil {
		t.Fatal("transaction must surface the forced failure")
	}
	events := traceEvents(t, store, trace.ID)
	if len(events) != 1 || events[0].ID != event.ID {
		t.Fatalf("rolled back transaction must leave events untouched: %+v", events)
	}
}
