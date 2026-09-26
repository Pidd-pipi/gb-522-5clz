package service

import (
	"encoding/json"
	"math"
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

func mergeTestDB(t *testing.T) (*repository.Store, model.TraceCapture) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open("file:"+t.Name()+"?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, _ := db.DB()
	sqlDB.SetMaxOpenConns(1)
	if err := db.AutoMigrate(&model.User{}, &model.FiberRoute{}, &model.TraceCapture{}, &model.EventMarker{}, &model.LocalizationCase{}, &model.AuditLog{}); err != nil {
		t.Fatal(err)
	}
	store := repository.NewStore(db)
	route := model.FiberRoute{RouteCode: "T-MERGE", Name: "Merge Route", LengthM: 3000, RefractiveIndex: 1.468, LaunchConnector: "SC/APC", RouteStatus: "active"}
	if err := store.Routes.Create(&route); err != nil {
		t.Fatal(err)
	}
	points := make([]float64, 240)
	for i := range points {
		value := 28 - float64(i)*0.035
		if i >= 62 {
			value -= 2.8
		}
		if i >= 154 {
			value -= 4.6
		}
		points[i] = math.Round((value+math.Sin(float64(i)*0.37)*0.08)*1000) / 1000
	}
	raw, _ := json.Marshal(points)
	trace := model.TraceCapture{RouteID: route.ID, WavelengthNM: 1550, PulseWidthNS: 100, SampleIntervalNS: 100, RawPointsJSON: datatypes.JSON(raw), CapturedAt: time.Now(), UploadedBy: 1, DenoiseWindow: 5, PeakThresholdDB: 0.8, MergeWindow: 3}
	if err := store.Traces.Create(&trace); err != nil {
		t.Fatal(err)
	}
	return store, trace
}

func TestDetectSafeMergePreservesReviewAndSyncsAlgorithmValues(t *testing.T) {
	store, trace := mergeTestDB(t)
	events := NewEventService(store)
	actor := Actor{ID: 1, Username: "reviewer", Role: "reviewer", RequestID: "req-merge"}

	first, err := events.Detect(trace.ID, dto.DetectEventsRequest{DenoiseWindow: 5, PeakThresholdDB: 0.8, MergeWindow: 3}, actor)
	if err != nil {
		t.Fatal(err)
	}
	if first.DetectedCount != 2 || first.CreatedCount != 2 {
		t.Fatalf("expected two new events, got %+v", first)
	}
	initial, err := store.Events.ForTrace(trace.ID)
	if err != nil || len(initial) != 2 {
		t.Fatalf("load initial events: %v %v", initial, err)
	}
	far := initial[1]
	originalAlgoDistance := far.AlgorithmDistanceM
	reviewedDistance := 1570.5
	if _, err := events.Review(far.ID, dto.ReviewEventRequest{EventType: constants.EventBend, DistanceM: &reviewedDistance, ReviewNote: "人工确认：弯曲，距离已修订"}, actor); err != nil {
		t.Fatal(err)
	}

	summary, err := events.Detect(trace.ID, dto.DetectEventsRequest{DenoiseWindow: 5, PeakThresholdDB: 2.0, MergeWindow: 3}, actor)
	if err != nil {
		t.Fatal(err)
	}
	if summary.DetectedCount != 2 || summary.CreatedCount != 1 || summary.MatchedReviewedCount != 1 || summary.PendingReReviewCount != 0 {
		t.Fatalf("unexpected merge summary: %+v", summary)
	}
	merged, err := store.Events.ForTrace(trace.ID)
	if err != nil || len(merged) != 2 {
		t.Fatalf("load merged events: %v %v", merged, err)
	}
	var kept model.EventMarker
	for _, event := range merged {
		if event.ID == far.ID {
			kept = event
		} else if event.Reviewed {
			t.Fatalf("only the reviewed event may survive with its identity")
		}
	}
	if kept.ID != far.ID || !kept.Reviewed || kept.PendingReReview {
		t.Fatalf("reviewed event identity must be preserved: %+v", kept)
	}
	if kept.EventType != constants.EventBend || kept.DistanceM != reviewedDistance || kept.ReviewNote != "人工确认：弯曲，距离已修订" || kept.ReviewedBy == nil || kept.ReviewedAt == nil {
		t.Fatalf("human review fields must be retained: %+v", kept)
	}
	if kept.AlgorithmEventType != constants.EventSplice || kept.AlgorithmDistanceM != originalAlgoDistance {
		t.Fatalf("algorithm values must sync with the new run: %+v", kept)
	}
}

func TestDetectSafeMergeFlagsReviewedEventsWithoutNewBasis(t *testing.T) {
	store, trace := mergeTestDB(t)
	events := NewEventService(store)
	actor := Actor{ID: 1, Username: "reviewer", Role: "reviewer", RequestID: "req-stale"}

	if _, err := events.Detect(trace.ID, dto.DetectEventsRequest{PeakThresholdDB: 0.8, MergeWindow: 3}, actor); err != nil {
		t.Fatal(err)
	}
	initial, err := store.Events.ForTrace(trace.ID)
	if err != nil {
		t.Fatal(err)
	}
	note := "已复核：远端断点"
	if _, err := events.Review(initial[1].ID, dto.ReviewEventRequest{EventType: constants.EventBreak, ReviewNote: note}, actor); err != nil {
		t.Fatal(err)
	}

	summary, err := events.Detect(trace.ID, dto.DetectEventsRequest{PeakThresholdDB: 5.0, MergeWindow: 3}, actor)
	if err != nil {
		t.Fatal(err)
	}
	if summary.DetectedCount != 0 || summary.CreatedCount != 0 || summary.MatchedReviewedCount != 0 || summary.PendingReReviewCount != 1 {
		t.Fatalf("unexpected summary for empty detection: %+v", summary)
	}
	remaining, err := store.Events.ForTrace(trace.ID)
	if err != nil || len(remaining) != 1 {
		t.Fatalf("stale reviewed event must remain listed: %v %v", remaining, err)
	}
	stale := remaining[0]
	if !stale.Reviewed || !stale.PendingReReview || stale.ReviewNote != note {
		t.Fatalf("reviewed event must be flagged pending re-review with review intact: %+v", stale)
	}

	recoveredDistance := 1570.0
	if _, err := events.Review(stale.ID, dto.ReviewEventRequest{EventType: constants.EventBreak, DistanceM: &recoveredDistance, ReviewNote: "二次确认：保留原判定"}, actor); err != nil {
		t.Fatal(err)
	}
	reloaded, err := store.Events.Get(stale.ID)
	if err != nil {
		t.Fatal(err)
	}
	if reloaded.PendingReReview {
		t.Fatalf("a new review must clear the pending re-review flag")
	}
}

func TestDetectFailureLeavesEventsCurveAndAuditUntouched(t *testing.T) {
	store, trace := mergeTestDB(t)
	events := NewEventService(store)
	actor := Actor{ID: 1, Username: "analyst", Role: "analyst", RequestID: "req-rollback"}

	if _, err := events.Detect(trace.ID, dto.DetectEventsRequest{PeakThresholdDB: 0.8, MergeWindow: 3}, actor); err != nil {
		t.Fatal(err)
	}
	initial, err := store.Events.ForTrace(trace.ID)
	if err != nil {
		t.Fatal(err)
	}
	var auditsBefore int64
	if err := store.DB.Model(&model.AuditLog{}).Count(&auditsBefore).Error; err != nil {
		t.Fatal(err)
	}

	if err := store.DB.Migrator().DropTable("audit_logs"); err != nil {
		t.Fatal(err)
	}
	if _, err := events.Detect(trace.ID, dto.DetectEventsRequest{PeakThresholdDB: 2.0, MergeWindow: 3}, actor); err == nil {
		t.Fatal("expected failed batch to surface an error")
	}

	after, err := store.Events.ForTrace(trace.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(initial) || after[0].ID != initial[0].ID || after[1].ID != initial[1].ID {
		t.Fatalf("events must stay unchanged after failed batch: before=%v after=%v", initial, after)
	}
	reloadedTrace, err := store.Traces.Get(trace.ID)
	if err != nil {
		t.Fatal(err)
	}
	if reloadedTrace.PeakThresholdDB != 0.8 {
		t.Fatalf("processing parameters must stay unchanged after failed batch: %v", reloadedTrace.PeakThresholdDB)
	}
	if len(reloadedTrace.ProcessedJSON) == 0 {
		t.Fatal("processed curve must stay unchanged after failed batch")
	}
}
