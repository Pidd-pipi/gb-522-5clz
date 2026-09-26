package service

import (
	"testing"

	"fiber-otdr-fault-localization/backend/internal/constants"
	"fiber-otdr-fault-localization/backend/internal/model"
)

func algorithmOnlyEvent(id uint, distance float64) model.EventMarker {
	return model.EventMarker{ID: id, TraceID: 7, DistanceM: distance, EventType: constants.EventSplice, InsertionLossDB: 1, ReflectanceDB: -40, Confidence: 0.8, AlgorithmEventType: constants.EventSplice, AlgorithmDistanceM: distance, AlgorithmInsertionLossDB: 1, AlgorithmBacked: true}
}

func humanReviewedEvent(id uint, algorithmDistance, humanDistance float64) model.EventMarker {
	event := algorithmOnlyEvent(id, algorithmDistance)
	event.DistanceM = humanDistance
	event.EventType = constants.EventBend
	event.Reviewed = true
	event.ReviewNote = "人工确认为弯曲点"
	return event
}

func incomingEvent(distance float64) model.EventMarker {
	event := algorithmOnlyEvent(0, distance)
	event.EventType = constants.EventConnector
	event.AlgorithmEventType = constants.EventConnector
	event.InsertionLossDB = 2.5
	event.AlgorithmInsertionLossDB = 2.5
	event.Confidence = 0.9
	return event
}

func updateFor(plan eventMergePlan, id uint) (model.EventMarker, bool) {
	for _, event := range plan.updates {
		if event.ID == id {
			return event, true
		}
	}
	return model.EventMarker{}, false
}

func TestPlanEventMergeReplacesUnreviewedEvents(t *testing.T) {
	plan := planEventMerge([]model.EventMarker{algorithmOnlyEvent(1, 100)}, []model.EventMarker{incomingEvent(100)}, reviewMatchToleranceM)
	if len(plan.updates) != 0 || plan.synced != 0 || plan.orphaned != 0 {
		t.Fatalf("unreviewed events must not produce updates: %+v", plan)
	}
	if len(plan.inserts) != 1 || plan.inserts[0].DistanceM != 100 {
		t.Fatalf("expected one inserted replacement event, got %+v", plan.inserts)
	}
}

func TestPlanEventMergeKeepsJudgmentAndSyncsAlgorithmValues(t *testing.T) {
	plan := planEventMerge([]model.EventMarker{humanReviewedEvent(1, 100, 105)}, []model.EventMarker{incomingEvent(102)}, reviewMatchToleranceM)
	if plan.synced != 1 || plan.orphaned != 0 || len(plan.inserts) != 0 || len(plan.updates) != 1 {
		t.Fatalf("unexpected plan: %+v", plan)
	}
	merged := plan.updates[0]
	if merged.DistanceM != 105 || merged.EventType != constants.EventBend || merged.ReviewNote != "人工确认为弯曲点" || !merged.Reviewed {
		t.Fatalf("manual judgment must be preserved: %+v", merged)
	}
	if merged.AlgorithmDistanceM != 102 || merged.AlgorithmEventType != constants.EventConnector || merged.AlgorithmInsertionLossDB != 2.5 || merged.InsertionLossDB != 2.5 || merged.Confidence != 0.9 || !merged.AlgorithmBacked {
		t.Fatalf("latest algorithm values must be synced: %+v", merged)
	}
}

func TestPlanEventMergeMarksUnmatchedReviewedEventPending(t *testing.T) {
	plan := planEventMerge([]model.EventMarker{humanReviewedEvent(1, 500, 505)}, []model.EventMarker{incomingEvent(100)}, reviewMatchToleranceM)
	if plan.orphaned != 1 || plan.synced != 0 || len(plan.inserts) != 1 {
		t.Fatalf("unexpected plan: %+v", plan)
	}
	orphan, ok := updateFor(plan, 1)
	if !ok {
		t.Fatalf("reviewed event must stay in the list: %+v", plan)
	}
	if orphan.Reviewed || orphan.AlgorithmBacked {
		t.Fatalf("unmatched event must be marked pending review without algorithm basis: %+v", orphan)
	}
	if orphan.DistanceM != 505 || orphan.EventType != constants.EventBend || orphan.ReviewNote != "人工确认为弯曲点" || orphan.AlgorithmDistanceM != 500 {
		t.Fatalf("manual judgment and last algorithm basis must be kept: %+v", orphan)
	}
}

func TestPlanEventMergeRestoresRematchedOrphan(t *testing.T) {
	orphan := humanReviewedEvent(1, 300, 308)
	orphan.Reviewed = false
	orphan.AlgorithmBacked = false
	plan := planEventMerge([]model.EventMarker{orphan}, []model.EventMarker{incomingEvent(305)}, reviewMatchToleranceM)
	if plan.synced != 1 || plan.orphaned != 0 || len(plan.updates) != 1 {
		t.Fatalf("unexpected plan: %+v", plan)
	}
	restored := plan.updates[0]
	if !restored.Reviewed || !restored.AlgorithmBacked || restored.AlgorithmDistanceM != 305 || restored.ReviewNote != "人工确认为弯曲点" || restored.DistanceM != 308 {
		t.Fatalf("re-matched orphan must be restored with judgment intact: %+v", restored)
	}
}

func TestPlanEventMergeMatchesNearestPairOneToOne(t *testing.T) {
	plan := planEventMerge([]model.EventMarker{humanReviewedEvent(1, 100, 100), humanReviewedEvent(2, 106, 106)}, []model.EventMarker{incomingEvent(105)}, reviewMatchToleranceM)
	if plan.synced != 1 || plan.orphaned != 1 || len(plan.inserts) != 0 {
		t.Fatalf("unexpected plan: %+v", plan)
	}
	nearer, ok := updateFor(plan, 2)
	if !ok || !nearer.Reviewed || !nearer.AlgorithmBacked || nearer.AlgorithmDistanceM != 105 {
		t.Fatalf("nearest reviewed event must win the single candidate: %+v", nearer)
	}
	farther, ok := updateFor(plan, 1)
	if !ok || farther.Reviewed || farther.AlgorithmBacked {
		t.Fatalf("farther reviewed event must become pending review: %+v", farther)
	}
}

func TestPlanEventMergeHonorsToleranceBoundary(t *testing.T) {
	inside := planEventMerge([]model.EventMarker{humanReviewedEvent(1, 100, 100)}, []model.EventMarker{incomingEvent(125)}, reviewMatchToleranceM)
	if inside.synced != 1 {
		t.Fatalf("delta equal to tolerance must match: %+v", inside)
	}
	outside := planEventMerge([]model.EventMarker{humanReviewedEvent(1, 100, 100)}, []model.EventMarker{incomingEvent(126)}, reviewMatchToleranceM)
	if outside.orphaned != 1 || len(outside.inserts) != 1 {
		t.Fatalf("delta beyond tolerance must not match: %+v", outside)
	}
}

func TestPlanEventMergeHandlesMixedBatch(t *testing.T) {
	existing := []model.EventMarker{algorithmOnlyEvent(1, 50), humanReviewedEvent(2, 200, 205), humanReviewedEvent(3, 900, 900)}
	detected := []model.EventMarker{incomingEvent(48), incomingEvent(210), incomingEvent(1000)}
	plan := planEventMerge(existing, detected, reviewMatchToleranceM)
	if plan.synced != 1 || plan.orphaned != 1 || len(plan.inserts) != 2 || len(plan.updates) != 2 {
		t.Fatalf("unexpected plan: %+v", plan)
	}
	if plan.updates[0].ID != 2 || plan.updates[1].ID != 3 {
		t.Fatalf("updates must be ordered by event id for deterministic writes: %+v", plan.updates)
	}
	if !plan.updates[0].Reviewed || plan.updates[0].AlgorithmDistanceM != 210 {
		t.Fatalf("matched reviewed event must sync algorithm values: %+v", plan.updates[0])
	}
	if plan.updates[1].Reviewed || plan.updates[1].AlgorithmBacked {
		t.Fatalf("unmatched reviewed event must be pending review: %+v", plan.updates[1])
	}
}
