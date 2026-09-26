package service

import (
	"encoding/json"
	"errors"
	"math"
	"sort"
	"time"

	"fiber-otdr-fault-localization/backend/internal/algorithm"
	"fiber-otdr-fault-localization/backend/internal/dto"
	"fiber-otdr-fault-localization/backend/internal/model"
	"fiber-otdr-fault-localization/backend/internal/repository"
	"gorm.io/datatypes"
)

// reviewMatchToleranceM 与基线比对的默认距离容差一致：重跑检测时，
// 带人工判定的事件按原算法距离在该容差内寻找相近的新检出事件。
const reviewMatchToleranceM = 25.0

type EventService struct{ store *repository.Store }

func NewEventService(store *repository.Store) *EventService { return &EventService{store} }

func (s *EventService) Detect(traceID uint, request dto.DetectEventsRequest, actor Actor) (dto.DetectionSummary, error) {
	trace, err := s.store.Traces.Get(traceID)
	if errors.Is(err, repository.ErrNotFound) {
		return dto.DetectionSummary{}, notFound("trace")
	}
	if err != nil {
		return dto.DetectionSummary{}, internal("get trace failed", err)
	}
	route, err := s.store.Routes.Get(trace.RouteID)
	if err != nil {
		return dto.DetectionSummary{}, internal("get trace route failed", err)
	}
	var raw []float64
	if err := json.Unmarshal(trace.RawPointsJSON, &raw); err != nil {
		return dto.DetectionSummary{}, internal("decode raw trace failed", err)
	}
	window := request.DenoiseWindow
	if window == 0 {
		window = trace.DenoiseWindow
	}
	if window == 0 {
		window = 5
	}
	threshold := request.PeakThresholdDB
	if threshold == 0 {
		threshold = trace.PeakThresholdDB
	}
	if threshold == 0 {
		threshold = 0.8
	}
	merge := request.MergeWindow
	if merge == 0 {
		merge = trace.MergeWindow
	}
	if merge == 0 {
		merge = 3
	}
	filtered, err := algorithm.MovingMedian(raw, window)
	if err != nil {
		return dto.DetectionSummary{}, &AppError{CodeAlgorithmInput, 422, "trace denoising failed", err}
	}
	noise, err := algorithm.EstimateNoiseFloor(filtered)
	if err != nil {
		return dto.DetectionSummary{}, &AppError{CodeAlgorithmInput, 422, "noise floor estimation failed", err}
	}
	detected, rejected, err := algorithm.Detect(filtered, threshold, merge, trace.SampleIntervalNS, route.RefractiveIndex, route.LengthM)
	if err != nil {
		return dto.DetectionSummary{}, &AppError{CodeAlgorithmInput, 422, "event detection failed", err}
	}
	events := make([]model.EventMarker, 0, len(detected))
	for _, item := range detected {
		events = append(events, model.EventMarker{TraceID: trace.ID, DistanceM: item.DistanceM, EventType: item.Type, InsertionLossDB: item.InsertionLossDB, ReflectanceDB: item.ReflectanceDB, Confidence: item.Confidence, AlgorithmEventType: item.Type, AlgorithmDistanceM: item.DistanceM, AlgorithmInsertionLossDB: item.InsertionLossDB, AlgorithmBacked: true})
	}
	processed, _ := json.Marshal(filtered)
	var plan eventMergePlan
	err = s.store.Transaction(func(tx *repository.Store) error {
		if err := tx.Traces.UpdateProcessing(trace.ID, datatypes.JSON(processed), noise, window, threshold, merge); err != nil {
			return err
		}
		existing, err := tx.Events.ForTrace(trace.ID)
		if err != nil {
			return err
		}
		plan = planEventMerge(existing, events, reviewMatchToleranceM)
		if err := tx.Events.DeleteAlgorithmOnly(trace.ID); err != nil {
			return err
		}
		for i := range plan.updates {
			if err := tx.Events.ApplyMerge(&plan.updates[i]); err != nil {
				return err
			}
		}
		if err := tx.Events.CreateBatch(plan.inserts); err != nil {
			return err
		}
		params := map[string]any{"denoise_window": window, "peak_threshold_db": threshold, "merge_window": merge, "noise_floor_db": noise, "detected": len(events), "rejected_out_of_bounds": rejected, "inserted": len(plan.inserts), "synced_reviewed": plan.synced, "orphaned_reviewed": plan.orphaned}
		return tx.Audits.Create(audit(actor, "trace.events_detected", "TraceCapture", trace.ID, &route.ID, "{}", snapshot(params)))
	})
	if err != nil {
		return dto.DetectionSummary{}, internal("save detected events failed", err)
	}
	return dto.DetectionSummary{TraceID: trace.ID, DetectedCount: len(events), NoiseFloorDB: noise, ThresholdDB: threshold, RejectedCount: rejected, SyncedReviewedCount: plan.synced, OrphanedReviewedCount: plan.orphaned}, nil
}

func (s *EventService) List(query dto.EventQuery) ([]model.EventMarker, dto.Pagination, error) {
	normalizePage(&query.Page, &query.PageSize)
	items, total, err := s.store.Events.List(query)
	if err != nil {
		return nil, dto.Pagination{}, internal("list events failed", err)
	}
	return items, dto.Pagination{Page: query.Page, PageSize: query.PageSize, Total: total}, nil
}

func (s *EventService) Review(id uint, request dto.ReviewEventRequest, actor Actor) (model.EventMarker, error) {
	if !request.EventType.Valid() {
		return model.EventMarker{}, invalid("event_type is not supported", nil)
	}
	event, err := s.store.Events.Get(id)
	if errors.Is(err, repository.ErrNotFound) {
		return event, notFound("event")
	}
	if err != nil {
		return event, internal("get event failed", err)
	}
	trace, err := s.store.Traces.Get(event.TraceID)
	if err != nil {
		return event, internal("get event trace failed", err)
	}
	route, err := s.store.Routes.Get(trace.RouteID)
	if err != nil {
		return event, internal("get event route failed", err)
	}
	before := event
	event.EventType = request.EventType
	if request.DistanceM != nil {
		event.DistanceM = *request.DistanceM
	}
	if event.DistanceM > route.LengthM {
		return event, invalid("reviewed distance exceeds route length", nil)
	}
	now := time.Now()
	event.Reviewed = true
	event.ReviewNote = request.ReviewNote
	event.ReviewedBy = &actor.ID
	event.ReviewedAt = &now
	err = s.store.Transaction(func(tx *repository.Store) error {
		if err := tx.Events.Review(&event); err != nil {
			return err
		}
		return tx.Audits.Create(audit(actor, "event.reviewed", "EventMarker", event.ID, &route.ID, snapshot(before), snapshot(event)))
	})
	if err != nil {
		return event, internal("review event failed", err)
	}
	return event, nil
}

// eventMergePlan 描述一次安全合并重跑对事件列表的全部写操作，
// 由调用方在单个事务内应用，失败时整体回滚。
type eventMergePlan struct {
	inserts  []model.EventMarker // 新参数下首次检出、需要创建的事件
	updates  []model.EventMarker // 保留人工判定、需要回写的事件（同步算法原值或标为待复核）
	synced   int                 // 带人工判定且重新命中、已同步最新算法原值的事件数
	orphaned int                 // 新参数下不再命中、被标为待复核的事件数
}

// planEventMerge 计算重跑检测的安全合并方案：
// 未复核且仍有算法依据的事件被新检出结果整体替换；带人工判定的事件
// （已复核，或此前已失去算法依据）按原算法距离与新事件一对一最近匹配，
// 命中则保留人工判定并同步最新算法原值，未命中则保留在列表并标为待复核。
func planEventMerge(existing, detected []model.EventMarker, toleranceM float64) eventMergePlan {
	plan := eventMergePlan{inserts: []model.EventMarker{}, updates: []model.EventMarker{}}
	kept := make([]model.EventMarker, 0, len(existing))
	for _, event := range existing {
		if event.Reviewed || !event.AlgorithmBacked {
			kept = append(kept, event)
		}
	}
	type candidatePair struct {
		kept, detected int
		delta          float64
	}
	pairs := make([]candidatePair, 0, len(kept))
	for keptIndex, event := range kept {
		for detectedIndex, incoming := range detected {
			delta := math.Abs(event.AlgorithmDistanceM - incoming.AlgorithmDistanceM)
			if delta <= toleranceM {
				pairs = append(pairs, candidatePair{keptIndex, detectedIndex, delta})
			}
		}
	}
	sort.SliceStable(pairs, func(i, j int) bool {
		if pairs[i].delta == pairs[j].delta {
			if pairs[i].kept == pairs[j].kept {
				return pairs[i].detected < pairs[j].detected
			}
			return pairs[i].kept < pairs[j].kept
		}
		return pairs[i].delta < pairs[j].delta
	})
	matchOf := make([]int, len(kept))
	for i := range matchOf {
		matchOf[i] = -1
	}
	matchedDetected := make([]bool, len(detected))
	for _, pair := range pairs {
		if matchOf[pair.kept] >= 0 || matchedDetected[pair.detected] {
			continue
		}
		matchOf[pair.kept] = pair.detected
		matchedDetected[pair.detected] = true
	}
	for i, event := range kept {
		if matchOf[i] < 0 {
			event.Reviewed = false
			event.AlgorithmBacked = false
			plan.updates = append(plan.updates, event)
			plan.orphaned++
			continue
		}
		incoming := detected[matchOf[i]]
		event.AlgorithmEventType = incoming.AlgorithmEventType
		event.AlgorithmDistanceM = incoming.AlgorithmDistanceM
		event.AlgorithmInsertionLossDB = incoming.AlgorithmInsertionLossDB
		event.InsertionLossDB = incoming.InsertionLossDB
		event.ReflectanceDB = incoming.ReflectanceDB
		event.Confidence = incoming.Confidence
		event.Reviewed = true
		event.AlgorithmBacked = true
		plan.updates = append(plan.updates, event)
		plan.synced++
	}
	for i, incoming := range detected {
		if !matchedDetected[i] {
			plan.inserts = append(plan.inserts, incoming)
		}
	}
	sort.SliceStable(plan.updates, func(i, j int) bool { return plan.updates[i].ID < plan.updates[j].ID })
	return plan
}
