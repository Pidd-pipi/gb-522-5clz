package repository

import (
	"errors"
	"fmt"

	"fiber-otdr-fault-localization/backend/internal/dto"
	"fiber-otdr-fault-localization/backend/internal/model"
	"gorm.io/gorm"
)

type EventRepository struct{ db *gorm.DB }

func (r *EventRepository) DeleteUnreviewedForTrace(traceID uint) error {
	if err := r.db.Where("trace_id = ? AND reviewed = ?", traceID, false).Delete(&model.EventMarker{}).Error; err != nil {
		return fmt.Errorf("clear unreviewed events: %w", err)
	}
	return nil
}

func (r *EventRepository) CreateBatch(events []model.EventMarker) error {
	if len(events) == 0 {
		return nil
	}
	if err := r.db.Create(&events).Error; err != nil {
		return fmt.Errorf("create detected events: %w", err)
	}
	return nil
}

func (r *EventRepository) SyncAlgorithmValues(event *model.EventMarker) error {
	result := r.db.Model(&model.EventMarker{}).Where("id = ?", event.ID).Updates(map[string]any{"algorithm_event_type": event.AlgorithmEventType, "algorithm_distance_m": event.AlgorithmDistanceM, "algorithm_insertion_loss_db": event.AlgorithmInsertionLossDB, "insertion_loss_db": event.InsertionLossDB, "reflectance_db": event.ReflectanceDB, "confidence": event.Confidence, "pending_re_review": false})
	if result.Error != nil {
		return fmt.Errorf("sync algorithm values: %w", result.Error)
	}
	if result.RowsAffected == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *EventRepository) MarkPendingReReview(ids []uint) error {
	if len(ids) == 0 {
		return nil
	}
	if err := r.db.Model(&model.EventMarker{}).Where("id IN ?", ids).Update("pending_re_review", true).Error; err != nil {
		return fmt.Errorf("mark events pending re-review: %w", err)
	}
	return nil
}

func (r *EventRepository) Get(id uint) (model.EventMarker, error) {
	var event model.EventMarker
	if err := r.db.First(&event, id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return event, ErrNotFound
		}
		return event, fmt.Errorf("get event marker: %w", err)
	}
	return event, nil
}

func (r *EventRepository) List(query dto.EventQuery) ([]model.EventMarker, int64, error) {
	db := r.db.Model(&model.EventMarker{})
	if query.TraceID != nil {
		db = db.Where("trace_id = ?", *query.TraceID)
	}
	if query.RouteID != nil {
		db = db.Where("trace_id IN (?)", r.db.Model(&model.TraceCapture{}).Select("id").Where("route_id = ?", *query.RouteID))
	}
	if query.Type.Valid() {
		db = db.Where("event_type = ?", query.Type)
	}
	if query.Reviewed != nil {
		db = db.Where("reviewed = ?", *query.Reviewed)
	}
	if query.PendingReReview != nil {
		db = db.Where("pending_re_review = ?", *query.PendingReReview)
	}
	var total int64
	if err := db.Count(&total).Error; err != nil {
		return nil, 0, fmt.Errorf("count events: %w", err)
	}
	var events []model.EventMarker
	if err := db.Order("trace_id DESC, distance_m ASC").Offset((query.Page - 1) * query.PageSize).Limit(query.PageSize).Find(&events).Error; err != nil {
		return nil, 0, fmt.Errorf("list events: %w", err)
	}
	return events, total, nil
}

func (r *EventRepository) ForTrace(traceID uint) ([]model.EventMarker, error) {
	var events []model.EventMarker
	if err := r.db.Where("trace_id = ?", traceID).Order("distance_m ASC").Find(&events).Error; err != nil {
		return nil, fmt.Errorf("list events for trace: %w", err)
	}
	return events, nil
}

func (r *EventRepository) Review(event *model.EventMarker) error {
	result := r.db.Model(&model.EventMarker{}).Where("id = ?", event.ID).Updates(map[string]any{"event_type": event.EventType, "distance_m": event.DistanceM, "reviewed": true, "pending_re_review": false, "review_note": event.ReviewNote, "reviewed_by": event.ReviewedBy, "reviewed_at": event.ReviewedAt})
	if result.Error != nil {
		return fmt.Errorf("review event marker: %w", result.Error)
	}
	if result.RowsAffected == 0 {
		return ErrNotFound
	}
	return nil
}
