#include "buffer/clock_replacer.h"

ClockReplacer::ClockReplacer(size_t num_pages) {
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
  lock_guard<mutex> lock_guard(lock_);
  if (Size() == 0)
    return false;
  while (true) {
    if (clock_hand_ == (int)clock_vec_.size())
      clock_hand_ = 0;
    if (clock_vec_[clock_hand_].second)
      break;
    clock_vec_[clock_hand_].second = true;
    clock_hand_++;
  }
  *frame_id = clock_vec_[clock_hand_].first;
  clock_vec_.erase(clock_vec_.begin()+clock_hand_);
  list_map_.erase(*frame_id);
  return true;
}

void ClockReplacer::Pin(frame_id_t frame_id) {
  lock_guard<mutex> lock_guard(lock_);
  if (list_map_.find(frame_id) != list_map_.end()) {
    clock_vec_.erase(list_map_[frame_id]);
    list_map_.erase(frame_id);
  }
}
void ClockReplacer::Unpin(frame_id_t frame_id) {
  lock_guard<mutex> lock_guard(lock_);
  if (list_map_.find(frame_id) == list_map_.end()) {
    clock_vec_.emplace_back( frame_id, false );
    list_map_[frame_id] = clock_vec_.begin();
  }
}
size_t ClockReplacer::Size() { return clock_vec_.size(); }
