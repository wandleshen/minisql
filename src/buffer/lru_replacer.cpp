#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) {

}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  lock_guard<mutex> lock_guard(lock_);
  if (Size() == 0)
    return false;
  *frame_id = lru_list_.back();
  lru_list_.pop_back();
  list_map_.erase(*frame_id);
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  lock_guard<mutex> lock_guard(lock_);
  if (list_map_.find(frame_id) != list_map_.end()) {
    lru_list_.erase(list_map_[frame_id]);
    list_map_.erase(frame_id);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  lock_guard<mutex> lock_guard(lock_);
  if (list_map_.find(frame_id) == list_map_.end()) {
    lru_list_.push_front(frame_id);
    list_map_[frame_id] = lru_list_.begin();
  }
}

size_t LRUReplacer::Size() {
  return lru_list_.size();
}