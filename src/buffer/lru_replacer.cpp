#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) {

}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) { 	//替换（即删除）least recently used的页
  lock_guard<mutex> lock_guard(lock_);
  if (Size() == 0)
    return false;	//如果当前没有可以替换的元素则返回false；
  *frame_id = lru_list_.back();//将替换页的页帧号存储在输出参数frame_id中输出并返回true
  lru_list_.pop_back();
  list_map_.erase(*frame_id);
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {	//将数据页固定使之不能被Replacer替换
  lock_guard<mutex> lock_guard(lock_);
  if (list_map_.find(frame_id) != list_map_.end()) {
    lru_list_.erase(list_map_[frame_id]);
    list_map_.erase(frame_id);	//从lru_list_中移除该数据页对应的页帧
  }
}
    
void LRUReplacer::Unpin(frame_id_t frame_id) {	//将数据页解除固定
  lock_guard<mutex> lock_guard(lock_);
  if (list_map_.find(frame_id) == list_map_.end()) {
    lru_list_.push_front(frame_id);	//将解除固定的数据页放入lru_list_中
    list_map_[frame_id] = lru_list_.begin();
  }
}

size_t LRUReplacer::Size() {	//返回当前LRUReplacer中能够被替换的数据页的数量
  return lru_list_.size();
}
