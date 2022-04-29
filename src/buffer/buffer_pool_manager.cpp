#include "buffer/buffer_pool_manager.h"
#include "glog/logging.h"
#include "page/bitmap_page.h"

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
        : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page: page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  lock_guard<recursive_mutex> lock_guard(latch_);
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  frame_id_t frame_id;
  if (page_table_.find(page_id) != page_table_.end()) {
    replacer_->Pin(page_table_[page_id]);
    pages_[page_table_[page_id]].pin_count_++;
    return &pages_[page_table_[page_id]];
  } else {
    if (!free_list_.empty()) {
      frame_id = free_list_.front();
      free_list_.pop_front();
    } else if (replacer_->Size() != 0){
      replacer_->Victim(&frame_id);
    } else {
      return nullptr;
    }
  }
  // 2.     If R is dirty, write it back to the disk.
  auto page = &pages_[frame_id];
  page_id_t r_page_id = page->page_id_;
  if (page->IsDirty())
    FlushPage(r_page_id);
  // 3.     Delete R from the page table and insert P.
  page_table_.erase(r_page_id);
  page_table_[page_id] = frame_id;
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  disk_manager_->ReadPage(page_id, page->data_);
  page->pin_count_ = 1;
  page->page_id_ = page_id;
  page->is_dirty_ = false;
  return page;
}

Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  lock_guard<recursive_mutex> lock_guard(latch_);
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  if (free_list_.empty() && replacer_->Size() == 0)
    return nullptr;
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  frame_id_t frame_id;
  Page* p;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    frame_id = replacer_->Victim(&frame_id);
  }
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  p = &pages_[frame_id];
  p->ResetMemory();
  p->pin_count_ = 1;
  p->is_dirty_ = false;
  // 4.   Set the page ID output parameter. Return a pointer to P.
  page_id = disk_manager_->AllocatePage();
  p->page_id_ = page_id;
  page_table_[page_id] = frame_id;
  return p;
}

bool BufferPoolManager::DeletePage(page_id_t page_id) {
  lock_guard<recursive_mutex> lock_guard(latch_);
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  if (page_table_.find(page_id) == page_table_.end())
    return true;
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  auto page = &pages_[page_table_[page_id]];
  if (page->pin_count_ > 0)
    return false;
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  free_list_.emplace_back(page_table_[page_id]);
  page_table_.erase(page_id);
  page->ResetMemory();
  page->is_dirty_ = false;
  DeallocatePage(page_id);
  return true;
}

bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  lock_guard<recursive_mutex> lock_guard(latch_);
  if (page_table_.find(page_id) == page_table_.end())
    return false;
  Page* p = &pages_[page_table_[page_id]];
  if (is_dirty)
    FlushPage(page_id);
  replacer_->Unpin(page_table_[page_id]);
  p->pin_count_ = 0;
  return true;
}

bool BufferPoolManager::FlushPage(page_id_t page_id) {
  lock_guard<recursive_mutex> lock_guard(latch_);
  if (page_id == INVALID_PAGE_ID)
    return false;
  if (page_table_.find(page_id) != page_table_.end()) {
    auto page = &pages_[page_table_[page_id]];
    disk_manager_->WritePage(page_id, page->data_);
    return true;
  }
  return false;
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}