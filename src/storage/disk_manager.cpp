#include <stdexcept>
#include <sys/stat.h>

#include "glog/logging.h"
#include "page/bitmap_page.h"
#include "storage/disk_manager.h"

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

static constexpr size_t N = DiskManager::BITMAP_SIZE;

page_id_t DiskManager::AllocatePage() {
  for (size_t i = 1; i < MAX_VALID_PAGE_ID - N; i+=N+1) {
    ReadPhysicalPage(i, meta_data_);
    BitmapPage<PAGE_SIZE>* bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE>*>(meta_data_);
    uint32_t offset;
    if (bitmap->AllocatePage(offset)) {
      WritePhysicalPage(i, meta_data_);
      bool isNew = bitmap->get_page_allocated() == 1;
      ReadPhysicalPage(0, meta_data_);
      DiskFileMetaPage *meta_page = reinterpret_cast<DiskFileMetaPage *>(meta_data_);
      meta_page->num_allocated_pages_++;
      meta_page->extent_used_page_[i / N]++;
      if (isNew)
        meta_page->num_extents_++;
      WritePhysicalPage(0, meta_data_);
      return i / (N + 1) * N + offset;
    }
  }
  return INVALID_PAGE_ID;
}

void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  if (IsPageFree(logical_page_id) || MapPageId(logical_page_id) > MAX_VALID_PAGE_ID)
    return;
  for (size_t i = 0; i < PAGE_SIZE; i++) {
    meta_data_[i] = 0;
  }
  WritePage(logical_page_id, meta_data_);
  // update bitmap
  page_id_t bitmap_page_id = logical_page_id / N * (N+1) + 1;
  ReadPhysicalPage(bitmap_page_id, meta_data_);
  BitmapPage<PAGE_SIZE>* bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE>*>(meta_data_);
  bitmap->DeAllocatePage(MapPageId(logical_page_id)-bitmap_page_id-1);
  WritePhysicalPage(bitmap_page_id, meta_data_);
  bool isNew = bitmap->get_page_allocated() == 0;
  ReadPhysicalPage(0, meta_data_);
  DiskFileMetaPage* meta_page = reinterpret_cast<DiskFileMetaPage*>(meta_data_);
  meta_page->num_allocated_pages_--;
  meta_page->extent_used_page_[bitmap_page_id/N]--;
  if (isNew)
    meta_page->num_extents_--;
  WritePhysicalPage(0, meta_data_);
}

bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  page_id_t bitmap_page_id = logical_page_id / N * (N+1) + 1;
  ReadPhysicalPage(bitmap_page_id, meta_data_);
  BitmapPage<PAGE_SIZE>* bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE>*>(meta_data_);
  return bitmap->IsPageFree(MapPageId(logical_page_id)-bitmap_page_id-1);
}

page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  return (logical_page_id / N * (1+N) + logical_page_id % N + 1) + 1;
}

int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}