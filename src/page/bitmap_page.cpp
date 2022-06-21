#include "page/bitmap_page.h"

template<size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  if (page_allocated_ >= GetMaxSupportedSize())		//超过最大范围则无法分配空闲页，返回false
    return false;
  page_allocated_++;
  page_offset = next_free_page_;	//偏移量移到下一个空闲页上
  bytes[page_offset / 8] |= (1 << (page_offset % 8));
  for (size_t i = 0; i < GetMaxSupportedSize(); i++)	//从中找出第一个空闲页
    if (IsPageFree(i)) {
      next_free_page_ = i;
      break;
    }
  return true;
}

template<size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  if (IsPageFree(page_offset))	//若该页本身就是空闲页则无法回收
    return false;
  page_allocated_--;	//已分配页数减一
  bytes[page_offset / 8] &= ~(1 << (page_offset % 8));
  next_free_page_ = page_offset;
  return true;
}

template<size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  uint32_t byte_index = page_offset / 8;
  uint8_t bit_index = page_offset % 8;
  return IsPageFreeLow(byte_index, bit_index);	//通过下面的函数判断
}

template<size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  return ~bytes[byte_index] & (1 << bit_index);
}

template
class BitmapPage<64>;

template
class BitmapPage<128>;

template
class BitmapPage<256>;

template
class BitmapPage<512>;

template
class BitmapPage<1024>;

template
class BitmapPage<2048>;

template
class BitmapPage<4096>;
