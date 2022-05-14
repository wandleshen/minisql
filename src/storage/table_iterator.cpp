#include "common/macros.h"
#include "storage/table_iterator.h"
#include "storage/table_heap.h"

TableIterator::TableIterator() {
  ptr = nullptr;   //指向row的指针
  table_heap_ = nullptr; //table_heap_指针
}
TableIterator::TableIterator(const Row* r, const TableHeap* th)
{     
  if (r) ptr = new Row(*r); //非空则拷贝
  else ptr = nullptr;
  if (th) table_heap_ = (TableHeap*)th;    //非空则拷贝 update: BufferPoolManager 没有拷贝构造函数
}
TableIterator::TableIterator(const TableIterator &other) {
  if (other.ptr) ptr = new Row(*other.ptr); 
  if (other.table_heap_) table_heap_ = (TableHeap*)other.table_heap_;
}

TableIterator::~TableIterator() {
  //delete ptr;   //非空则进行释放 update: delete nullptr 仍然是 valid 的
  ptr = nullptr;
  table_heap_ = nullptr;
}

bool TableIterator::operator==(const TableIterator &itr) const {
  //row指针都为空或者指向同一行
  return ((ptr == itr.ptr || (!ptr && !itr.ptr)) && table_heap_->buffer_pool_manager_ == itr.table_heap_->buffer_pool_manager_);}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return !(*this == itr);
}

const Row &TableIterator::operator*() {
  return *ptr;
}

Row *TableIterator::operator->() {
  return ptr; 
}

TableIterator &TableIterator::operator++() {
  //获取这一页的id
  page_id_t page_id = ptr->GetRowId().GetPageId();
  //获取该页的内容，需要进行类型转换：page->TablePage
  TablePage* page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(page_id));  // update: page 没有赋值重载
  //新建两个RowId指针
  RowId *next = new RowId; //使用默认构造函数
  RowId *now = new RowId;
  *now = ptr->GetRowId(); //得到该行的RowId

  //return false 也就是说找不到下一个，此时指向最后一个元素的后一个元素
  if (!page->GetNextTupleRid(*now, next))
  {
    if (page->GetNextPageId() == INVALID_PAGE_ID) /*已经是最后一页了*/
    {
      ptr = nullptr; /*说明到了末尾， 置为null*/
      delete next;
      delete now;
      return *this;
    }
    //找下一页
    page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(page->GetNextPageId()));
    
    while (!page->GetFirstTupleRid(next)) /*找不到该页的首个元组*/
    {
      if (page->GetNextPageId() == INVALID_PAGE_ID) /*已经是最后一页了*/
      {
        ptr = nullptr; /*说明到了末尾， 置为null*/
        delete next;
        delete now;
        return *this;
      }
      page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(page->GetNextPageId()));
    }
  }
  //找到了下一个rowid ：*next
  delete ptr;
  ptr = new Row(*next); //更新ptr的rowid
  table_heap_->GetTuple(ptr, nullptr);

  delete next;
  delete now;
  return *this;}

TableIterator TableIterator::operator++(int) {
  TableIterator old(*this);
  ++(*this);
  return TableIterator(old);}  // update: 返回一个拷贝构造对象
