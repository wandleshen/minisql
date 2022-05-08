#include "common/macros.h"
#include "storage/table_iterator.h"
#include "storage/table_heap.h"

TableIterator::TableIterator() {
  Row *ptr = nullptr;   //指向row的指针
  BufferPoolManager *buffer_pool_manager_ = nullptr; //buffer-pool-manager指针
}
TableIterator::TableIterator(const Row* r, const BufferPoolManager * bf) 
{     
  if (r) ptr = new Row(*r); //非空则拷贝
  if (bf) buffer_pool_manager_ = new BufferPoolManager(*bf);    //非空则拷贝
}
TableIterator::TableIterator(const TableIterator &other) {
  if (other.ptr) ptr = new Row(*other.ptr); 
  if (other.buffer_pool_manager_) buffer_pool_manager_ = new BufferPoolManager(*other.buffer_pool_manager_);
}

TableIterator::~TableIterator() {
  if(ptr) delete ptr;   //非空则进行释放
  if(buffer_pool_manager_) delete buffer_pool_manager_;  //非空则进行释放
  ptr = nullptr;
  buffer_pool_manager_ = nullptr;
}

bool TableIterator::operator==(const TableIterator &itr) const {
  //row指针都为空或者指向同一行
  return ((*ptr == *itr.ptr || !ptr && !itr.ptr ) && *buffer_pool_manager_ == *itr.buffer_pool_manager_);}

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
  TablePage page = *(reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id)));
  //新建两个RowId指针
  RowId *next = new RowId; //使用默认构造函数
  RowId *now = new RowId;
  *now = ptr->GetRowId(); //得到该行的RowId

  //return fulse 也就是说找不到下一个，此时指向最后一个元素的后一个元素
  if (!page.GetNextTupleRid(*now, next)) 
  { 
    //找下一页
    page = *(reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page.GetNextPageId())));
    
    while (!page.GetFirstTupleRid(next)) /*找不到该页的首个元组*/
    {
      if (page.GetNextPageId() == INVALID_PAGE_ID) /*已经是最后一页了*/
      {
        ptr = nullptr; /*说明到了末尾， 置为null*/
        delete next;
        delete now;
        return *this;
      }
    }
  }
  //找到了下一个rowid ：*next
  *ptr = Row(RowId(*next));/*拷贝该行*/
  delete next;
  delete now;
  return *this;}

TableIterator TableIterator::operator++(int) {
  TableIterator old(*this);
  ++(*this);
  return old;}
