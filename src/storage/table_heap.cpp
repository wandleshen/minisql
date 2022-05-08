#include "storage/table_heap.h"

bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
  return false;
}

bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

bool TableHeap::UpdateTuple(const Row &row, const RowId &rid, Transaction *txn) {
  return false;
}

void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Delete the tuple from the page.

}

void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback the delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::FreeHeap() {

}

bool TableHeap::GetTuple(Row *row, Transaction *txn) {
  return false;
}

TableIterator TableHeap::Begin(Transaction *txn) {
  //得到第一页
  TablePage first = *(reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_)));
  //建立一个指向行的指针
  RowId* first_id = new RowId;
  //直到遇见1才停
  //bool GetFirstTupleRid(RowId *first_rid);
  while (1) 
  {
    if (first.GetFirstTupleRid(first_id)) break; /*找到了第一个*/
    if (first.GetNextPageId() == INVALID_PAGE_ID) /*已经是最后一页了，并且没有找到第一个元组*/
    {
      delete first_id;  /*释放空间*/
      return TableIterator(nullptr, buffer_pool_manager_);  /*返回null*/
    }
    //更新page
    first = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(first.GetNextPageId()));
  }
  //构建迭代器
  TableIterator ite(first_id, buffer_pool_manager_);
  //释放空间
  delete first_id;
  //返回迭代器
  return ite;}

TableIterator TableHeap::End() {
  //flag: 行指针为空
  return TableIterator(nullptr, buffer_pool_manager_);}
