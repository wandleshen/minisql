#ifndef MINISQL_TABLE_ITERATOR_H
#define MINISQL_TABLE_ITERATOR_H

#include "common/rowid.h"
#include "record/row.h"
#include "transaction/transaction.h"
#include "buffer/buffer_pool_manager.h"

class TableHeap;

class TableIterator {

public:
  // you may define your own constructor based on your member variables
  explicit TableIterator();
  
  explicit TableIterator(const Row* r, TableHeap *bf);
  
  explicit TableIterator(const TableIterator &other);

  TableIterator(const Row *r, const TableHeap *th);
  virtual ~TableIterator();

  inline bool operator==(const TableIterator &itr) const;

  inline bool operator!=(const TableIterator &itr) const;

  const Row &operator*();

  Row *operator->();

  TableIterator &operator++();

  TableIterator operator++(int);

private:
  // add your own private member variables here
  Row* ptr;   //指向当前这一行
  TableHeap* table_heap_;  //指向当前的table_heap_
};

#endif //MINISQL_TABLE_ITERATOR_H
