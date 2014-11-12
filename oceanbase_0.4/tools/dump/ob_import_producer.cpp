#include "ob_import_producer.h"

#include "ob_import.h"
#include "tokenizer.h"
#include "slice.h"

using namespace oceanbase::common;

int ImportProducer::init()
{
  return OB_SUCCESS;
}

int ImportProducer::produce(RecordBlock &obj)
{
  int ret = ComsumerQueue<RecordBlock>::QUEUE_SUCCESS;

  if (!reader_.eof()) {
    if (reader_.get_records(obj, rec_delima_, delima_, INT64_MAX) != 0) {
      TBSYS_LOG(ERROR, "can't get record");
      ret = ComsumerQueue<RecordBlock>::QUEUE_ERROR;
    }
  } else if ( reader_.get_buffer_size() > 0 ) {
    TBSYS_LOG(ERROR, "buffer is not empty, maybe last line has no rec_delima, please check");
    ret = ComsumerQueue<RecordBlock>::QUEUE_QUITING;
  }
  else {
    ret = ComsumerQueue<RecordBlock>::QUEUE_QUITING;
  }

  return ret;
}
