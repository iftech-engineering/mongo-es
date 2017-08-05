import 'source-map-support/register'

import { ObjectID, Timestamp } from 'mongodb'
import { OpLog, TransformTask, Document, IntermediateRepresentation } from '../src/types'
import { transformer, applyUpdate, ignoreUpdate } from '../src/transform'
import test from 'ava'

const oplog: OpLog = {
  "ts": new Timestamp(14, 1495012567),
  "t": 21,
  "h": 6069675571563967122,
  "v": 2,
  "op": "u",
  "ns": "db0.collection0",
  "o2": {
    "_id": new ObjectID("aaaaaaaaaaaaaaaaaaaaaaaa")
  },
  "o": {
    "$set": {
      "field0.field1": "set nested field"
    },
    "$unset": {
      "field0.field2": 1
    }
  }
}

const task: TransformTask = {
  mapping: {
    "field0.field1": "field0.field1",
    "field0.field2": "field0.field2"
  }
}

const task2: TransformTask = {
  mapping: {
    "field3": "field3"
  }
}

const doc: Document = {
  _id: new ObjectID("aaaaaaaaaaaaaaaaaaaaaaaa"),
  field0: {
    field1: 1,
    field2: 2
  }
}

test('transformer create', t => {
  t.deepEqual(transformer(task, 'create', doc), <IntermediateRepresentation>{
    action: 'create',
    id: 'aaaaaaaaaaaaaaaaaaaaaaaa',
    data: {
      field0: {
        field1: 1,
        field2: 2
      }
    },
    parent: undefined,
  })
})

test('transformer update', t => {
  t.deepEqual(transformer(task, 'update', doc), <IntermediateRepresentation>{
    action: 'update',
    id: 'aaaaaaaaaaaaaaaaaaaaaaaa',
    data: {
      field0: {
        field1: 1,
        field2: 2
      }
    },
    parent: undefined,
  })
})

test('transformer delete', t => {
  t.deepEqual(transformer(task, 'delete', doc), <IntermediateRepresentation>{
    action: 'delete',
    id: 'aaaaaaaaaaaaaaaaaaaaaaaa',
    data: {},
    parent: undefined,
  })
})

test('applyUpdate', t => {
  t.deepEqual(applyUpdate(task, doc, oplog.o.$set, oplog.o.$unset), {
    _id: new ObjectID("aaaaaaaaaaaaaaaaaaaaaaaa"),
    field0: {
      field1: 'set nested field',
    }
  })
})

test('ignoreUpdate true', t => {
  t.is(ignoreUpdate(task2, oplog), true)
})

test('ignoreUpdate false', t => {
  t.is(ignoreUpdate(task, oplog), false)
})
