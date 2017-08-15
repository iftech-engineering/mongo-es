import 'source-map-support/register'

import { ObjectID, Timestamp } from 'mongodb'
import { OpLog, Document, IntermediateRepresentation } from '../src/types'
import { Controls, Task } from '../src/config'
import Processor from '../src/processor'
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

const task: Task = new Task({
  from: {
    phase: 'scan',
  },
  extract: {},
  transform: {
    mapping: {
      "field0.field1": "field0.field1",
      "field0.field2": "field0.field2"
    }
  },
  load: {},
})

const task2: Task = new Task({
  from: {
    phase: 'scan',
  },
  extract: {},
  transform: {
    mapping: {
      "field3": "field3"
    }
  },
  load: {},
})

const doc: Document = {
  _id: new ObjectID("aaaaaaaaaaaaaaaaaaaaaaaa"),
  field0: {
    field1: 1,
    field2: 2,
  },
}

test('transformer create', t => {
  const processor = new Processor(task, new Controls({}))
  t.deepEqual(processor.transformer('create', doc), <IntermediateRepresentation>{
    action: 'create',
    id: 'aaaaaaaaaaaaaaaaaaaaaaaa',
    data: {
      field0: {
        field1: 1,
        field2: 2,
      },
    },
    parent: undefined,
  })
})

test('transformer update', t => {
  const processor = new Processor(task, new Controls({}))
  t.deepEqual(processor.transformer('update', doc), <IntermediateRepresentation>{
    action: 'update',
    id: 'aaaaaaaaaaaaaaaaaaaaaaaa',
    data: {
      field0: {
        field1: 1,
        field2: 2,
      },
    },
    parent: undefined,
  })
})

test('transformer delete', t => {
  const processor = new Processor(task, new Controls({}))
  t.deepEqual(processor.transformer('delete', doc), <IntermediateRepresentation>{
    action: 'delete',
    id: 'aaaaaaaaaaaaaaaaaaaaaaaa',
    data: {},
    parent: undefined,
  })
})

test('applyUpdate', t => {
  const transform = new Processor(task, new Controls({}))
  t.deepEqual(transform.applyUpdate(doc, oplog.o.$set, oplog.o.$unset), {
    _id: new ObjectID("aaaaaaaaaaaaaaaaaaaaaaaa"),
    field0: {
      field1: 'set nested field',
    },
  })
})

test('ignoreUpdate true', t => {
  const processor = new Processor(task2, new Controls({}))
  t.is(processor.ignoreUpdate(oplog), true)
})

test('ignoreUpdate false', t => {
  const processor = new Processor(task, new Controls({}))
  t.is(processor.ignoreUpdate(oplog), false)
})
