import test from 'ava'
import { ObjectID, Timestamp } from 'mongodb'

import { OpLog, Document, IR } from '../src/types'
import { Controls, Task } from '../src/config'
import Processor from '../src/processor'

const oplog: OpLog = {
  "ts": new Timestamp(14, 1495012567),
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
  const processor = new Processor(task, new Controls({}), null as any, null as any)
  t.deepEqual(processor.transformer('upsert', doc), <IR>{
    action: 'upsert',
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
  const processor = new Processor(task, new Controls({}), null as any, null as any)
  t.deepEqual(processor.transformer('upsert', doc), <IR>{
    action: 'upsert',
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
  const processor = new Processor(task, new Controls({}), null as any, null as any)
  t.deepEqual(processor.transformer('delete', doc), <IR>{
    action: 'delete',
    id: 'aaaaaaaaaaaaaaaaaaaaaaaa',
    parent: undefined,
  })
})

test('applyUpdate', t => {
  const transform = new Processor(task, new Controls({}), null as any, null as any)
  t.deepEqual(transform.applyUpdate(doc, oplog.o.$set, oplog.o.$unset), {
    _id: new ObjectID("aaaaaaaaaaaaaaaaaaaaaaaa"),
    field0: {
      field1: 'set nested field',
    },
  })
})

test('ignoreUpdate true', t => {
  const processor = new Processor(task2, new Controls({}), null as any, null as any)
  t.is(processor.ignoreUpdate(oplog), true)
})

test('ignoreUpdate false', t => {
  const processor = new Processor(task, new Controls({}), null as any, null as any)
  t.is(processor.ignoreUpdate(oplog), false)
})

test('mergeOplogs insert then update', t => {
  const processor = new Processor({
    transform: {
      mapping: {
        "field0": "field0",
        "field1": "field1",
      },
    },
  } as any, new Controls({}), null as any, null as any)
  const oplogs = processor.mergeOplogs([{
    ts: new Timestamp(0, 0),
    op: 'i',
    ns: 'example1',
    o: {
      _id: new ObjectID("aaaaaaaaaaaaaaaaaaaaaaaa"),
      field0: 0,
    },
  }, {
    ts: new Timestamp(0, 1),
    op: 'u',
    ns: 'example1',
    o: {
      $set: {
        field1: 1,
      },
      $unset: {
        field0: 1,
      },
    },
    o2: {
      _id: new ObjectID("aaaaaaaaaaaaaaaaaaaaaaaa"),
    },
  }])
  t.deepEqual(oplogs, [{
    ts: new Timestamp(0, 1),
    op: 'i',
    ns: 'example1',
    o: {
      _id: new ObjectID("aaaaaaaaaaaaaaaaaaaaaaaa"),
      field1: 1,
    },
  }])
})

test('mergeOplogs update then update', t => {
  const processor = new Processor({
    transform: {
      mapping: {
        "field0": "field0",
        "field1": "field1",
      },
    },
  } as any, new Controls({}), null as any, null as any)
  const oplogs = processor.mergeOplogs([{
    ts: new Timestamp(0, 1),
    op: 'u',
    ns: 'example1',
    o: {
      field0: 1,
      $set: {
        field1: 1,
      },
    },
    o2: {
      _id: new ObjectID("aaaaaaaaaaaaaaaaaaaaaaaa"),
    },
  }, {
    ts: new Timestamp(0, 0),
    op: 'u',
    ns: 'example1',
    o: {
      $set: {
        field1: 2,
        field0: 3,
      },
    },
    o2: {
      _id: new ObjectID("aaaaaaaaaaaaaaaaaaaaaaaa"),
    },
  }])
  t.deepEqual(oplogs, [{
    ts: new Timestamp(0, 1),
    op: 'u',
    ns: 'example1',
    o: {
      field0: 1,
      $set: {
        field1: 1,
        field0: 3,
      },
    },
    o2: {
      _id: new ObjectID("aaaaaaaaaaaaaaaaaaaaaaaa"),
    },
  }])
})
