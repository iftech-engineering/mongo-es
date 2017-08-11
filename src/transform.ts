import { forEach, size, get, set, unset, has, keys } from 'lodash'

import { Document, OpLog, IntermediateRepresentation, ObjectID } from './types'
import { MongoDB, Elasticsearch } from './models'
import { Task } from './models/Config'

export function transformer(task: Task, action: 'create' | 'update' | 'delete', doc: Document): IntermediateRepresentation | null {
  const IR: IntermediateRepresentation = {
    action,
    id: doc._id.toHexString(),
    data: {},
    parent: task.transform.parent && get<string>(doc, task.transform.parent)
  }
  if (action === 'delete') {
    return IR
  }
  forEach(task.transform.mapping, (value, key) => {
    if (has(doc, key)) {
      set(IR.data, value, get(doc, key))
    }
  })
  if (size(IR.data) === 0) {
    return null
  }
  return IR
}

export function applyUpdate(task: Task, doc: Document, $set: any = {}, $unset: any = {}): Document {
  forEach(task.transform.mapping, (value, key) => {
    if (has($unset, key)) {
      unset(doc, value)
    }
    if (has($set, key)) {
      set(doc, value, get($set, key))
    }
  })
  return doc
}

export function ignoreUpdate(task: Task, oplog: OpLog): boolean {
  let ignore = true
  if (oplog.op === 'u') {
    forEach(task.transform.mapping, (value, key) => {
      ignore = ignore && !(has(oplog.o, key) || has(oplog.o.$set, key) || has(oplog.o.$unset, key))
    })
  }
  return ignore
}

async function retrieveFromMongoDB(task: Task, id: ObjectID): Promise<Document | null> {
  try {
    const doc = await MongoDB.getCollection(task.extract.db, task.extract.collection).findOne({
      _id: id,
    })
    console.debug('retrieve from mongodb', doc)
    return doc
  } catch (err) {
    console.warn('retrieve from mongodb', task.name(), id, err.message)
    return null
  }
}

export function document(task: Task, doc: Document): IntermediateRepresentation | null {
  return transformer(task, 'create', doc)
}

export async function oplog(task: Task, oplog: OpLog): Promise<IntermediateRepresentation | null> {
  try {
    switch (oplog.op) {
      case 'i': {
        return transformer(task, 'create', oplog.o)
      }
      case 'u': {
        if (size(oplog.o2) !== 1 || !oplog.o2._id) {
          console.warn('oplog', 'cannot transform', oplog)
          return null
        }
        if (ignoreUpdate(task, oplog)) {
          console.debug('ignoreUpdate', oplog)
          return null
        }
        if (keys(oplog.o).filter(key => key.startsWith('$')).length === 0) {
          return transformer(task, 'update', {
            _id: oplog.o2._id,
            ...oplog.o,
          })
        }
        const old = task.transform.parent
          ? await Elasticsearch.search(task, oplog.o2._id)
          : await Elasticsearch.retrieve(task, oplog.o2._id)
        const doc = old ? applyUpdate(task, old, oplog.o.$set, oplog.o.$unset) : await retrieveFromMongoDB(task, oplog.o2._id)
        return doc ? transformer(task, 'update', doc) : null
      }
      case 'd': {
        if (size(oplog.o) !== 1 || !oplog.o._id) {
          console.warn('oplog', 'cannot transform', oplog)
          return null
        }
        const doc = task.transform.parent
          ? await Elasticsearch.search(task, oplog.o._id)
          : oplog.o
        console.debug(doc)
        return doc ? transformer(task, 'delete', doc) : null
      }
      default: {
        return null
      }
    }
  } catch (err) {
    console.error('oplog', err)
    return null
  }
}
