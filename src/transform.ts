import { forEach, size, get, set, unset, has, keys } from 'lodash'
import { Task, TransformTask, Document, OpLog, IntermediateRepresentation, ObjectID } from './types'
import { mongodb, elasticsearch } from './models'
import { taskName } from './utils'

export function transformer(task: TransformTask, action: 'create' | 'update' | 'delete', doc: Document): IntermediateRepresentation | null {
  const IR: IntermediateRepresentation = {
    action,
    id: doc._id.toHexString(),
    data: {},
    parent: task.parent && get<string>(doc, task.parent)
  }
  if (action === 'delete') {
    return IR
  }
  forEach(task.mapping, (value, key) => {
    if (has(doc, key)) {
      set(IR.data, value, get(doc, key))
    }
  })
  if (size(IR.data) === 0) {
    return null
  }
  return IR
}

export function applyUpdate(task: TransformTask, doc: Document, $set: any = {}, $unset: any = {}): Document {
  forEach(task.mapping, (value, key) => {
    if (has($unset, key)) {
      unset(doc, value)
    }
    if (has($set, key)) {
      set(doc, value, get($set, key))
    }
  })
  return doc
}

export function ignoreUpdate(task: TransformTask, oplog: OpLog): boolean {
  let ignore = true
  if (oplog.op === 'u') {
    forEach(task.mapping, (value, key) => {
      ignore = ignore && !(has(oplog.o, key) || has(oplog.o.$set, key) || has(oplog.o.$unset, key))
    })
  }
  return ignore
}

async function retrieveFromMongoDB(task: Task, id: ObjectID): Promise<Document | null> {
  try {
    const doc = await mongodb()[task.extract.db].collection(task.extract.collection).findOne({
      _id: id,
    })
    console.debug('retrieve from mongodb', doc)
    return doc
  } catch (err) {
    console.warn('retrieve from mongodb', taskName(task), id, err.message)
    return null
  }
}

async function searchFromElasticsearch(task: Task, id: ObjectID): Promise<Document | null> {
  return new Promise<Document | null>((resolve, reject) => {
    elasticsearch().search<Document>({
      index: task.load.index,
      type: task.load.type,
      body: {
        query: {
          term: {
            _id: id.toHexString(),
          },
        },
      },
    }, (err, response: any) => {
      if (err) {
        console.warn('search from elasticsearch', taskName(task), id, err.message)
        resolve(null)
        return
      }
      console.debug('search from elasticsearch', response)
      if (response.hits.total === 0) {
        resolve(null)
        return
      }
      const doc = response.hits.hits[0]._source
      doc._id = new ObjectID(response.hits.hits[0]._id)
      if (task.transform.parent && response.hits.hits[0]._parent) {
        doc[task.transform.parent] = new ObjectID(response.hits.hits[0]._parent)
      }
      resolve(doc)
    })
  })
}

async function retrieveFromElasticsearch(task: Task, id: ObjectID): Promise<Document | null> {
  return new Promise<Document | null>((resolve, reject) => {
    elasticsearch().get<Document>({
      index: task.load.index as string,
      type: task.load.type,
      id: id.toHexString(),
    }, (err, response) => {
      if (err) {
        console.warn('retrieve from elasticsearch', taskName(task), id, err.message)
        resolve(null)
        return
      }
      console.debug('retrieve from elasticsearch', response)
      resolve(response ? {
        ...response._source,
        _id: new ObjectID(response._id)
      } : null)
    })
  })
}

export function document(task: Task, doc: Document): IntermediateRepresentation | null {
  return transformer(task.transform, 'create', doc)
}

export async function oplog(task: Task, oplog: OpLog): Promise<IntermediateRepresentation | null> {
  try {
    switch (oplog.op) {
      case 'i': {
        return transformer(task.transform, 'create', oplog.o)
      }
      case 'u': {
        if (size(oplog.o2) !== 1 || !oplog.o2._id) {
          console.warn('oplog', 'cannot transform', oplog)
          return null
        }
        if (ignoreUpdate(task.transform, oplog)) {
          console.debug('ignoreUpdate', oplog)
          return null
        }
        if (keys(oplog.o).filter(key => key.startsWith('$')).length === 0) {
          return transformer(task.transform, 'update', {
            _id: oplog.o2._id,
            ...oplog.o,
          })
        }
        const old = task.transform.parent
          ? await searchFromElasticsearch(task, oplog.o2._id)
          : await retrieveFromElasticsearch(task, oplog.o2._id)
        const doc = old ? applyUpdate(task.transform, old, oplog.o.$set, oplog.o.$unset) : await retrieveFromMongoDB(task, oplog.o2._id)
        return doc ? transformer(task.transform, 'update', doc) : null
      }
      case 'd': {
        if (size(oplog.o) !== 1 || !oplog.o._id) {
          console.warn('oplog', 'cannot transform', oplog)
          return null
        }
        const doc = task.transform.parent
          ? await retrieveFromElasticsearch(task, oplog.o._id)
          : oplog.o
        console.debug(doc)
        return doc ? transformer(task.transform, 'delete', doc) : null
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
