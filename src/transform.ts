import { forEach, size, get, set, has, keys } from 'lodash'
import { Task, Document, OpLog, IntermediateRepresentation, ObjectId } from './types'
import { mongo, elasticsearch } from './models'

function transformer(action: 'create' | 'update' | 'delete', task: Task, doc: Document): IntermediateRepresentation | null {
  const IR: IntermediateRepresentation = {
    action,
    id: doc._id.toHexString(),
    data: {},
    parent: get<string>(doc, task.transform.parent)
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

function ignoreUpdate(task: Task, oplog: OpLog): boolean {
  let ignore = true
  if (oplog.op === 'u') {
    forEach(task.transform.mapping, (value, key) => {
      ignore = ignore && !(has(oplog.o, key) || has(oplog.o.$set, key) || has(oplog.o.$unset, key))
    })
  }
  return ignore
}

async function retrieveFromMongo(task: Task, id: ObjectId): Promise<Document | null> {
  try {
    const doc = await mongo()[task.extract.db].collection(task.extract.collection).findOne({
      _id: id,
    })
    console.debug('retrieveFromMongo', doc)
    return doc
  } catch (err) {
    console.warn('retrieveFromMongo', id, err.message)
    return null
  }
}

async function searchFromElasticsearch(task: Task, id: ObjectId): Promise<Document | null> {
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
    }, (err, response) => {
      if (err) {
        console.warn('searchFromElasticsearch', id, err.message)
        resolve(null)
        return
      }
      console.debug('searchFromElasticsearch', response)
      resolve(response.hits.total > 0 ? {
        ...response.hits.hits[0]._source,
        _id: ObjectId.createFromHexString(response.hits.hits[0]._id)
      } : null)
    })
  })
}

async function retrieveFromElasticsearch(task: Task, id: ObjectId): Promise<Document | null> {
  return new Promise<Document | null>((resolve, reject) => {
    elasticsearch().get<Document>({
      index: task.load.index as string,
      type: task.load.type,
      id: id.toHexString(),
    }, (err, response) => {
      if (err) {
        console.warn('retrieveFromElasticsearch', id, err.message)
        resolve(null)
        return
      }
      console.debug('retrieveFromElasticsearch', response)
      resolve(response ? {
        ...response._source,
        _id: ObjectId.createFromHexString(response._id)
      } : null)
    })
  })
}

export function document(task: Task, doc: Document): IntermediateRepresentation | null {
  return transformer('create', task, doc)
}

export async function oplog(task: Task, oplog: OpLog): Promise<IntermediateRepresentation | null> {
  try {
    switch (oplog.op) {
      case 'i': {
        return transformer('create', task, oplog.o)
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
          return transformer('update', task, {
            _id: oplog.o2._id,
            ...oplog.o,
          })
        }
        const doc = (task.transform.parent
          ? await searchFromElasticsearch(task, oplog.o2._id)
          : await retrieveFromElasticsearch(task, oplog.o2._id)
        ) || await retrieveFromMongo(task, oplog.o2._id)
        return doc ? transformer('update', task, doc) : null
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
        return doc ? transformer('delete', task, doc) : null
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
