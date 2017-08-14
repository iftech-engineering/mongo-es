import { forEach, size, get, set, unset, has, keys } from 'lodash'

import { Document, OpLog, IntermediateRepresentation } from '../types'
import { MongoDB, Elasticsearch } from './index'
import { Task } from './Config'

export default class Transform {
  private task: Task

  constructor(task: Task) {
    this.task = task
  }

  transformer(action: 'create' | 'update' | 'delete', doc: Document): IntermediateRepresentation | null {
    const IR: IntermediateRepresentation = {
      action,
      id: doc._id.toHexString(),
      data: {},
      parent: this.task.transform.parent && get<string>(doc, this.task.transform.parent)
    }
    if (action === 'delete') {
      return IR
    }
    forEach(this.task.transform.mapping, (value, key) => {
      if (has(doc, key)) {
        set(IR.data, value, get(doc, key))
      }
    })
    if (size(IR.data) === 0) {
      return null
    }
    return IR
  }

  applyUpdate(doc: Document, $set: any = {}, $unset: any = {}): Document {
    forEach(this.task.transform.mapping, (value, key) => {
      if (has($unset, key)) {
        unset(doc, value)
      }
      if (has($set, key)) {
        set(doc, value, get($set, key))
      }
    })
    return doc
  }

  ignoreUpdate(oplog: OpLog): boolean {
    let ignore = true
    if (oplog.op === 'u') {
      forEach(this.task.transform.mapping, (value, key) => {
        ignore = ignore && !(has(oplog.o, key) || has(oplog.o.$set, key) || has(oplog.o.$unset, key))
      })
    }
    return ignore
  }


  public async document(doc: Document): Promise<IntermediateRepresentation | null> {
    return await this.transformer('create', doc)
  }

  public async oplog(oplog: OpLog): Promise<IntermediateRepresentation | null> {
    try {
      switch (oplog.op) {
        case 'i': {
          return this.transformer('create', oplog.o)
        }
        case 'u': {
          if (size(oplog.o2) !== 1 || !oplog.o2._id) {
            console.warn('oplog', 'cannot transform', oplog)
            return null
          }
          if (this.ignoreUpdate(oplog)) {
            console.debug('ignoreUpdate', oplog)
            return null
          }
          if (keys(oplog.o).filter(key => key.startsWith('$')).length === 0) {
            return this.transformer('update', {
              _id: oplog.o2._id,
              ...oplog.o,
            })
          }
          const old = this.task.transform.parent
            ? await Elasticsearch.search(this.task, oplog.o2._id)
            : await Elasticsearch.retrieve(this.task, oplog.o2._id)
          const doc = old ? this.applyUpdate(old, oplog.o.$set, oplog.o.$unset) : await MongoDB.retrieve(this.task, oplog.o2._id)
          return doc ? this.transformer('update', doc) : null
        }
        case 'd': {
          if (size(oplog.o) !== 1 || !oplog.o._id) {
            console.warn('oplog', 'cannot transform', oplog)
            return null
          }
          const doc = this.task.transform.parent
            ? await Elasticsearch.search(this.task, oplog.o._id)
            : oplog.o
          console.debug(doc)
          return doc ? this.transformer('delete', doc) : null
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
}
