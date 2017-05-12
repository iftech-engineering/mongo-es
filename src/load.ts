import { forEach } from 'lodash'
import { IntermediateRepresentation, LoadTask } from './types'
import { elasticsearch } from './models'
import { IndicesCreateParams, IndicesPutMappingParams } from 'elasticsearch'

export async function create(params: IndicesCreateParams): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    elasticsearch().indices.create(params, (err, response) => {
      err ? reject(err) : resolve(response)
    })
  })
}

export async function putMapping(params: IndicesPutMappingParams): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    elasticsearch().indices.putMapping(params, (err, response) => {
      err ? reject(err) : resolve(response)
    })
  })
}

export async function exists(name: string): Promise<boolean> {
  return new Promise<boolean>((resolve, reject) => {
    elasticsearch().indices.exists({
      index: name
    }, (err, response) => {
      err ? reject(err) : resolve(response)
    })
  })
}

export async function bulk(task: LoadTask, IRs: IntermediateRepresentation[]): Promise<void> {
  const body: any[] = []
  forEach(IRs, (IR) => {
    switch (IR.action) {
      case 'create':
      case 'update': {
        body.push({
          index: {
            _index: task.index,
            _type: task.type,
            _id: IR.id,
            _parent: IR.parent,
          },
        })
        body.push(IR.data)
        break
      }
      case 'delete': {
        body.push({
          delete: {
            _index: task.index,
            _type: task.type,
            _id: IR.id,
            _parent: IR.parent,
          }
        })
        break
      }
    }
  })
  return new Promise<void>((resolve, reject) => {
    elasticsearch().bulk({
      body,
    }, (err, response) => {
      err ? reject(err) : resolve(response)
    })
  })
}
