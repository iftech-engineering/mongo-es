import { forEach } from 'lodash'

import { IntermediateRepresentation } from './types'
import { Elasticsearch } from './models'
import { Task } from './models/Config'

export async function bulk(task: Task, IRs: IntermediateRepresentation[]): Promise<void> {
  if (IRs.length === 0) {
    return
  }
  const body: any[] = []
  forEach(IRs, (IR) => {
    switch (IR.action) {
      case 'create':
      case 'update': {
        body.push({
          index: {
            _index: task.load.index,
            _type: task.load.type,
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
            _index: task.load.index,
            _type: task.load.type,
            _id: IR.id,
            _parent: IR.parent,
          }
        })
        break
      }
    }
  })
  return await Elasticsearch.bulk({ body })
}
