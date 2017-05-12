import { forEach } from 'lodash'
import { IntermediateRepresentation, LoadTask } from './types'
import { elasticsearch } from './models'

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
