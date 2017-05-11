import { forEach } from 'lodash'
import { TransformTask, Document, OpLog, IntermediateRepresentation } from './types'

export async function document(task: TransformTask, doc: Document): Promise<IntermediateRepresentation> {
  const IR: IntermediateRepresentation = {
    action: 'create',
    id: doc._id,
    data: {}
  }
  forEach(task.mapping, (value, key) => {
    if (value === '_parent') {
      IR.parent = doc[key]
    } else {
      IR.data[value] = doc[key]
    }
  })
  return IR
}

// export function oplog(task: TransformTask) {
//   return async function (oplog: OpLog): Promise<IntermediateRepresentation> {

//   }
// }
