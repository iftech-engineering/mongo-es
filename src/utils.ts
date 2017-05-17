import { Task } from './types'

export function taskName(task: Task): string {
  return `${task.extract.db}.${task.extract.collection} -> ${task.load.index}.${task.load.type}`
}
