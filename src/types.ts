import { Timestamp, ObjectID } from 'mongodb'

export type Document = {
  _id: ObjectID
  [key: string]: any
}

export type OplogDiff = {
  op: 'i'
  o: {
    _id: ObjectID
    [key: string]: any
  }
} | {
  op: 'u'
  o: {
    $set?: any
    $unset?: any
    [key: string]: any
  }
  o2: {
    _id: ObjectID
  }
} | {
  op: 'd'
  o: {
    _id: ObjectID
  }
}

export type OpLog = {
  ts: Timestamp
  t: number
  h: number
  v: number
  ns: string
  fromMigrate?: boolean
} & OplogDiff

export type IntermediateRepresentation = {
  action: 'create' | 'update' | 'delete'
  id: string
  parent?: string
  data: any
}
