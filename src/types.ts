import { Timestamp, ObjectID } from 'mongodb'

export type MongoDoc = {
  _id: ObjectID
  [key: string]: any
}

export type OplogInsert = {
  op: 'i'
  o: {
    _id: ObjectID
    [key: string]: any
  }
}

export type OplogUpdate = {
  op: 'u'
  o: {
    $set?: any
    $unset?: any
    [key: string]: any
  }
  o2: {
    _id: ObjectID
  }
}

export type OplogDelete = {
  op: 'd'
  o: {
    _id: ObjectID
  }
}

export type OpLog = {
  ts: Timestamp
  ns: string
  fromMigrate?: boolean
} & (OplogInsert | OplogUpdate | OplogDelete)

export type IRUpsert = {
  action: 'upsert'
  id: string
  parent?: string
  data: {
    [key: string]: any
  }
  timestamp: number
}

export type IRDelete = {
  action: 'delete'
  id: string
  parent?: string
  timestamp: number
}

export type IR = IRUpsert | IRDelete
