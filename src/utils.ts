import { Readable } from 'stream'

export function limitStreamReadSpeed(stream: Readable, maxDPS?: number): Readable {
  if (!maxDPS) {
    return stream
  }
  let dataCount = 0
  const timer = setInterval(() => {
    dataCount = 0
    stream.resume()
  }, 1000)
  stream.addListener('data', (doc) => {
    dataCount++
    if (dataCount >= maxDPS) {
      stream.pause()
    }
  })
  stream.addListener('end', () => {
    clearInterval(timer)
  })
  return stream
}
