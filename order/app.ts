import { EVENTS, TOPICS } from './kafka'
import express from 'express'
import { initConsumer } from './kafka/consumer'
import { produceEvent } from './kafka/producer'

const app = express()

app.use(express.json())

app.post('/order', async (req, res) => {
  const { userId, productId, quantity } = req.body

  await produceEvent(TOPICS.PAYMENT_SERVICE, {
    type: EVENTS.ORDER_CREATED,
    payload: { orderId: Math.floor(Math.random() * 1000) },
  })

  res.json({ userId, productId, quantity })
})

app.listen(3000, async () => {
  console.log('Listening on port 3000')

  initConsumer().catch((e: unknown) => console.log(e))
})
