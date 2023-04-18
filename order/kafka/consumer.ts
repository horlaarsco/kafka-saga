import { Kafka } from 'kafkajs'
import { EVENTS, TOPICS } from '.'

const kafka = new Kafka({
  clientId: 'merchant-service',
  brokers: ['localhost:9092'],
})

const initConsumer = async () => {
  const consumer = kafka.consumer({ groupId: 'order-service' })

  await consumer.connect()
  await consumer.subscribe({ topic: TOPICS.ORDER_SERVICE, fromBeginning: true })

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      console.log('Message received from Kafka topic: ' + topic)
      console.log(message?.value?.toString())

      const data = JSON.parse(message?.value?.toString()!)

      switch (data.type) {
        case EVENTS.PAYMENT_SUCCESS:
          console.log('Payment is successful.')
          break
        default:
          console.log('Invalid event type.....')
          break
      }
    },
  })
}

export { initConsumer }
